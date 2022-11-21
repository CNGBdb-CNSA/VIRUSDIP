"""
virus forms
"""
import base64
import time
import json
import urllib.parse
from django import forms
from django.db.models import Q
from django.urls import reverse

from virus.functions import virus_search, update_query_q, process_update_link, sync_excel_headers, \
    get_query_set, get_virus_download_link, process_blast_file, send_mail, get_complete_data, get_random_string

from virus.models import VirusAllUpdate, VirusAllData, CompleteData

SORT_KEY_CHOICE = (
    ('sequence_no', 'sequence_no'),
    ('sequence_platform', 'sequence_platform'),
    ('assembly_method', 'assembly_method'),
    ('host', 'host'),
    ('data_source', 'data_source'),
    ('submit_org', 'submit_org'),
    ('public_date', 'public_date'),
    ('organism', 'organism'),
    ('strain_name', 'strain_name'),
    ('tax_id', 'tax_id'),
    ('length', 'length'),
    ('collect_location', 'collect_location'),
    ('collect_date', 'collect_date'),
    ('sample_provide_org', 'sample_provide_org'),
    ('article', 'article'),
    ('sample_provider', 'sample_provider')
)

SORT_TYPE_CHOICE = (
    ('-', '-'),
)

IS_NCOV_CHOICE = (
    ('false', 'false'),
    ('true', 'true')
)

FILE_TYPE_CHOICE = (
    ('excel', 'excel'),
    ('file', 'file')
)

COMPLETE_TYPE_CHOICE = (
    ('sequence_no', 'sequence_no'),
    ('host', 'host'),
    ('organism', 'organism'),
    ('collect_location', 'collect_location'),
    ('strain_name', 'strain_name')
)


class SearchForm(forms.Form):
    """
    搜索数据表单基类
    """
    q = forms.CharField(required=False, max_length=1024)
    start = forms.IntegerField(required=True, min_value=0)
    length = forms.IntegerField(required=True, )
    sort_type = forms.ChoiceField(required=False, choices=SORT_TYPE_CHOICE)

    def clean_length(self):
        """
        清洗length
        """
        length = self.cleaned_data['length']
        if length not in [10, 25, 50, 100]:
            raise forms.ValidationError('Invalid length')
        return length

    def clean_q(self):
        """
        清洗query
        """
        q = self.cleaned_data['q']
        try:
            q = json.loads(q)
        except json.JSONDecodeError:
            raise forms.ValidationError('Invalid query')
        key_list = ["sequence_no", "strain_name", "collect_location", "host", "collect_date",
                    "public_date"]
        for key in q.keys():
            if key not in key_list:
                raise forms.ValidationError('Invalid query')
        return q


class AdvancedSearchForm(SearchForm):
    """
    高级搜索表单
    """
    sort_key = forms.ChoiceField(required=False, choices=SORT_KEY_CHOICE)
    is_ncov = forms.ChoiceField(required=False, choices=IS_NCOV_CHOICE)

    def query_db(self):
        """
        db query
        """
        sort_key = self.cleaned_data['sort_key']
        is_ncov = self.cleaned_data['is_ncov']
        sort_type = self.cleaned_data['sort_type']
        query = self.cleaned_data['q']
        start = self.cleaned_data['start']
        length = self.cleaned_data['length']

        if sort_key == '':
            sort_key = '-public_date'
        if sort_key == 'collect_date':
            sort_key = 'collect_date_start'
        elif sort_key == 'public_date':
            sort_key = 'public_date_start'
        if sort_type == '-' and sort_key != '-public_date':
            sort_key = sort_type + sort_key
        # 获取查询集
        virus_all, count = virus_search(query, is_ncov)
        virus_all = virus_all.order_by(sort_key, '-sequence_no')
        data = []
        for virus in virus_all[int(start):int(start) + int(length)]:
            tmp = {}
            for field in VirusAllData._meta.fields:  # pylint: disable=protected-access
                if field.name == 'public_date':
                    tmp[field.name] = getattr(virus, field.name).strftime('%Y-%m-%d')
                elif field.name == 'length':
                    tmp[field.name] = '{:,}'.format(getattr(virus, field.name))
                else:
                    tmp[field.name] = getattr(virus, field.name)

            if tmp['virus_tag'] == 'seq':
                tmp['sequence_link'] = 'https://db.cngb.org/search/sequence/{0}'.format(virus.sequence_no)
            elif tmp['virus_tag'] == 'gisaid':
                tmp['sequence_link'] = 'https://www.epicov.org/epi3/start/{0}'.format(virus.sequence_no)
            elif tmp['virus_tag'] == 'cnsa':
                tmp['sequence_link'] = ''
            data.append(tmp)
        return data, count


class UpdateVerificationForm(forms.Form):
    """
    获取更新日志表单
    """
    q = forms.CharField(required=False, max_length=32)
    start = forms.IntegerField(required=True, min_value=0)
    length = forms.IntegerField(required=True, )
    is_ncov = forms.ChoiceField(required=True, choices=IS_NCOV_CHOICE)

    def clean_length(self):
        """
        清洗length
        """
        length = self.cleaned_data['length']
        if length not in [10, 25, 50, 100]:
            raise forms.ValidationError('Invalid length')
        return length

    def query_db(self):
        """
        query db
        """
        is_ncov = self.cleaned_data['is_ncov']
        query = self.cleaned_data['q']
        start = self.cleaned_data['start']
        length = self.cleaned_data['length']

        if is_ncov == 'true':
            update_all = VirusAllUpdate.objects.filter(
                Q(virus_tag='ncov') | Q(virus_tag='gisaid') | Q(virus_tag='cnsa'))
            data_count = VirusAllData.objects.filter(
                Q(virus_tag='ncov') | Q(virus_tag='gisaid') | Q(virus_tag='cnsa')).count()

        else:
            update_all = VirusAllUpdate.objects.all()
            data_count = update_all.count()

        context = {'code': 0, 'data': {'data': []}}
        tmp_ = {'virus_count': '{:,}'.format(data_count)}
        update_all = update_query_q(query, update_all)
        context['data']['total'] = update_all.count()
        if not context['data']['total']:
            tmp_['update_time'] = None
        else:
            tmp_['update_time'] = update_all[0].updated_time.strftime('%Y-%m-%d %H:%M:%S')

            for update_data in update_all[int(start):int(start) + int(length)]:
                sequence_link = process_update_link(update_data.sequence_no)
                tmp = {
                    'content': update_data.content,
                    'series_no': update_data.sequence_no,
                    'sequence_link': sequence_link,
                    'virus_tag': update_data.virus_tag,
                    'updated_time': update_data.updated_time.strftime('%Y-%m-%d %H:%M:%S'),
                }
                context['data']['data'].append(tmp)

        context['data']['logs_data'] = tmp_
        return context


class GetFileVerificationForm(forms.Form):
    """
    获取下载文档表单
    """
    download_type = forms.ChoiceField(required=False, choices=FILE_TYPE_CHOICE)
    is_ncov = forms.ChoiceField(required=False, choices=IS_NCOV_CHOICE)
    headers = forms.CharField(required=False, max_length=512)
    id_list = forms.CharField(required=False)
    valid_headers = ['sequence_no', 'host', 'organism', 'tax_id', 'length', 'collect_location', 'collect_date',
                     'public_date', 'data_source', 'submit_org', 'sequence_platform', 'assembly_method',
                     'sample_provide_org', 'strain_name', 'sample_provider', 'article', 'jbrowse', 'files']
    table_headers = ['Sequence ID', 'Host', 'Organism', 'Tax ID', 'Length', 'Location',
                     'Sample collection date', 'Released date', 'Data source platform',
                     'Submitter organization', 'Sequencing technology/Platform', 'Assembly method',
                     'Originating Lab', 'Virus name', 'Submitter', 'Literature', 'Browse', 'DownloadLink']

    def clean_is_ncov(self):
        """ clean is_ncov """
        is_ncov = self.cleaned_data['is_ncov']
        if not is_ncov:
            is_ncov = 'false'
        return is_ncov

    def clean_headers(self):
        """
        清洗header
        """

        headers = self.cleaned_data['headers']

        if headers:
            headers = headers.split(',')
            if len(set(headers)) != len(headers) or not set(headers).issubset(set(self.valid_headers)):
                raise forms.ValidationError('Invalid headers')
        return headers

    def clean_id_list(self):
        """
        清洗id_list
        """
        id_list = self.cleaned_data['id_list']

        if id_list:
            id_list = id_list.split(',')
            if len(id_list) > 100:
                raise forms.ValidationError('Invalid id_list')

        return id_list

    def clean(self):
        """
        联合处理
        """
        is_ncov = self.cleaned_data.get('is_ncov', 'false')
        download_type = self.cleaned_data.get('download_type', 'excel')
        headers = self.cleaned_data.get('headers', [])
        id_list = self.cleaned_data.get('id_list', [])

        if download_type == 'excel' and not headers:
            raise forms.ValidationError('Invalid headers')
        if download_type == 'file' and not id_list:
            raise forms.ValidationError('Invalid id_list')
        if is_ncov == 'false' and (len(id_list) == 0 or len(id_list) > 100):
            if not (self.has_error('is_ncov') or self.has_error('is_list')):
                raise forms.ValidationError('Invalid id_list with non ncov')

    def query_db(self):
        """
        query db
        """
        id_list = self.cleaned_data['id_list']
        download_type = self.cleaned_data['download_type']
        is_ncov = self.cleaned_data['is_ncov']

        if download_type == 'file':
            virus_seq_data = get_query_set(is_ncov, download_type, id_list)
            file_data = get_virus_download_link(virus_seq_data, id_list)
            if file_data['code'] != 0:
                context = {'code': 2, 'error': file_data['error']}
            else:
                context = {
                    'code': 0,
                    'data': {'download_link': file_data['data']['link']}
                }
        else:
            # excel 响应流式下载链接
            params = {
                'download_type': download_type,
                'is_ncov': is_ncov,
                'headers': ','.join(self.cleaned_data['headers']),
                'id_list': ','.join(id_list)
            }
            context = {
                'code': 0,
                'data': {
                    'download_link': reverse('virus:excel_download') + "?" + urllib.parse.urlencode(params)}
            }
        return context


class BlastVerificationForm(forms.Form):
    """
    Blast 表单
    """
    is_ncov = forms.ChoiceField(required=True, choices=IS_NCOV_CHOICE)
    id_list = forms.CharField(required=True)

    def clean_id_list(self):
        """
        清洗id_list
        """
        id_list = self.cleaned_data['id_list']
        id_list = id_list.split(',')
        if len(set(id_list)) != len(id_list) and len(id_list) == 0:
            raise forms.ValidationError('Invalid id_list')
        if len(id_list) > 100:
            raise forms.ValidationError('Invalid id_list')

        return id_list

    def query_db(self):
        """
        query db
        """
        id_list = self.cleaned_data['id_list']
        is_ncov = self.cleaned_data['is_ncov']

        if is_ncov == 'true':
            project = '2019_ncov'
        else:
            project = 'virus'
        virus_seq_data = VirusAllData.objects.filter(~(Q(virus_tag='gisaid'))).order_by('sequence_no')
        virus_seq_data = virus_seq_data.filter(sequence_no__in=id_list)
        new_file_name = process_blast_file(virus_seq_data, id_list)
        file_name = base64.urlsafe_b64encode(new_file_name.encode())
        context = {
            'code': 0,
            'data': {'file_name': file_name.decode(), 'project': project}
        }
        return context


class SendEmailVerificationForm(forms.Form):
    """
    发送邮件校验表单
    """
    name = forms.CharField(required=True)
    email = forms.EmailField(required=True)
    organization = forms.CharField(required=True, max_length=1024)
    location = forms.CharField(required=True, max_length=1024)
    message = forms.CharField(required=True)

    def process_email(self):
        """
        处理邮件信息
        """
        name = self.cleaned_data['name']
        email = self.cleaned_data['email']
        organization = self.cleaned_data['organization']
        location = self.cleaned_data['location']
        message = self.cleaned_data['message']

        location_list = location.split(',')
        parm = {
            'name': name,
            'email': email,
            'organization': organization,
            'location': location,
            'message': message,
            'location_list': location_list
        }
        data = send_mail(parm)
        return data


class AutoCompleteVerificationForm(forms.Form):
    """
    自动补全表单
    """
    q = forms.CharField(required=False, max_length=32)
    key_type = forms.ChoiceField(required=False, choices=COMPLETE_TYPE_CHOICE)
    is_ncov = forms.ChoiceField(required=False, choices=IS_NCOV_CHOICE)

    def query_db(self):
        """
        query db
        """
        is_ncov = self.cleaned_data['is_ncov']
        key_type = self.cleaned_data['key_type']
        q = self.cleaned_data['q']

        key_tmp = {
            'host': 1,
            'organism': 2,
            'strain_name': 3,
            'collect_location': 4,
        }
        context = {'code': 0, 'total': 0, 'data': {'data': []}}
        if VirusAllData.objects.filter(sequence_no=q).exists():
            context = {'code': 0, 'total': 1, 'data': {'data': [{'complete': q, 'type': 'sequence_no'}]}}
        else:
            if is_ncov == 'false':
                virus_all = CompleteData.objects.all()
            else:
                virus_all = CompleteData.objects.filter(Q(virus_tag=1) | Q(virus_tag=3) | Q(virus_tag=4))
            context = get_complete_data(key_type, is_ncov, virus_all, key_tmp, context, q)
        return context


class CsvDownloadFrom(GetFileVerificationForm):
    """ CSV 下载验证表单"""
    def get_queryset(self):
        """
        获取 queryset
        @return:
        """
        is_ncov = self.cleaned_data['is_ncov']
        download_type = self.cleaned_data['download_type']
        id_list = self.cleaned_data['id_list']

        return get_query_set(is_ncov, download_type, id_list)

    @staticmethod
    def render_queryset(row, request_host):
        """
        处理响应数据
        @param row: 从数据库提取数据
        @param request_host: 请求域名
        @return: 组装数据
        """
        row = list(row)
        row.append(request_host + reverse('virus:get_download_link') + '?id={0}'.format(row[0]))
        return row

    def csv_filename(self):
        """
        获取 csv 文件名称
        @return:
        """
        queryset = self.get_queryset()
        new_file_name = 'CNGBdb_VirusDIP_excel{0}_all({1})_{2}.csv'.format(
            time.strftime('%Y%m%d'),
            queryset.count(),
            get_random_string(6))
        return new_file_name

    def csv_row_data(self, request_host):
        """
        使用生成器逐行响应 csv 数据
        @param request_host: 请求域名
        @return:
        """
        headers = self.cleaned_data['headers']
        headers, t_headers = sync_excel_headers(headers, self.valid_headers, self.table_headers)
        if 'DownloadLink' not in t_headers:
            t_headers.append('DownloadLink')
        yield t_headers
        for row in self.get_queryset().values_list(*headers):
            yield self.render_queryset(row, request_host)
