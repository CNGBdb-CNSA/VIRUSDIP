"""
gisaid form
"""
import datetime
from django import forms
from django.db.models import Q
from gisaid.functions import search_gisaid_sequence
from virus.models import VirusAllData, VirusAllUpdate, VirusAllLogs
from website.functions import judgment_query_data
from virus.forms import SearchForm


class GetGisaidListForm(SearchForm):
    """
    获取gisaid列表表单
    """
    sort_key = forms.ChoiceField(required=False, choices=SORT_KEY_CHOICE)

    def query_db(self):
        """
        query db
        """
        sort_key = self.cleaned_data['sort_key']
        sort_type = self.cleaned_data['sort_type']
        q = self.cleaned_data['q']
        start = self.cleaned_data['start']
        length = self.cleaned_data['length']

        sort_key = SORT_KEY_DIC.get(sort_key)
        if sort_key is None:
            sort_key = '-public_date'
        if sort_key == 'collect_date':
            sort_key = 'collect_date_start'
        elif sort_key == 'public_date':
            sort_key = 'public_date_start'
        if sort_type == '-' and sort_key != '-public_date':
            sort_key = sort_type + sort_key

        # 判断query是否有值
        query_data = judgment_query_data(q)
        if not query_data:
            gisaid_all = VirusAllData.objects.filter(virus_tag='gisaid').order_by(sort_key, 'sequence_no')
        else:
            gisaid_all = search_gisaid_sequence(q).order_by(sort_key, 'sequence_no')

        context = {'code': 0, 'data': {'data': []}}
        # 按照 covv_subm_date 正序
        context['data']['total'] = gisaid_all.count()
        for gisaid in gisaid_all[int(start):int(start) + int(length)]:
            if gisaid.length is not None:
                seq_length = '{:,}'.format(gisaid.length)
            else:
                seq_length = '--'
            tmp = {
                'covv_accession_id': gisaid.sequence_no,
                'organism': gisaid.organism,
                'sequence_length': seq_length,
                'covv_subm': gisaid.sample_provider,
                'covv_host': gisaid.host,
                'covv_virus_name': gisaid.strain_name,
                'covv_subm_lab': gisaid.submit_org,
                'covv_subm_date': gisaid.public_date.strftime('%Y-%m-%d'),
                'covv_location': gisaid.collect_location,
                'covv_collection_date': gisaid.collect_date,
                'covv_orig_lab': gisaid.sample_provide_org,
                'covv_seq_technology': gisaid.sequence_platform,
                'covv_assembly_method': gisaid.assembly_method,
                'covv_authors': gisaid.author,
                'covv_passage': gisaid.passage,
                'covv_comment': gisaid.virus_comment,
                'covv_comment_type': gisaid.virus_comment_type,
            }
            context['data']['data'].append(tmp)
        return context


class UpdateVerificationForm(forms.Form):
    """
    获取更新日志表单
    """
    q = forms.CharField(required=False, max_length=64)
    start = forms.IntegerField(required=True, min_value=0)
    length = forms.IntegerField(required=True, )

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
        q = self.cleaned_data['q']
        start = self.cleaned_data['start']
        length = self.cleaned_data['length']

        context = {'code': 0, 'data': {'data': []}}
        # 按照 accession 正序
        update_all = VirusAllUpdate.objects.filter(virus_tag='gisaid')
        if q:
            try:
                q = datetime.datetime.strptime(q, '%Y-%m-%d')
            except ValueError:
                try:
                    q = datetime.datetime.strptime(q, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    update_all = update_all.filter(Q(content__icontains=q) | Q(sequence_no__icontains=q))
                else:
                    update_all = update_all.filter(updated_time=q)
            else:
                update_all = update_all.filter(updated_time__gte=q,
                                               updated_time__lte=q + datetime.timedelta(days=1))
        update_all = update_all.order_by('-updated_time', 'id')
        context['data']['total'] = update_all.count()
        gisaid_count = VirusAllData.objects.filter(virus_tag='gisaid').count()
        logs_all = VirusAllLogs.objects.filter(virus_tag='gisaid').order_by('-updated_time').first()
        tmp_ = {'gisaid_count': '{:,}'.format(int(gisaid_count))}
        if logs_all is None:
            tmp_['adds'] = 0
            tmp_['update_time'] = None
        else:
            tmp_['adds'] = '{:,}'.format(logs_all.adds)
            tmp_['update_time'] = logs_all.updated_time.strftime('%Y-%m-%d %H:%M:%S')
        context['data']['logs_count'] = tmp_

        for update_data in update_all[int(start):int(start) + int(length)]:
            tmp = {
                'content': update_data.content,
                'covv_accession_id': update_data.sequence_no,
                'updated_time': update_data.updated_time.strftime('%Y-%m-%d %H:%M:%S'),
            }
            context['data']['data'].append(tmp)
        return context
