# -*- coding: utf-8 -*-
"""
    virus function
"""

import fnmatch
import os
import zipfile
import gzip
import bz2
import time
import datetime
import logging
import operator
import random
import string
from functools import reduce
import requests
from django.conf import settings
from django.core.mail import EmailMessage
from django.db.models import Q
from legacyhash import hash as py2_hash

from virus.models import VirusAllData, CompleteData
from website.functions import judgment_query_data

info_logger = logging.getLogger('virus_info_logger')
email_info_logger = logging.getLogger('email_info_logger')


def sync_excel_headers(headers, valid_headers, table_headers):
    """
    映射查询表头导展示表头，并重新排序
    :param headers: 前端输入表头
    :param valid_headers: 合法表头列表
    :param table_headers: 合法展示表头列表
    :return:
    """
    new_headers, new_tables_headers = [], []
    for item in valid_headers:
        if item in headers:
            if item == 'files':
                pass
            else:
                new_headers.append(item)
            new_tables_headers.append(table_headers[valid_headers.index(item)])
    return new_headers, new_tables_headers


def get_random_string(length=6):
    """
    @param length: 取样长度
    @return: 随机字符串
    """
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))


def _process_virus_files(virus_seq_data, id_list):
    """
    打包选中的序列文件
    @param virus_seq_data: queryset 对象
    @param id_list: 命中的序列文件 ID
    @return: 打包文件路径
    """
    file_path_list = []
    for _id in id_list:
        file_path = get_file_path(_id)
        file_path_list.append(os.path.join(settings.SENDFILE_ROOT, file_path))
    new_file_name = 'CNGBdb_VirusDIP_Sequence{0}_all({1})_{2}.zip'.format(
        time.strftime('%Y%m%d'),
        virus_seq_data.count(),
        get_random_string(6))
    new_file_path = os.path.join(settings.SENDFILE_ROOT, 'dc_virus/tmp/', new_file_name)
    zip_file = zipfile.ZipFile(new_file_path, 'w', zipfile.ZIP_DEFLATED)
    for _file in file_path_list:
        zip_file.write(_file, os.path.basename(_file))
    zip_file.close()
    return new_file_path


def get_virus_download_link(virus_seq_data, id_list):
    """
    生成文件下载链接，依赖 dc_user_data 目录下的以下目录：
     dc_cga/virus/2019-nCoV/ ncov 数据
     dc_cga/virus/ 病毒库全库数据
     dc_cga/tmp/ 临时路径
    """
    # 生成结果文件
    new_file_path = _process_virus_files(virus_seq_data, id_list)

    request_param = {
        'file_path': new_file_path,
        'app_token': settings.DC_DC_APP_TOKEN,
        'from_app': settings.DC_PROJECT_CODE_NAME
    }
    try:
        response = requests.get(settings.GET_FILE_LINK_SERVICE, params=request_param, timeout=30)
        response.raise_for_status()
    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError):
        info_logger.info(
            'code: 2010,  Request to get file link:%s failed.', str(new_file_path))
        data = {'code': 2, 'error': [2010]}
        return data
    else:
        data = response.json()
        if data['code'] == 2:
            info_logger.info(
                'code: %s, Request to get file link:%s  filed', str(data['code']), new_file_path)
        return data


def process_blast_file(virus_seq_data, id_list):
    """
    处理blast文件信息
    """
    file_path_list = []
    for _id in id_list:
        file_path = get_file_path(_id)
        file_path_list.append(os.path.join(settings.SENDFILE_ROOT, file_path))
    new_file_name = '{0}_all({1})_{2}.fasta'.format(time.strftime('%Y%m%d'), virus_seq_data.count(),
                                                    get_random_string(6))
    new_file_path = os.path.join(settings.SENDFILE_ROOT, 'dc_virus/tmp/', new_file_name)
    write_blast_file(file_path_list, new_file_path)

    zip_file_name = '{0}_all({1})_{2}.zip'.format(time.strftime('%Y%m%d'), virus_seq_data.count(),
                                                  get_random_string(6))
    zip_file_path = os.path.join(settings.SENDFILE_ROOT, 'dc_virus/tmp/', zip_file_name)
    zip_file = zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED)
    zip_file.write(new_file_path)
    zip_file.close()
    os.remove(new_file_path)
    return zip_file_name


def write_blast_file(file_path_list, new_file_path):
    """
    blast 压缩文件处理
    """
    for file_ in file_path_list:
        old_file_path = os.path.join(settings.SENDFILE_ROOT, file_)
        if str(old_file_path).endswith('.gz'):
            gzip_file = gzip.GzipFile(old_file_path)
            with open(new_file_path, mode='ab+') as file_g:
                file_g.write(gzip_file.read())
        elif str(old_file_path).endswith('.bz2'):
            bz2_file = bz2.BZ2File(old_file_path)
            with open(new_file_path, mode='ab+') as file_b:
                file_b.write(bz2_file.read())
        else:
            with open(old_file_path, 'rb') as file_o:
                with open(new_file_path, 'ab+') as file_t:
                    file_t.write(file_o.read() + bytes('\r\n', encoding="utf8"))



def process_update_link(series_no):
    """
    process update sq_link
    """
    sq_link = ''
    try:
        data = VirusAllData.objects.get(sequence_no=series_no)
    except VirusAllData.DoesNotExist:
        pass
    else:
        if data.virus_tag == 'seq':
            sq_link = 'https://db.cngb.org/search/sequence/{0}'.format(data.sequence_no)
        elif data.virus_tag == 'gisaid':
            sq_link = 'https://www.epicov.org/epi3/start/{0}'.format(data.sequence_no)
        elif data.virus_tag == 'cnsa':
            sq_link = ''
        else:
            sq_link = data.sequence_link
    return sq_link



def process_seqence_file(_id):
    """
    查询序列库，写入文件，生成链接
    """
    file_path_prefix = os.path.join(settings.SENDFILE_ROOT, 'dc_virus/tmp')
    for file in os.listdir(file_path_prefix):
        if fnmatch.fnmatch(file, '*'):
            # 判断文件的最后修改时间，根据修改时间清理文件
            filemt = time.localtime(os.stat(os.path.join(file_path_prefix, file)).st_mtime)
            filetime = datetime.datetime(filemt[0], filemt[1], filemt[2], filemt[3])
            diffours = (datetime.datetime.now() - filetime).seconds / 60 / 60
            if diffours > 12:
                # 删除12个小时未修改的文件
                os.remove(os.path.join(file_path_prefix, file))

    # 链接mongo，查询序列文件内容，写入文档
    file_path = os.path.join(file_path_prefix, '{}.fasta'.format(_id))
    seq_db_index = py2_hash(_id) % 10
    settings.MDB.select_multiple_db(seq_db_index)
    seq_data = settings.MDB['sequence'].find_one({'accession': _id})
    if seq_data:
        seq_text = '\n'.join(seq_data['sequence'])
    else:
        seq_text = ''
    with open(file_path, 'a') as seq_file:
        seq_file.write(seq_text)
    return file_path


def get_file_path(_id):
    """
    获取单个文件链接：
        根据不同数据来源，获取不同的文件路径
    """
    file_path = ''
    virus_all = VirusAllData.objects.filter(sequence_no=_id)
    for virus in virus_all:
        if virus.virus_tag == 'ncov':
            file_path = os.path.join(settings.SENDFILE_ROOT, 'dc_virus/2019-nCoV', virus.filename)
        # elif virus.virus_tag == 'gisaid':
        #     file_path = gisaid_file_process(_id, virus.filename)
        elif virus.virus_tag == 'seq':
            file_path = process_seqence_file(_id)
        elif virus.virus_tag == 'cnsa':
            file_path = os.path.join(settings.SENDFILE_ROOT, 'dc_virus/cnsa_sequence', virus.filename)
        elif virus.virus_tag == 'refseq':
            file_path = os.path.join(settings.SENDFILE_ROOT, 'dc_virus/all', virus.filename)

    return file_path


def get_file_link(_id):
    """
    生成单个文件下载链接
    """
    # 获取文件地址
    file_path = get_file_path(_id)
    _param = {
        'file_path': os.path.join(settings.SENDFILE_ROOT, file_path),
        'app_token': settings.DC_DC_APP_TOKEN,
        'from_app': settings.DC_PROJECT_CODE_NAME
    }
    try:
        response = requests.get(settings.GET_FILE_LINK_SERVICE, params=_param, timeout=30)
        response.raise_for_status()
    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError):
        info_logger.info(
            'code: 2010,  Request to get file link:%s failed.', str(file_path))
        data = {'code': 2, 'error': [2010]}
        return data
    else:
        data = response.json()
        if data['code'] == 2:
            info_logger.info(
                'code: %s, Request to get file link:%s  filed', str(data['code']), file_path)
        return data



def update_query_q(q, update_all):
    """
    update query
    """
    if q:
        try:
            q = datetime.datetime.strptime(q, '%Y-%m-%d')
        except ValueError:
            try:
                q = datetime.datetime.strptime(q, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                if update_all.filter(sequence_no=q).count():
                    update_all = update_all.filter(sequence_no=q)
                else:
                    update_all = update_all.filter(content__contains=q)
            else:
                update_all = update_all.filter(updated_time=q)
        else:
            update_all = update_all.filter(updated_time__gte=q, updated_time__lte=q + datetime.timedelta(days=1))
    return update_all.order_by('-updated_time')


def verification_q_data(q, key_):
    """
    校验q参数
    """
    flg = False
    query_data = ''
    for key in q.keys():
        if key_ != key:
            query_data += q[key]
    if not query_data and q[key_]:
        flg = True
    return flg


def get_q_query_count(q, virus_all, key):
    """
    获取q参数查询
    """
    virus = CompleteData.objects.filter(complete=q[key]).first()
    if virus and virus.virus_count is not None:
        count = virus.virus_count
    else:
        count = virus_all.count()
    return count


def get_advanced_search_count(is_ncov, q, virus_all):
    """
    处理高级搜索count问题
    """
    count = 0
    if is_ncov == 'false':
        for key in ['strain_name', 'host', 'collect_location']:
            if verification_q_data(q, key):
                count = get_q_query_count(q, virus_all, key)
    else:
        count = virus_all.count()
    return count




def traverse_queryset(virus_all):
    """
    遍历查询集
    """
    data_ = []
    for data in virus_all:
        tmp = {}
        tmp['complete'] = data.complete
        tmp['type'] = data.get_key_type_display()
        data_.append(tmp)
    context = {'code': 0, 'total': len(data_),
               'data': {'data': data_}}
    return context
