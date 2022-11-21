# -*- coding: utf-8 -*-
"""
gisaid functions
"""

import os
import time
import datetime
import operator
from functools import reduce
import csv
import requests
from django.conf import settings
from django.db.models import Q
from virus.models import VirusAllData
from virus.functions import get_random_string
from virus.models import CompleteData



def _process_virus_excel(valid_headers, table_headers, gisaid_all):
    """
    打包选中的元信息
    :param valid_headers: 数据库字段表头
    :param table_headers: 展示字段表头
    :param gisaid_all: queryset 对象
    :return:
    """
    new_file_name = '{0}_all({1})_{2}.csv'.format(time.strftime('%Y%m%d'), gisaid_all.count(), get_random_string(6))
    new_file_path = os.path.join(settings.SENDFILE_ROOT, 'dc_virus/tmp/', new_file_name)
    file_data = open(new_file_path, 'w', newline='', encoding='utf-8-sig')
    csv_writer = csv.writer(file_data)
    csv_writer.writerow(table_headers)
    for item in gisaid_all:
        line = [getattr(item, col) for col in valid_headers]
        if line[5] == '4':
            line[5] = 'High'
            line[6] = 'Relatively good quality sequences.'
        elif line[5] == '3':
            line[5] = 'Medium'
        elif line[5] == '2':
            line[5] = 'Low'
        elif line[5] == '1':
            line[5] = 'Alert'
        elif line[5] == '0':
            line[5] = ''
        csv_writer.writerow(line)
    file_data.close()
    return new_file_path


def get_virus_download_link(valid_headers, table_headers, gisaid_all):
    """
    生成文件下载链接，依赖 dc_user_data 目录下的以下目录：

    :param table_headers: 下载表头
    :param valid_headers: 展示表头
    :param virus_seq_data: 过滤出来的 query_set 对象
    :return: 可用的下载链接
    """

    # 生成结果文件
    new_file_path = _process_virus_excel(valid_headers, table_headers, gisaid_all)
    request_param = {
        'file_path': new_file_path,
        'app_token': settings.DC_DC_APP_TOKEN,
        'from_app': settings.DC_PROJECT_CODE_NAME
    }
    try:
        response = requests.get(settings.GET_FILE_LINK_SERVICE, params=request_param, timeout=30)
        response.raise_for_status()
    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError):
        data = {'code': 2, 'error': [2010]}
        return data
    else:
        data = response.json()
        return data


