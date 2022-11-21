"""
literature functions
"""
import copy
import operator
import re
from functools import reduce
import elasticsearch
from django.conf import settings
from django.db.models import Q
from literature.models import Literature


def convert_highlight(item):
    """
    查询结果的高亮转换
    :param item: 匹配对象(字典）
    :return: converted document
    """
    hit_result = item['_source']
    hit = copy.deepcopy(item['_source'])
    for k, source_value in hit.items():
        highlight_key = k + '_hl'

        try:
            highlight_value = item['highlight'][k + '.keyword'] if k + '.keyword' in item['highlight'] else \
                item['highlight'][k]
        except KeyError:
            hit_result[highlight_key] = source_value
        else:
            if isinstance(source_value, list):
                # 若高亮字段为列表，则列表中原元素替换为高亮元素，保持原顺序
                for highlight_item in highlight_value:
                    clean_item = highlight_item.replace("<span class='high-light'>", '').replace("</span>", '')
                    if clean_item in source_value:
                        source_value = [highlight_item if i == clean_item else i for i in source_value]
                hit_result[highlight_key] = source_value
            elif not isinstance(source_value, bytes):
                # 若高亮字段为字符串
                hit_result[highlight_key] = ''.join(highlight_value)
            else:
                raise ValueError('Invalid highlight type: {0}'.format(type(source_value)))

    return hit_result


def set_prefix_for_response(value, prefix):
    """
    给指定数据, 加上指定前缀. 当前用来给 物种ID 和 文献ID 加上前缀.
    :param value: 待加前缀数字
    :param prefix: 前缀
    :return: 加上前缀后的数字
    """
    re_number = re.compile(r'\d+')

    if value.find(prefix) == -1:  # 给源 ID 加上前缀
        try:
            source_id = re_number.findall(value)[0]
        except IndexError:
            pass
        else:
            new_id = prefix + source_id
            value = value.replace(source_id, new_id)
    return value


def format_link(test_value, format_string, format_id=''):
    """
    format link
    """
    link = ''
    if test_value:
        link = format_string.format(format_id) if format_id else format_string.format(test_value)
    return link


class LiteratureSearch:
    """
    LiteratureSearch
    """

    def __init__(self, query, start, size, tags, pub_date):
        self.query = query
        self.start = start
        self.size = size
        self.tags = tags
        self.pub_date = pub_date


    def fetch_hits_data(self, es_hits, terms_ids):
        """
        fetch hits data
        terms_id： Literature 表中的数据
        """
        data_dict = {}
        for doc in es_hits:
            doc_hl = convert_highlight(doc)
            if doc_hl['identifier'] in self.query.split() or doc_hl['accession'] in self.query.split():
                identifier_hl = set_prefix_for_response(doc_hl['identifier_hl'], 'PMID:')
            else:
                identifier_hl = 'PMID:' + doc_hl['identifier']
        order_data = []
        for pmid in terms_ids:
            order_data.append(data_dict[pmid])
        return order_data

    def get_pagination_data(self, query_set):
        """
        get pagination data
        """
        data = {'result': [], 'total': query_set.count()}
        terms_ids = list(query_set[self.start: self.start + self.size].values_list('pmid', flat=True))

        try:
            res = settings.ES.search(index=settings.ES_LITERATURE_INDEX, body=query_body, request_timeout=30)
        except (elasticsearch.TransportError, elasticsearch.ConnectionError, elasticsearch.ConnectionTimeout):
            return None

        data['result'] = self.fetch_hits_data(res['hits']['hits'], terms_ids)
        return data

    def search(self):
        """
        search
        """
        query_set = self.build_query()
        return self.get_pagination_data(query_set)
