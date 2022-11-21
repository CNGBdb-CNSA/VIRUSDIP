"""
literature forms
"""
from datetime import datetime, date

from django import forms
from django.db.models import Func, Count, F, Q
from literature.functions import LiteratureSearch
from literature.models import Literature

FIELD_CHOICE = (
    ('pub_date', 'pub_date'),
    ('tags', 'tags')
)
CHART_TYPE_CHOICE = (
    ('pie', 'pie'),
    ('line_chart', 'line_chart'),
    ('histogram', 'histogram')
)

TAGS_CHOICE = (
    ('', 'No choice provided'),
    ('viral_pathogen', 'viral_pathogen'),
    ('animal_model', 'animal_model'),
    ('detection_and_diagnosis', 'detection_and_diagnosis'),
    ('drugs_and_clinical', 'drugs_and_clinical'),
    ('vaccine_development', 'vaccine_development'),
)


class StatisticsForm(forms.Form):
    """
    统计数据处理表单
    """
    field = forms.ChoiceField(required=True, choices=FIELD_CHOICE)
    chart_type = forms.ChoiceField(required=True, choices=CHART_TYPE_CHOICE)

    tags_list = ["animal_model", "detection_and_diagnosis", "drugs_and_clinical", "vaccine_development",
                 "viral_pathogen"]
    countries_list = ["Others", "China", "US", "UK", "Italy", "France", "Germany"]

    @staticmethod
    def get_date_range():
        """
        获取从 2019-12 到当前日期的所有月份
        @return: 月份列表
        """
        data_range = []
        start_month = date(2019, 12, 1)
        today = date.today()

        for year in range(2019, today.year + 1):
            for month in range(1, 13):
                tmp_date = date(year, month, 1)
                if start_month <= tmp_date <= today:
                    data_range.append(tmp_date.strftime('%Y-%m'))
        return data_range

    @staticmethod
    def fetch_aggs_count_by_country(country, query_result):
        """
        fetch aggs count by country
        """
        for db_query_name, db_count in query_result:
            if db_query_name == country:
                return db_count
        return 0

    @staticmethod
    def convert_tags(tag):
        """
        分类标签转换
        @param tag: 输入数据
        @return: 转换后标签
        """
        display_tags = {
            "viral_pathogen": "Epidemiology",
            "animal_model": "Animal Models",
            "detection_and_diagnosis": "Diagnostic",
            "drugs_and_clinical": "Drug and Clinical",
            "vaccine_development": "Vaccine Research"
        }
        try:
            return display_tags[tag]
        except KeyError:
            return tag

    def query_db(self, chart_type, pie_query, line_chart_query, filter_list):
        """
        db query
        """
        if chart_type == 'pie':
            data = []
            for key, count in pie_query:
                data.append({'name': self.convert_tags(key), 'num': count})
        else:
            data = {}
            for field_data, filter_query in filter_list:
                query_result = list(line_chart_query.filter(filter_query))
                for country in self.countries_list:
                    if country not in data:
                        data[country] = {
                            'x_data': [self.convert_tags(field_data)],
                            'series_data': [self.fetch_aggs_count_by_country(country, query_result)]
                        }
                    else:
                        data[country]['x_data'].append(self.convert_tags(field_data))
                        data[country]['series_data'].append(self.fetch_aggs_count_by_country(country, query_result))
        return data



class SearchForm(forms.Form):
    """
    统计数据处理表单
    """
    query = forms.CharField(required=False, max_length=64)
    start = forms.IntegerField(required=True, min_value=0)
    size = forms.IntegerField(required=True, )
    tags = forms.CharField(required=False, max_length=128)
    pub_date = forms.CharField(required=False, max_length=32)

    def clean_size(self):
        """
        清洗 size 字段
        @return: 清洗后字段
        """
        size = self.cleaned_data['size']
        if size not in [10, 25, 50, 100]:
            raise forms.ValidationError('Invalid size')
        return size

    def clean_query(self):
        """
        clean query
        """
        query = self.cleaned_data['query']
        return query.lower()

    def clean_tags(self):
        """
        清洗 tags 字段
        @return: 清洗结果
        """
        tags = self.cleaned_data['tags']
        if tags:
            tags = tags.split(',')
            avail_tags = list(zip(*TAGS_CHOICE))[0]
            final_tags = []
            for i in tags:
                if i in avail_tags and i != '':
                    final_tags.append(i)
                else:
                    raise forms.ValidationError('Invalid tags')
        else:
            return []
        return final_tags

    def clean_pub_date(self):
        """
        清洗 pub_date 字段
        @return: 清洗后字段
        """
        pub_date = self.cleaned_data['pub_date']
        if pub_date:
            pub_date = pub_date.split(',')
            if len(pub_date) != 2:
                raise forms.ValidationError('Invalid pub_date')

            for input_date in pub_date:
                try:
                    datetime.strptime(input_date, '%Y-%m-%d')
                except ValueError:
                    raise forms.ValidationError('Invalid pub_date')

            if datetime.strptime(pub_date[0], '%Y-%m-%d') > datetime.strptime(pub_date[1], '%Y-%m-%d'):
                raise forms.ValidationError('Invalid pub_date')
            pub_date = [datetime.strptime(i, '%Y-%m-%d') for i in pub_date]
        else:
            pub_date = []
        return pub_date

    def search(self):
        """
        执行文献检索
        @return: 检索后，获取的指定分页结果
        """
        query = self.cleaned_data['query']
        start = self.cleaned_data['start']
        size = self.cleaned_data['size']
        tags = self.cleaned_data['tags']
        pub_date = self.cleaned_data['pub_date']

        return search_handle.search()
