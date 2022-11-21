# -*- coding: utf-8 -*-
"""
literature urls
"""
from django.conf.urls import include, url
from literature import views

# page_urlpatterns = [
# ]

ajax_urlpatterns = [
    url(r'^search_literature/$', views.search, name='search'),
    url(r'^get_literature_stats/$', views.statistics, name='statistics'),
    url(r'^get_literature_logs/$', views.logs, name='logs'),
]
urlpatterns = [
    # url(r'^', include(page_urlpatterns)),
    url(r'^ajax/', include(ajax_urlpatterns)),
]
