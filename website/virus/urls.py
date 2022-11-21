# -*- coding: utf-8 -*-
"""
virus urls
"""
from django.conf.urls import include, url

from virus import views

ajax_urlpatterns = [
    url(r'^get_virus_update/$', views.get_virus_update, name='get_virus_update'),
    url(r'^get_virus_files/$', views.get_virus_files, name='get_virus_files'),
    url(r'^blast_virus_file/$', views.blast_virus_file, name='blast_virus_file'),
    url(r'^virus_auto_complete/$', views.virus_auto_complete, name='virus_auto_complete'),
    url(r'^virus_advanced_search/$', views.virus_advanced_search, name='virus_advanced_search'),
    url(r'^send_email/$', views.send_email, name='send_email'),
    url(r'^get_download_link/$', views.get_download_link, name='get_download_link'),
    url(r'^excel_download/$', views.excel_download, name='excel_download'),
]

urlpatterns = [
    url(r'^ajax/', include(ajax_urlpatterns)),
]
