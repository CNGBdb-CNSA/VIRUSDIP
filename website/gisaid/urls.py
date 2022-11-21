"""
gisaid url
"""
from django.conf.urls import include, url

from gisaid import views

ajax_urlpatterns = [
    url(r'^get_gisaid_list/$', views.get_gisaid_list, name='get_gisaid_list'),
    url(r'^get_gisaid_update/$', views.get_gisaid_update, name='get_gisaid_update'),
    # url(r'^get_gisaid_files/$', views.get_gisaid_files, name='get_gisaid_files'),
    url(r'^get_scroll_data/$', views.get_scroll_data, name='get_scroll_data'),
    url(r'^gisaid_auto_complete/$', views.gisaid_auto_complete, name='gisaid_auto_complete'),
]
urlpatterns = [
    url(r'^ajax/', include(ajax_urlpatterns)),
]
