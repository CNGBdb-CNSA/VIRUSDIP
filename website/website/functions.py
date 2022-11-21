# -*- coding: utf-8 -*-
"""
website functions
"""


def project_navbar(tabname):
    u"""
    Description.

    项目主导航栏按钮状态。
    created date: 2017-04-18
    author: doo
    last update: 2017-04-18
    """
    project_navbar_ = {
        'home': '',
    }
    project_navbar_[tabname] = 'active'
    return project_navbar_


def judgment_query_data(q):
    """
    key判断
    """
    q_data = ''
    for key in q.keys():
        q_data += str(q[key])
    return q_data
