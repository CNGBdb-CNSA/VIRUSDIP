# -*- coding: utf-8 -*-
import calendar
import datetime
import logging
import os
import re
import sys

import psycopg2
import django


django.setup(set_prefix=False)

from virus.models import CompleteData, VirusAllData


class FormatDate(object):
    attributes = ['year', 'month', 'day']
    format_dict = {
        '%Y': [attributes[0]],
        '%Y-%m': attributes[:2],
        '%Y-%m-%d': attributes,
        '%Y/%m/%d': attributes,
        '%d-%b-%Y': attributes,
        '%b-%Y': attributes[:2],
    }

    def __init__(self, input_date):
        for attr in self.attributes:
            setattr(self, attr, None)
        self.next_year = None

        cleaned_date = self._clean_input(input_date)
        self._set_date(cleaned_date)
        if self.year is None:
            raise ValueError("Can not find date format for %s!\n" % input_date)

    @staticmethod
    def _clean_input(input_date):
        """
        1. 处理 '0021-03-27' 特殊格式，转换为 '2021-03-27'
        2. 处理 '20-21-06-19' 特殊格式，转换为 '2021-06-19'
        3. 处理 '202106-06' 特殊格式，转换为 '2021-06-06'
        :param input_date: 输入日期
        :return:
        """
        if re.match(r'^(00\d{2})-(\d{2})-(\d{2})', input_date):
            return '20' + input_date[2:]
        if re.match(r'^20-21-(\d{2})-(\d{2})', input_date):
            return input_date.replace('-', '', 1)
        if re.match(r'^2021(\d{2})-(\d{2})', input_date):
            return input_date.replace('2021', '2021-', 1)
        return input_date

    def _set_date(self, string):
        string = re.sub(r'\s+', '', string)
        pattern = re.match(r'^(\d{4})/(\d{4})', string)
        if pattern:
            self.year = int(pattern.group(1))
            self.next_year = int(pattern.group(2))
        else:
            for key, values in self.format_dict.items():
                date = None
                try:
                    date = datetime.datetime.strptime(string, key)
                except ValueError:
                    continue
                for value in values:
                    setattr(self, value, getattr(date, value))
                break

    @property
    def date_normal(self):
        date_for = None
        if self.next_year is not None:
            date_for = '{0.year}/{0.next_year}'.format(self)
        else:
            alist = []
            for i, attr in enumerate(self.attributes):
                value = getattr(self, attr)
                if value is None:
                    break
                if i > 0:
                    value = '{0:0>2d}'.format(value)
                alist.append(value)
            date_for = '-'.join(map(str, alist))
        return date_for

    @staticmethod
    def output_date(year, month, day):
        date = datetime.datetime(year, month, day)
        return date.strftime('%Y-%m-%d')

    @property
    def date_range(self):
        ranges = []
        if self.next_year is not None:
            ranges.append(self.output_date(self.year, 1, 1))
            ranges.append(self.output_date(self.next_year, 12, 31))
        elif self.day is not None:
            date_str = self.output_date(self.year, self.month, self.day)
            ranges = [date_str, date_str]
        elif self.month is not None:
            _, end = calendar.monthrange(self.year, self.month)
            ranges.append(self.output_date(self.year, self.month, 1))
            ranges.append(self.output_date(self.year, self.month, end))
        else:
            ranges.append(self.output_date(self.year, 1, 1))
            ranges.append(self.output_date(self.year, 12, 31))
        return ranges


class ProcessCompleteData(object):
    def __init__(self, data, db, logger):
        self.data = data
        self.db = db
        self.logger = logger

        self.conn = None
        self.cur = None
        self.connect_db()

    def connect_db(self):
        try:
            self.conn = psycopg2.connect(**self.db)
            self.cur = self.conn.cursor()
        except (Exception, psycopg2.DatabaseError) as err:
            self.logger.error('Connect to postgresql failed. ')
            raise err


    def disconnect(self):
        try:
            self.cur.close()
            self.conn.close()
        except psycopg2.DatabaseError:
            pass


class CompleteCount(object):
    def __init__(self, db, logger):
        self.db = db
        self.logger = logger
        self.conn = None
        self.cur = None

        self.connect_db()

    def connect_db(self):
        try:
            self.conn = psycopg2.connect(**self.db)
            self.cur = self.conn.cursor()
        except (Exception, psycopg2.DatabaseError) as err:
            self.logger.error('Connect to postgresql failed. ')
            raise err

    def sync_to_db(self, _type, counter):
        for key, count in counter:
            if key:
                self.cur.execute(cmd, (count, key))
        self.conn.commit()
        self.logger.info('Updated count for type:{} . '.format(_type))

    def count_keys(self):
        self.logger.info('Count complete keys. ')
        complete_keys = ['host', 'collect_location', 'organism', 'strain_name']
        count_max = 10000
        for _type in complete_keys:
            self.cur.execute(cmd)
            counter = self.cur.fetchall()
            self.sync_to_db(_type, counter)
        self.disconnect()
        self.logger.info('Update virus count finished.')

    def disconnect(self):
        try:
            self.cur.close()
            self.conn.close()
        except psycopg2.DatabaseError:
            pass

