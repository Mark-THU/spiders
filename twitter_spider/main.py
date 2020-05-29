# -*- coding: utf-8 -*-
from datetime import *
import time
import pymysql
import re
import subprocess
import argparse

# 根据主题爬取数据
def crawl_by_theme(keywords_str, time_since, time_until):
    if args.task_type == 'keyword_search':
        pattern = re.compile(r'\((.*?)\)')
        result = pattern.findall(keywords_str)
        for i in range(len(result)):
            result[i] = result[i].split('|')

        keywords_list = []
        for i in range(len(result)):
            keywords_list = multiply(keywords_list, result[i])

        for k in keywords_list:
            if args.lang == '':
                cmdline = 'python3 begin.py -key_word "%s" -file_name %s -keyword "%s" ' \
                          '-date_since %s -date_until %s -database %s -ip %s' \
                          % (k, args.save_name, k, time_since, time_until, args.database, args.ip)
                p = subprocess.Popen(cmdline, shell=True)
                p.wait()
            else:
                cmdline = 'python3 begin.py -key_word "%s" -file_name %s -keyword "%s" ' \
                          '-date_since %s -date_until %s -language %s -database %s -ip %s' \
                          % (k, args.save_name, k, time_since, time_until, args.lang, args.database, args.ip)
                p = subprocess.Popen(cmdline, shell=True)
                p.wait()
    if args.task_type == 'user_search':
        if args.lang == '':
            cmdline = 'python3 begin.py -file_name %s -from_acount %s -date_since %s' \
                      ' -date_until %s -keyword %s -database %s -ip %s' \
                      % (args.save_name, keywords_str, time_since, time_until, keywords_str, args.database, args.ip)
            p = subprocess.Popen(cmdline, shell=True)
            p.wait()
        else:
            cmdline = 'python3 begin.py -file_name %s -from_acount %s -date_since %s' \
                      ' -date_until %s -language %s -keyword %s -database %s -ip %s' \
                      % (args.save_name, keywords_str, time_since, time_until, args.lang, keywords_str, args.database,
                         args.ip)
            p = subprocess.Popen(cmdline, shell=True)
            p.wait()


# 两数组中元素两两组合
def multiply(list1, list2):
    result = []
    if len(list1) == 0:
        return list2
    if len(list2) == 0:
        return list1
    for i in list1:
        for j in list2:
            result.append(i + ' ' + j)
    return result


# 爬虫任务
def task():
    db = pymysql.connect(
        host=args.ip,
        port=3306,
        user='root',
        passwd='fit4-305',
        db=args.database,
        charset='utf8'
    )
    cursor = db.cursor()
    sql = 'select * from %s' % args.task_list
    cursor.execute(sql)
    keywords = cursor.fetchall()
    if keywords:
        for theme in keywords:
            time_until = date.today().strftime('%Y-%m-%d')
            # 新加入的话题
            if theme[3]:
                time_since = datetime.strftime(theme[4].date(), '%Y-%m-%d')
                crawl_by_theme(theme[1], time_since, time_until)
                sql = 'update %s set updatetime = "%s", isnew= 0 where id = %s' % (args.task_list, time_until, theme[0])
                cursor.execute(sql)
                db.commit()
            else:
                time_remove = theme[6]
                if datetime.today() > time_remove:
                    pass
                else:
                    time_since = datetime.strftime(theme[5].date(), '%Y-%m-%d')
                    crawl_by_theme(theme[1], time_since, time_until)
                    sql = 'update %s set updatetime = "%s" where id = %s' % (args.task_list, time_until, theme[0])
                    cursor.execute(sql)
                    db.commit()


if __name__ == '__main__':
    # 设置命令行输入参数
    parser = argparse.ArgumentParser(description='argparse for the twitter crawler')
    parser.add_argument('task_type', type=str, default='', help='select task_type: keyword_search/user_search')
    parser.add_argument('save_name', type=str, default='', help='table name to save the result')
    parser.add_argument('database', type=str, default='', help='name database you want to save data')
    parser.add_argument('ip', type=str, default='', help='ip of mysql database')
    parser.add_argument('task_list', type=str, default='',
                        help='keyword list(keyword_search), username list(user_search)')
    parser.add_argument('-lang', type=str, default='', help='select language')
    args = parser.parse_args()

    while True:
        task()
        time.sleep(86400)
        # 清空布隆过滤器的数组
        file = open(args.save_name, 'wb+')
        file.close()
