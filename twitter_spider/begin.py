# -*- coding: utf-8 -*-

from scrapy import cmdline
import argparse

parser = argparse.ArgumentParser(description='argparse for the crawler')
parser.add_argument('-key_word', type=str, default='')
parser.add_argument('-topic', type=str, default='')
parser.add_argument('-language', type=str, default='')
parser.add_argument('-date_since', type=str, default='')
parser.add_argument('-date_until', type=str, default='')
parser.add_argument('-num_of_data', type=float, default=float('inf'))
parser.add_argument('-from_acount', type=str, default='')
parser.add_argument('-to_acount', type=str, default='')
parser.add_argument('-mention_acount', type=str, default='')
parser.add_argument('-file_name', type=str, default='Default')
parser.add_argument('-keyword', type=str, default='')
parser.add_argument('-database', type=str, default='streaming')
parser.add_argument('-ip', type=str, default='166.111.137.183')
args = parser.parse_args()
key_word = args.key_word
topic = args.topic
language = args.language
date_since = args.date_since
date_until = args.date_until
num_of_data = args.num_of_data
from_acount = args.from_acount
to_acount = args.to_acount
mention_acount = args.mention_acount
file_name = args.file_name
keyword = args.keyword
database = args.database
ip = args.ip

if __name__ == '__main__':
    cmdline.execute("scrapy crawl twitter_spider_start".split())

# python begin.py -key_word trump -language en -date_since 2019-01-01 -date_until 2019-09-01 -from_acount realDonaldTrump -file_name trump_file	
