# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
import time
from bloom_filter import BloomFilter
from begin import file_name, keyword, database, ip
BF = BloomFilter(name=file_name, length=100000000, number=10, save_frequency=1000)


class TwitterSpiderPipeline(object):
    def __init__(self):
        self.table_name = file_name
        self.keyword = keyword

        self.db = pymysql.connect(
            host=ip,
            port=3306,
            user='root',
            passwd='fit4-305',
            db=database,
            charset='utf8'
        )
        self.cursor = self.db.cursor()

        sql = 'create table  if not EXISTS %s(' \
              'id        bigint  NOT NULL  AUTO_INCREMENT,' \
              'tweetid  bigint  NOT NULL,' \
              'userid    bigint  NOT NULL,' \
              'username  varchar(64) NOT NULL,' \
              'tweettext mediumtext,' \
              'createdat DATETIME NOT NULL,' \
              'accesslevel bigint,' \
              'favoritecount bigint,' \
              'retweetcount bigint,' \
              'retweetedstatusid bigint,' \
              'retweetedstatustext mediumtext,' \
              'geolocation mediumtext,' \
              'place mediumtext,' \
              'replytouserid bigint,' \
              'replytoscreenname varchar(64),' \
              'replytostatusid bigint,' \
              'contributors varchar(64),' \
              'currentuserretweetid bigint,' \
              'source varchar(200),' \
              'isfavorited tinyint,' \
              'isretweetbyme tinyint,' \
              'istruncated tinyint,' \
              'isretweet tinyint,' \
              'ispossiblysensitive tinyint,' \
              'hashtagentities varchar(200),' \
              'updatetime timestamp NOT NULL,' \
              'sensitivity float NOT NULL,' \
              'sensitivity2 float NOT NULL,'\
              'keyword varchar(500),'\
              'PRIMARY KEY(id))ENGINE=InnoDB DEFAULT CHARSET=utf8' % self.table_name
        self.cursor.execute(sql)

    def check_status(self):
        try:
            self.db.ping()
        except:
            print("失去服务器连接，需要重连")
            self.db = pymysql.connect(
                host=ip,
                port=3306,
                user='root',
                passwd='fit4-305',
                db=database,
                charset='utf8'
            )
            self.cursor = self.db.cursor()

    def process_item(self, item, spider):
        t = item['createdat']
        timestruct = time.strptime(t, '%I:%M %p - %d %b %Y')
        item['createdat'] = time.strftime('%Y-%m-%d %H:%M:%S', timestruct)

        # self.check_status()
        #
        # sql = 'select * from %s where tweetid = %s' % (self.table_name, item['tweetid'])
        # self.cursor.execute(sql)

        # if item['tweetid'] in BF:
        if BF.is_contain(item['tweetid']):
            print('数据已经存在，不再插入')
        else:
            sql = 'insert into %s (tweetid, userid, username, tweettext, createdat, accesslevel, favoritecount, ' \
                  'retweetcount, retweetedstatusid, retweetedstatustext, geolocation, place, replytouserid, ' \
                  'replytoscreenname, replytostatusid, contributors, currentuserretweetid, source,' \
                  'isfavorited, isretweetbyme, istruncated, isretweet, ispossiblysensitive, hashtagentities, ' \
                  'sensitivity, sensitivity2, keyword) value ' \
                  '(%s, %s, "%s", "%s", "%s", %s, %s, %s, %s, "%s", "%s", "%s", %s, "%s", %s,"%s"' \
                  ', %s,"%s", %s, %s, %s, %s,%s, "%s", %s, %s, "%s")' \
                  % (self.table_name, item['tweetid'], item['userid'], item['username'], item['tweettext'],
                     item['createdat'], item['accesslevel']
                     , item['favoritecount'], item['retweetcount'], item['retweetedstatusid'],
                     item['retweetedstatustext'], item['geolocation'], item['place']
                     , item['replytouserid'], item['replytoscreenname'], item['replytostatusid'], item['contributor'],
                     item['currentuserretweetid'], item['source'], item['isfavorited']
                     , item['isretweetbyme'], item['istruncated'], item['isretweet'], item['ispossiblysensitive'],
                     item['hashtagentities'], item['sensitivity'], item['sensitivity2'], self.keyword)
            self.cursor.execute(sql)
            BF.insert(item['tweetid'])
        self.db.commit()

        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.db.close()
