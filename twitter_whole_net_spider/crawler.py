# -*- coding: utf-8 -*-

import tweepy
import pymysql
import datetime
import time
import argparse
import re
from bloom_filter import BloomFilter


# 使用twitter api 的全网爬虫
class TwitterApiSpider(object):
    # 初始化，连接数据库，建表
    def __init__(self, seed):
        self.db = pymysql.connect(
            host=host,
            port=3306,
            user='root',
            passwd='fit4-305',
            db=db,
            charset='utf8mb4'
        )
        self.cursor = self.db.cursor()

        self.seed = seed
        self.api_number = 0 #记录API顺序
        self.location = 0 #记录checkpoints位置
        self.api_error_times = 0 #记录API连续error次数
        self.bloom_filter = BloomFilter(name=name, length=length, number=number, save_frequency=save_frequency)
        # checkpoints_id表
        sql = 'create table if not exists checkpoints_id(' \
              'checkpoints bigint not null)' \
              'ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'
        self.cursor.execute(sql)
        print('建立checkpoints_id表')

        # checkpoints_info表
        sql = 'create table if not exists checkpoints_info(' \
              'checkpoints bigint not null)' \
              'ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'
        self.cursor.execute(sql)
        print('建立checkpoints_info表')

        # user_id表
        sql = 'create table if not exists user_id(' \
              'id bigint not null auto_increment,' \
              'userid bigint not null,' \
              'primary key(id))' \
              'ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'
        self.cursor.execute(sql)
        print('建立user_id表')

        sql = 'create table if not exists user_info(' \
              'id bigint not null auto_increment,' \
              'userid bigint not null,' \
              'username varchar(64) not null,' \
              'userscreenname varchar(64) not null,' \
              'description mediumtext,' \
              'createat timestamp not null default "1970-01-02 00:00:00",' \
              'url mediumtext,' \
              'profileimageurl mediumtext,' \
              'profilebackgroundimageurl mediumtext,' \
              'location mediumtext,' \
              'timezone mediumtext,' \
              'accesslevel bigint not null,' \
              'statuscount bigint not null,' \
              'followerscount bigint not null,' \
              'friendscount bigint not null,' \
              'favouritescount bigint not null,' \
              'listedcount bigint not null,' \
              'isprotected tinyint not null,' \
              'isgeoenabled tinyint not null,' \
              'isshowallinlinemedia tinyint not null,' \
              'iscontributorsenable tinyint not null,' \
              'isfollowrequestsent tinyint not null,' \
              'isprofilebackgroundtiled tinyint not null,' \
              'isprofileusebackgroundtiled tinyint not null,' \
              'istranslator tinyint not null,' \
              'isverified tinyint not null,' \
              'vtcoffset bigint,' \
              'lang varchar(64) default "en",' \
              'biggerprofileimageurl mediumtext,' \
              'biggerprofileimageurlhttps mediumtext,' \
              'miniprofileimageurl mediumtext,' \
              'miniprofileimageurlhttps mediumtext,' \
              'originalprofileimageurl mediumtext,' \
              'originalprofileimageurlhttps mediumtext,' \
              'profilebackgroundimageurlhttps mediumtext,' \
              'profilebanneripadurl mediumtext,' \
              'profilebanneripadretinaurl mediumtext,' \
              'profilebannermobileurl mediumtext,' \
              'profilebannermobileretinaurl mediumtext,' \
              'profilebannerretinaurl mediumtext,' \
              'profilebannerurl mediumtext,' \
              'profileimageurlhttps mediumtext,' \
              'updatetime timestamp not null default now(),' \
              'sensitivity float not null,' \
              'sensitivity2 float not null,' \
              'primary key(id))ENGINE=InnoDB default CHARSET=utf8mb4'
        self.cursor.execute(sql)
        print('建立user_info表')

        # 将seed加入到user_id中,如果user_id中没有内容
        sql = 'select userid from user_id where id = 1'
        self.check_status()
        self.cursor.execute(sql)
        if self.cursor.fetchall():
            pass
        else:
            sql = 'insert into user_id (userid) value (%s)' % self.seed
            self.check_status()
            self.cursor.execute(sql)
            self.db.commit()

        # 初始化checkpoints_id/info
        sql = 'select * from checkpoints_id'
        self.check_status()
        self.cursor.execute(sql)
        if self.cursor.fetchall():
            pass
        else:
            sql = 'insert into checkpoints_id (checkpoints) value (0)'
            self.check_status()
            self.cursor.execute(sql)
            self.db.commit()

        sql = 'select * from checkpoints_info'
        self.check_status()
        self.cursor.execute(sql)
        if self.cursor.fetchall():
            pass
        else:
            sql = 'insert into checkpoints_info (checkpoints) value (0)'
            self.check_status()
            self.cursor.execute(sql)
            self.db.commit()

    # 随机返回api
    def get_random_api(self):
        # 从tokens.txt中读取需要用到的密钥
        file = open('tokens.txt', 'r')
        tokens = []
        for line in file:
            tokens.append(line.split(','))

        random_int = self.api_number % len(tokens)
        print('使用app_key序号：' + str(random_int))
        self.api_number = self.api_number + 1
        consumer_key = tokens[random_int][0]
        consumer_secret = tokens[random_int][1]
        access_token = tokens[random_int][2]
        access_token_secret = tokens[random_int][3][0:-1]
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        return tweepy.API(auth, proxy=proxy, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    # 根据id，获取其朋友和粉丝，加入到user_id中，此函数用户中文用户id爬取
    def search_id_chinese(self, id):
        # 获取粉丝id
        cursor = -1
        while cursor != 0:
            api = self.get_random_api()
            try:
                result = api.followers_ids(user_id=id, cursor=cursor, count=5000)
                self.api_error_times = 0 #成功使用API，将错误次数计数清零
            except:
                self.api_error_times = self.api_error_times + 1
                if self.api_error_times % 10 == 0: #连续10次出错，则休息300s，并打印提示信息
                    self.api_error_times = 0
                    print('something goes wrong with followers_ids api, we will retry it in 300s')
                    time.sleep(300)
            else:
                followers_ids = result[0]
                for follower_id in followers_ids:
                    if self.bloom_filter.is_contain(str(follower_id)): #判断是否在BF中，是则跳过，不是则进一步判断是不是中国人
                        pass
                    else:
                        self.bloom_filter.insert(str(follower_id)) #不论是不是中国人都要将其插入BF，这样可以减少后面重复判断
                        if self.is_chinese(follower_id):
                            sql = 'insert into user_id (userid) value (%s)' % follower_id
                            self.check_status()
                            self.cursor.execute(sql)
                        else:
                            pass
                cursor = result[1][1] #获得新的cursor
                self.db.commit()
        # 处理朋友
        cursor = -1
        while cursor != 0:
            api = self.get_random_api()
            try:
                result = api.friends_ids(user_id=id, cursor=cursor, count=5000)
                self.api_error_times = 0
            except:
                self.api_error_times = self.api_error_times + 1
                if self.api_error_times % 10 == 0:
                    self.api_error_times = 0
                    print('something goes wrong with followers_ids api, we will retry it in 300s')
                    time.sleep(300)
            else:
                friends_ids = result[0]
                for friend_id in friends_ids:
                    # sql = 'select * from searched_list where userid = %s limit 1' % friend_id
                    # self.cursor.execute(sql)
                    if self.bloom_filter.is_contain(str(friend_id)):
                        pass
                    else:
                        self.bloom_filter.insert(str(friend_id))
                        if self.is_chinese(friend_id):
                            sql = 'insert into user_id (userid) value (%s)' % friend_id
                            self.check_status()
                            self.cursor.execute(sql)
                        else:
                            pass
                cursor = result[1][1]
                self.db.commit()
    
    # 此函数用户全网用户id爬取，不会判断是否是中国人
    def search_id(self, id):
        # 获取粉丝
        cursor = -1
        while cursor != 0:
            api = self.get_random_api()
            try:
                result = api.followers_ids(user_id=id, cursor=cursor, count=5000)
                self.api_error_times = 0
            except:
                self.api_error_times = self.api_error_times + 1
                if self.api_error_times % 10 == 0:
                    self.api_error_times = 0
                    print('something goes wrong with followers_ids api, we will retry it in 300s')
                    time.sleep(300)
            else:
                followers_ids = result[0]
                for follower_id in followers_ids:
                    # sql = 'select * from searched_list where userid = %s limit 1' % follower_id
                    # self.cursor.execute(sql)
                    if self.bloom_filter.is_contain(str(follower_id)):
                        pass
                    else:
                        sql = 'insert into user_id (userid) value (%s)' % follower_id
                        self.check_status()
                        self.cursor.execute(sql)
                        self.bloom_filter.insert(str(follower_id))

                cursor = result[1][1]
                self.db.commit()
        # 获取朋友
        cursor = -1
        while cursor != 0:
            api = self.get_random_api()
            try:
                result = api.friends_ids(user_id=id, cursor=cursor, count=5000)
                self.api_error_times = 0
            except:
                self.api_error_times = self.api_error_times + 1
                if self.api_error_times % 10 == 0:
                    self.api_error_times = 0
                    print('something goes wrong with followers_ids api, we will retry it in 300s')
                    time.sleep(300)
            else:
                friends_ids = result[0]
                for friend_id in friends_ids:
                    # sql = 'select * from searched_list where userid = %s limit 1' % friend_id
                    # self.cursor.execute(sql)
                    if self.bloom_filter.is_contain(str(friend_id)):
                        pass
                    else:
                        sql = 'insert into user_id (userid) value (%s)' % friend_id
                        self.check_status()
                        self.cursor.execute(sql)
                        self.bloom_filter.insert(str(friend_id))
                cursor = result[1][1]
                self.db.commit()

    # 根据user_id获取用户信息,
    def get_user_info(self):
        print('开始获取用户信息')
        sql = 'select checkpoints from checkpoints_info'
        self.check_status()
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        if result:
            if result == -1:
                print('已经获取了全部user_info')
                return
            else:
                self.location = result[0][0]
        else:
            self.location = self.location
        flag = 1
        while flag:
            api = self.get_random_api()
            sql = 'select userid from user_id limit %s, 100' % self.location # 每次处理100个用户
            self.check_status()
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            if result:
                userids = self.tuple_to_list(result)
                try:
                    users_info = api.lookup_users(user_ids=userids)
                    self.api_error_times = 0
                except:
                    self.api_error_times = self.api_error_times + 1
                    if self.api_error_times % 10 == 0:
                        self.api_error_times = 0
                        print('something goes wrong with lookup_users api, we will retry it in 300s')
                        time.sleep(300)
                else:
                    for user_info in users_info:
                        info = user_info._json
                        userid = info['id']
                        username = info['name']
                        userscreenname = info['screen_name']
                        description = info['description'].replace('\"', '')
                        createat = info['created_at']
                        createat_timestamp = datetime.datetime.strptime(createat, '%a %b %d %H:%M:%S +0000 %Y')
                        createat = createat_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                        url = info['url']
                        profileimageurl = info['profile_image_url']
                        profilebackgroundimageurl = info['profile_background_image_url']
                        location = info['location']
                        timezone = info['time_zone']
                        accesslevel = 0
                        statuscount = info['statuses_count']
                        followerscount = info['followers_count']
                        friendscount = info['friends_count']
                        favouritescount = info['favourites_count']
                        listedcount = info['listed_count']
                        isprotected = info['protected']
                        isgeoenabled = info['geo_enabled']
                        isshowallinlinemedia = False
                        iscontributorsenable = info['contributors_enabled']
                        isfollowrequestsent = info['follow_request_sent']
                        isprofilebackgroundtiled = info['profile_background_tile']
                        isprofileusebackgroundtiled = False
                        istranslator = info['is_translator']
                        isverified = info['verified']
                        vtcoffset = info['utc_offset']
                        lang = info['lang']
                        biggerprofileimageurl = info['profile_image_url']
                        biggerprofileimageurlhttps = 'NULL'
                        miniprofileimageurl = 'NULL'
                        miniprofileimageurlhttps = 'NULL'
                        originalprofileimageurl = 'NULL'
                        originalprofileimageurlhttps = 'NULL'
                        profilebackgroundimageurlhttps = info['profile_background_image_url_https']
                        profilebanneripadurl = 'NULL'
                        profilebanneripadretinaurl = 'NULL'
                        profilebannermobileurl = 'NULL'
                        profilebannermobileretinaurl = 'NULL'
                        profilebannerretinaurl = 'NULL'
                        profilebannerurl = 'NULL'
                        profileimageurlhttps = info['profile_image_url_https']
                        updatetime = datetime.date.today()
                        sensitivity = 0
                        sensitivity2 = 0

                        sql = 'insert into user_info (userid, username, userscreenname, description, createat, url, ' \
                              'profileimageurl, profilebackgroundimageurl, location, timezone, accesslevel, statuscount, ' \
                              'followerscount, friendscount, favouritescount, listedcount, isprotected, isgeoenabled, ' \
                              'isshowallinlinemedia, iscontributorsenable, isfollowrequestsent, isprofilebackgroundtiled, ' \
                              'isprofileusebackgroundtiled, ' \
                              'istranslator, isverified, vtcoffset, lang, biggerprofileimageurl, biggerprofileimageurlhttps, ' \
                              'miniprofileimageurl, miniprofileimageurlhttps, originalprofileimageurl, originalprofileimageurlhttps, ' \
                              'profilebackgroundimageurlhttps, profilebanneripadurl, profilebanneripadretinaurl, ' \
                              'profilebannermobileurl, profilebannermobileretinaurl, profilebannerretinaurl, profilebannerurl, ' \
                              'profileimageurlhttps, updatetime, sensitivity, sensitivity2) value (%s, "%s", "%s", "%s", "%s", "%s", "%s", ' \
                              '"%s", "%s", "%s", %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "%s", "%s", %s, %s, %s, %s, ' \
                              '%s, "%s", %s, %s, %s, %s, %s, %s, "%s", "%s", %s, %s)' % (
                                  userid, username, userscreenname, description, createat, url, profileimageurl,
                                  profilebackgroundimageurl, location, timezone, accesslevel,
                                  statuscount, followerscount, friendscount, favouritescount, listedcount, isprotected,
                                  isgeoenabled, isshowallinlinemedia,
                                  iscontributorsenable, isfollowrequestsent, isprofilebackgroundtiled,
                                  isprofileusebackgroundtiled,
                                  istranslator, isverified, vtcoffset,
                                  lang, biggerprofileimageurl, biggerprofileimageurlhttps, miniprofileimageurl,
                                  miniprofileimageurlhttps, originalprofileimageurl,
                                  originalprofileimageurlhttps, profilebackgroundimageurlhttps, profilebanneripadurl,
                                  profilebanneripadretinaurl, profilebannermobileurl,
                                  profilebannermobileretinaurl, profilebannerretinaurl, profilebannerurl, profileimageurlhttps,
                                  updatetime, sensitivity, sensitivity2)
                        sql = sql.replace('\"None\"', 'NULL').replace('None', 'NULL')
                        self.check_status()
                        # 这里用try和except是为了防止sql语句格式错误导致程序崩溃。毕竟我也很难预料到会出现什么奇怪的字符！
                        try:
                            self.cursor.execute(sql)
                        except:
                            print(sql)
                    self.db.commit()
                    self.location = self.location + 100
                    self.save_checkpoints_info(self.location)
            else:
                flag = 0
                self.save_checkpoints_info(-1)
                print('get all users info, all jobs done')

    # 获取所有的user_id
    def get_user_id(self):
        # 从checkpoints中读取出数据
        sql = 'select checkpoints from checkpoints_id'
        self.check_status()
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        if result:
            if result == -1:
                print('已经获取了全部user_id')
                return
            else:
                self.location = result[0][0]
        else:
            self.location = self.location

        # 开始迭代遍历user_id中的用户
        flag = 1
        while flag:
            sql = 'select userid from user_id limit %s, 1' % self.location
            self.check_status()
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            if result:
                userid = result[0][0]
                print('将要处理id:' + str(userid))
                if functions == 'whole_net_chinese':
                    self.search_id_chinese(userid)
                if functions == 'whole_net':
                    self.search_id(userid)
                self.location = self.location + 1
                self.save_checkpoints_id(self.location)
            else:
                flag = 0
                self.save_checkpoints_id(-1)
        print('已经获取了全部user_id')

    # 判断是否是中国人
    def is_chinese(self, id):
        # 使用正则表达式判断文本内容中是否有中文字符，python3 的str本身就是unicode编码，所以不用decode()
        # 判断是否是中文的原则就是，最近的10条推文中，出现过某条推文中包含中文但是不包含日文，因为日文中某些字与中文一致
        pattern_zh = re.compile(u'[\u4e00-\u9fa5]+')
        pattern_ja_ka = re.compile(u'[\u30a0-\u30ff]+')
        pattern_ja_hi = re.compile(u'[\u3040-\u309f]+')
        api = self.get_random_api()
        try:
            tweets = api.user_timeline(id=id, count=10)
            self.api_error_times = 0
            for tweet in tweets:
                text = tweet.text
                # print(tweet.text)
                if pattern_zh.search(text) and not pattern_ja_hi.search(text) and not pattern_ja_ka.search(text):
                    print(text)
                    print('包含中文，不包含日文')
                    return True
                else:
                    pass
        except:
            self.api_error_times = self.api_error_times + 1
            if self.api_error_times % 10 == 0:
                self.api_error_times = 0
                print('failed to get the tweets, so we take it as no chinese!, but we will take 300s for break.')
                time.sleep(300)
        return False

    # 保存id的checkpoints
    def save_checkpoints_id(self, num):
        sql = 'update checkpoints_id set checkpoints = %s' % num
        self.check_status()
        self.cursor.execute(sql)
        self.db.commit()

    # 保存info的checkpoints
    def save_checkpoints_info(self, num):
        sql = 'update checkpoints_info set checkpoints = %s' % num
        self.check_status()
        self.cursor.execute(sql)
        self.db.commit()

    # tuple 转 list
    def tuple_to_list(self, tuple):
        list = []
        for i in tuple:
            list.append(i[0])
        return list

    # check status
    def check_status(self):
        try:
            self.db.ping()
        except:
            print("lost connection to database server, we need to reconnect!")
            self.db = pymysql.connect(
                host=host,
                port=3306,
                user='root',
                passwd='fit4-305',
                db=db,
                charset='utf8'
            )
            self.cursor = self.db.cursor()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='argpaesr for twitter whole net spider')
    parser.add_argument('functions', type=str, default='', help='choose functions, whole_net/whole_net_chinese')
    parser.add_argument('-host', type=str, default='127.0.0.1', help='host ip for database')
    parser.add_argument('-database', type=str, default='twitter_users', help='database name for data to save')
    parser.add_argument('-proxy', type=str, default='127.0.0.1:1080', help='proxy for the spider, a str')
    parser.add_argument('-name', type=str, default='', help='name for the spider, must be a str, not null')
    parser.add_argument('-length', type=int, default=10000000, help='length of the bitarray')
    parser.add_argument('-number', type=int, default=7, help='number of hash functions to use')
    parser.add_argument('-save_frequency', type=int, default=1000, help='save frequency')

    args = parser.parse_args()
    functions = args.functions
    host = args.host
    db = args.database
    proxy = args.proxy
    name = args.name
    length = args.length
    number = args.number
    save_frequency = args.save_frequency

    spider = TwitterApiSpider(seed=749904886746066944)
    spider.get_user_id()
    spider.get_user_info()
