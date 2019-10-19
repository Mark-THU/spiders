# -*- coding: utf-8 -*-

import json
import re
from bs4 import BeautifulSoup
from scrapy import Request
from scrapy.spiders import CrawlSpider
import datetime

from twitter_spider.items import TwitterSpiderItem
from begin import key_word, topic, language, date_since, date_until, from_acount, to_acount, mention_acount


class twitter_spider(CrawlSpider):
    name = 'twitter_spider_start'
    counter = 0
    query = ''

    def start_requests(self):
        self.query = self.query + '&l=' + language + '&q='
        if key_word != '':
            self.query = self.query + key_word.replace(' ', '%20')
        if topic != '':
            self.query = self.query + '%20%23' + topic
        if from_acount != '':
            self.query = self.query + '%20from%3A' + from_acount
        if to_acount != '':
            self.query = self.query + '%20to%3A' + to_acount
        if mention_acount != '':
            self.query = self.query + '%20%40' + mention_acount
        if date_since != '':
            self.query = self.query + '%20since%3A' + date_since
        if date_until != '':
            self.query = self.query + '%20until%3A' + date_until

        start_url = 'https://twitter.com/search?f=tweets&vertical=default' + self.query + '&src=typd'
        print("爬虫初始化中...\n")
        print("URL:" + start_url + "\n")
        yield Request(url=start_url, callback=self.parse)

    def parse(self, response):
        item = TwitterSpiderItem()
        try:
            sites = json.loads(response.body_as_unicode())
        except:
            data = response.body.decode()
            soup = BeautifulSoup(data, 'lxml')
            max_position = soup.find('div', class_='stream-container')['data-max-position']
        else:
            data = sites['items_html']
            max_position = sites['min_position']
            soup = BeautifulSoup(data, 'lxml')

        if data == "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n \n":
            cmd = "%s 推文 获取完成" % key_word
            cmd_len = len(cmd)
            print("#" * cmd_len)
            print(cmd)
            print("#" * cmd_len)
            return

        twitters = soup.find_all('li', class_='js-stream-item stream-item stream-item')
        print("目前已经爬取数据量为： %s" % self.counter)
        print("收到新数据！")
        for each in twitters:
            tweetid = each['data-item-id']
            userid = each.find('div', class_='stream-item-header').a['data-user-id']
            username = each.find('span', class_='username u-dir u-textTruncate').get_text(strip=True)
            twitter_content_tag = each.find('div', class_='js-tweet-text-container')
            for tag in twitter_content_tag.find_all('a', class_='twitter-timeline-link'):
                tag.clear()
            twitter_content = ''
            for string in twitter_content_tag.strings:
                twitter_content = twitter_content + string

            twitter_content = twitter_content.replace('\n', ' ').replace('\"', ' ').replace('#', ''). \
                replace('@', '')
            tweettext = re.sub(r'\s{2,}', ' ', twitter_content)
            createdat = each.find('a', class_='tweet-timestamp js-permalink js-nav js-tooltip')['title']
            accesslevel = 1

            replycount = each.find('button', class_='ProfileTweet-actionButton js-actionButton js-actionReply') \
                .find('span', class_='ProfileTweet-actionCount').get_text(strip=True)
            if replycount == '':
                replycount = 'NULL'

            favoritecount = each.find('button', class_='ProfileTweet-actionButton js-actionButton js-actionFavorite')\
                .find('span', class_='ProfileTweet-actionCount').get_text(strip=True)
            if favoritecount == '':
                favoritecount = 'NULL'
            retweetcount = each.find('button', class_='ProfileTweet-actionButton js-actionButton js-actionRetweet')\
                .find('span', class_='ProfileTweet-actionCount').get_text(strip=True)
            if retweetcount == '':
                retweetcount = 'NULL'
            if each.find('div', class_='QuoteTweet-container') is not None:
                retweetedstatusid = each.find('div', class_='QuoteTweet-container').\
                    find('div', class_='QuoteTweet-innerContainer u-cf js-permalink js-media-container')['data-item-id']
                retweetedstatustext = each.find('div', class_='QuoteTweet-container').\
                    find('div', class_='QuoteTweet-text tweet-text u-dir js-ellipsis').get_text(strip=True)\
                    .replace('\"', ' ')
            else:
                retweetedstatusid = 'NULL'
                retweetedstatustext = ''
            geolocation = ''
            place = ''

            replytouserid = 'NULL'
            replytoscreenname = ''
            tmp = each.find('div', class_='ReplyingToContextBelowAuthor')
            if tmp is not None:
                if tmp.find('a', class_='pretty-link js-user-profile-link') is not None:
                    replytouserid = tmp.find('a', class_='pretty-link js-user-profile-link')['data-user-id']
                if tmp.find('span', class_='username u-dir u-textTruncate') is not None:
                    replytoscreenname = tmp.find('span', class_='username u-dir u-textTruncate').get_text(strip=True)

            replytostatusid = 'NULL'
            contributor = ''
            currentuserretweetid = -1
            source = 1
            isfavorited = 0
            isretweetbyme = 0
            istruncated = 0
            if each.find('div', class_='QuoteTweet-container') is not None:
                isretweet = 1
            else:
                isretweet = 0
            ispossiblysensitive = 0
            hashtagentities = ''
            if each.find_all('a', class_='twitter-hashtag pretty-link js-nav') != []:
                for tag in each.find_all('a', class_='twitter-hashtag pretty-link js-nav'):
                    hashtagentities = hashtagentities + tag.get_text(strip=True)

            sensitivity = 0
            sensitivity2 = 0
            updatetime = datetime.date.today()

            item['tweetid'] = tweetid
            item['userid'] = userid
            item['username'] = username
            item['tweettext'] = tweettext
            item['createdat'] = createdat
            item['accesslevel'] = accesslevel
            item['replycount'] = replycount
            item['favoritecount'] = favoritecount
            item['retweetcount'] = retweetcount
            item['retweetedstatusid'] = retweetedstatusid
            item['retweetedstatustext'] = retweetedstatustext
            item['geolocation'] = geolocation
            item['place'] = place
            item['replytouserid'] = replytouserid
            item['replytoscreenname'] = replytoscreenname
            item['replytostatusid'] = replytostatusid
            item['contributor'] = contributor
            item['currentuserretweetid'] = currentuserretweetid
            item['source'] = source
            item['isfavorited'] = isfavorited
            item['isretweetbyme'] = isretweetbyme
            item['istruncated'] = istruncated
            item['isretweet'] = isretweet
            item['ispossiblysensitive'] = ispossiblysensitive
            item['hashtagentities'] = hashtagentities
            item['sensitivity'] = sensitivity
            item['sensitivity2'] = sensitivity2
            item['updatetime'] = updatetime

            yield item
            self.counter += 1
        next_url = "https://twitter.com/i/search/timeline?f=tweets&vertical=default" + self.query + \
                   "&src=typd&composed_count=0&include_available_features=1&include_entities=1&max_position=" \
                   + max_position + "&oldest_unread_id=0"
        print("爬取下一页...\n")
        print("URL:" + next_url + "\n")
        yield Request(url=next_url, callback=self.parse, headers={'Referer': "https://twitter.com/"})