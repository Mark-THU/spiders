# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class TwitterSpiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    tweetid = scrapy.Field()
    userid = scrapy.Field()
    username = scrapy.Field()
    tweettext = scrapy.Field()
    createdat = scrapy.Field()
    accesslevel = scrapy.Field()
    replycount = scrapy.Field()
    favoritecount = scrapy.Field()
    retweetcount = scrapy.Field()
    retweetedstatusid = scrapy.Field()
    retweetedstatustext = scrapy.Field()
    geolocation = scrapy.Field()
    place = scrapy.Field()
    replytouserid = scrapy.Field()
    replytoscreenname = scrapy.Field()
    replytostatusid = scrapy.Field()
    contributor = scrapy.Field() # none
    currentuserretweetid = scrapy.Field() # -1
    source = scrapy.Field() # 1
    isfavorited = scrapy.Field() # false
    isretweetbyme = scrapy.Field() # false
    istruncated = scrapy.Field() # false
    isretweet = scrapy.Field()
    ispossiblysensitive = scrapy.Field() # 0
    hashtagentities = scrapy.Field() # none
    sensitivity = scrapy.Field() # 0
    sensitivity2 = scrapy.Field() # 0
    updatetime = scrapy.Field()
