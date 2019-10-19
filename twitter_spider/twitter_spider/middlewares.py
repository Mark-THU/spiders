# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html
from scrapy import signals
import random
from twitter_spider.user_agents import agents
from twitter_spider.settings import PROXIES
import base64
import time


class UserAgentMiddleware(object):
    """ 换User-Agent """

    def process_request(self, request, spider):
        agent = random.choice(agents)
        print("新的agent为：" + agent)
        request.headers["User-Agent"] = agent


class CheckMiddleware(object):
    """ 检测状态码,解决302重定向 """

    def process_response(self, request, response, spider):
        response_status = response.status
        response_url = response.url
        response_check_url = response_url.split('/')[2]
        request_url = request.url
        '''request_url_id_max = re.compile('max_position=([\w-]*)').findall(request_url)
        request_url_id_min = re.compile('min_position=([\w-]*)').findall(request_url)
        if len(request_url_id_max) > 0:
            request_id = request_url_id_max[0]
            request_url = "https://twitter.com/i/search/timeline?vertical=default&q=" + key_word\
                          + date_since + date_until + "&l=" + language + \
                          "&src=typd&include_available_features=1&include_entities=1&max_position=" + request_id + "&reset_error_state=false"
        else:
            if len(request_url_id_min) > 0:
                request_id = request_url_id_min[0]
                request_url = "https://twitter.com/i/search/timeline?vertical=default&q=" + key_word\
                              + date_since + date_until + "&l=" + language + \
                              "&src=typd&include_available_features=1&include_entities=1&min_position=" + request_id + "&reset_error_state=false"
                              '''
        request_url = request_url.replace('mobile.', '')
        print(request_url)

        if response_status == 200 and response_check_url == 'twitter.com':
            print("通过重定向检测，爬虫运行正常")
            return response
        else:
            print("遭遇反爬虫,休息5秒")
            clock = 0
            sleep_time = 5
            while (clock < sleep_time):
                nclock = sleep_time - clock
                print(nclock)
                time.sleep(1)
                clock += 1
            print('爬虫重新启动中')
            return request.replace(url=request_url)


class ProxyMiddleware(object):

    def process_request(self, request, spider):
        proxy = random.choice(PROXIES)
        if proxy['user_pass'] is not None:
            request.meta['proxy'] = "http://%s" % proxy['ip_port']
            encoded_user_pass = base64.b64encode(proxy['user_pass'].encode())
            request.headers['Proxy-Authorization'] = 'Basic ' + encoded_user_pass.decode()
            # request.headers['Proxy-Authorization'] = 'Basic ' + proxy['user_pass']
            print("使用代理为：" + proxy['ip_port'])
        else:
            print("使用代理为：" + proxy['ip_port'])
            request.meta['proxy'] = "http://%s" % proxy['ip_port']


class TwitterSpiderSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class TwitterSpiderDownloaderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
