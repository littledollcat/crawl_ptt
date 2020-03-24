# -*- coding: utf-8 -*-
import scrapy
from scrapy.exceptions import CloseSpider
from scrapy_redis.spiders import RedisCrawlSpider, RedisSpider
from ..items import PostItem, CommentItem

import re
from datetime import datetime

import pymysql
import redis

class PttSpider(RedisSpider):
    name = 'ptt'
    redis_key = 'ptt'

    allowed_domains = ['www.ptt.cc']
    ptt_url = 'https://www.ptt.cc'

    startTime = datetime.now()
    endTime = datetime.now()

    def __init__(self, category='', startTime='',endTime='',*args,  **kwargs):
        start_urls = f'{self.ptt_url}/bbs/{category}/index.html'
        
        #init first target url
        r = redis.Redis(host='redis', port=6379, decode_responses=True)
        if r.dbsize() < 1:
            r.lpush(self.redis_key,str(start_urls))
        
        #get time range
        self.startTime = datetime.strptime(startTime, '%Y-%m-%d')
        endTime = datetime.strptime(endTime, '%Y-%m-%d')
        self.endTime =  endTime if endTime < self.endTime else self.endTime

        super(PttSpider,self).__init__(*args, **kwargs)
    
    def parse(self, response):
        #get last page
        lastPageHref = response.css('a.btn.wide::attr(href)').extract()[1]

        for index, q in enumerate(response.css('div.r-ent')):
            postTime_str = ''
            href = str(q.css('div.title > a::attr(href)').extract_first())
            
            #if post exist, have href
            if href != 'None':
                postTime_str = re.search(r'M.(\d+).',href).group(1)
                    
                if postTime_str:
                    postTime = datetime.fromtimestamp(int(postTime_str))
                    #mean post's publishedTime over endTime, call last page and stop to crawl
                    if postTime > self.endTime:
                        yield scrapy.Request(self.ptt_url + lastPageHref, callback=self.parse, dont_filter= True)
                        break
                    if postTime > self.startTime and postTime < self.endTime:
                        #check if page need to call last page
                        if index == 0:
                            yield scrapy.Request(self.ptt_url + lastPageHref, callback=self.parse, dont_filter= True)
                        yield scrapy.Request(self.ptt_url + href, callback=self.parse_post, dont_filter= True)
                    elif postTime < self.startTime:
                        continue
                else:
                    self.logger.error('Get postTime fail on' + href)
    
    def parse_post(self, response):
        #start to parse post
        item = PostItem()
        item['canonicalUrl'] = response.url
        item['title'] = response.xpath(
            '//meta[@property="og:title"]/@content')[0].extract()
        item['authorId'] = response.xpath(
            '//div[@class="article-metaline"]/span[text()="作者"]/following-sibling::span[1]/text()')[
                0].extract().split(' ')[0]
        item['authorName'] = response.xpath(
            '//div[@class="article-metaline"]/span[text()="作者"]/following-sibling::span[1]/text()')[
                0].extract().split(' ')[1][1:-1]
        datetime_str = response.xpath(
            '//div[@class="article-metaline"]/span[text()="時間"]/following-sibling::span[1]/text()')[
                0].extract()
        
        item['publishedTime'] = datetime.strptime(datetime_str, '%a %b %d %H:%M:%S %Y')
        post_year = item['publishedTime'].year
        item['content'] = ''.join([i.extract() for i in response.xpath('//div[@id="main-content"]/text()')])
        
        #if the post doesn't need, drop it
        if item['publishedTime'] > self.endTime:
            del item
            raise CloseSpider('over time to close it!')
            return None
        yield item

        #crawl comment
        for index, push_m in enumerate(response.css('div.push')):
            comment_item = CommentItem()
            comment_item['canonicalUrl'] = response.url
            comment_item['commentId'] = push_m.css('span.push-userid::text').extract_first()
            comment_item['commentContent'] = ''.join([m.extract() for m in push_m.css('span.push-content::text')])
            
            datetime_str = push_m.css('span.push-ipdatetime::text').extract_first()
            datetime_str = str(post_year)+'/'+datetime_str.strip('\n')[1:]

            #record comment count
            comment_item['commentOrder'] = index+1

            comment_item['commentTime'] = datetime.strptime(datetime_str, '%Y/%m/%d %H:%M')
            
            yield comment_item
            
    