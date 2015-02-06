# -*- coding: utf-8 -*-
import re
import time
import scrapy

import json
from twisted.internet import defer, reactor
from scrapy.selector import Selector
from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.contrib.linkextractors import LinkExtractor
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.contrib.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.utils.url import urljoin_rfc
from doubanbook.items import *


class UserBooksSpider(CrawlSpider):

    name = "user_books"
    allowed_domains = ["douban.com"]
    start_urls = []
    rules = []

    books = set()
    history = dict() # record the amount of collects of a user that has been crawled.

    def parse_start_url(self, response):
        sel = Selector(response)
        read = sel.xpath('//*[@id="content"]//*[@class="number-accumulated"]//*[@class="number"]/text()').extract()
        #d = defer.Deferred()
        #reactor.callLater(pause, d.callback, (x, pause))
        mem = MemberItem()  
        user_id = re.search(r'http://book.douban.com/people/(\w+)/', response.url).group(1)
        mem['user_id'] = user_id
        mem['crawled'] = 1
        #time.sleep(2)
        if read and int(read[0]) > self.history[user_id]:
            if self.history[user_id] == -1:
                self.log('Add @ %s with %d collects.' % (user_id, int(read[0])), 
                    level=scrapy.log.INFO)
            else:
                self.log('Refresh @ %s with %d collects(%d recorded).' % (user_id, int(read[0]), self.history[user_id]), 
                    level=scrapy.log.INFO)
            mem['read'] = int(read[0]) # 更新阅读量
            req = scrapy.Request(url=response.url + 'collect', 
                callback=self.parse_user_collect)
            req.meta['user_id'] = user_id
            yield req
        else:
            if self.history[user_id] > 0:
                mem['read'] = self.history[user_id]    # value not change.        
            self.log('Ignore @ %s (saved)' % user_id, 
                level=scrapy.log.INFO)
        yield mem 


    def parse_user_collect(self, response):
        meta = response.meta
        sel = Selector(response)
        collect = []
        for sub in sel.xpath('//*[@id="content"]//ul[@class="interest-list"]//li[@class="subject-item"]'):
            book_id = sub.xpath('*[@class="pic"]/a/@href').re(r'http://book.douban.com/subject/(\w+)/')[0]
            #self.users[meta['user_id']]['book'].append(book_id)
            rt = RateItem()
            rt['user_id'] = meta['user_id']
            rt['book_id'] = book_id
            if book_id not in self.books:
                self.log('Add New book: << %s >>' % book_id, level=scrapy.log.INFO)
                self.books.add(book_id)
                req = scrapy.Request(url='https://api.douban.com/v2/book/%s' % book_id, 
                    callback=self.parse_book_page)
                req.meta['book_id'] = book_id
                yield req

            collect.append(book_id)

            rate = sub.xpath('*[@class="info"]/*[@class="short-note"]//span[1]/@class').re(r'\w+?([1-5])-t')
            if rate:
                rt['rate'] = rate[0]
            else:
                rt['rate'] = 0
            date = sub.xpath('*[@class="info"]/*[@class="short-note"]//*[@class="date"]/text()').re(r'(\d{4}-\d{2}-\d{2})\s+\w+')[0]
            if date:
                rt['date'] = date
            else:
                self.log('parse_user_collect date parse error!', 
                    level=scrapy.log.WARNING)
            tags = sub.xpath('*[@class="info"]/*[@class="short-note"]//*[@class="tags"]/text()').re(r'.*: (.*)')
            if tags:
                rt['tags'] = tags[0].split(' ')

            comment = sub.xpath('*[@class="info"]/*[@class="short-note"]/p/text()').extract()
            if comment == [u'\n  ']:
                rt['comment'] = None
            else:
                rt['comment'] = comment[0]

            yield rt

        
        self.log(' %d rate history by @ %s' % (len(collect), meta['user_id']), 
                level=scrapy.log.INFO)
        self.history[meta['user_id']] += len(collect)

        # 翻页
        next_page = sel.xpath('//*[@id="content"]//span[@class="next"]/a/@href').extract()
        if next_page: 
            req = scrapy.Request(url=next_page[0], callback=self.parse_user_collect)
            req.meta['user_id'] = meta['user_id']
            yield req
        else:
            # End of the pages.
            self.log(' === End @ %s with %d new collects(shown below) ===' % (meta['user_id'], len(collect)), 
                level=scrapy.log.INFO)
            self.log(' '.join(collect),
                level=scrapy.log.INFO)
            ht = HistoryItem()
            ht['user_id']  = meta['user_id']
            ht['errstr']   = ''
            ht['collects'] = len(collect)
            print repr(ht)
            yield ht

    def parse_book_page(self, response):
        meta = response.meta
        bt = BookItem()
        book_info = json.loads(response.body)
        if book_info:
            bt['info'] = book_info
            yield bt
        else:
            self.log('<< %s >> info not complete!' % meta['book_id'],
                leve=scrapy.log.WARNING)
