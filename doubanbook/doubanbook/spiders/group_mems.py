# -*- coding: utf-8 -*-
import scrapy
import pymongo
import json

from scrapy.selector import Selector
from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.contrib.linkextractors import LinkExtractor
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.contrib.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.utils.url import urljoin_rfc
from doubanbook.items import MemberItem, GroupItem

# 填入需要抓取的小组id
ids = ['dm'] # MLPR 27885 BigData dm 503043

class GroupMemsSpider(CrawlSpider):
    
    name = "group_mems"
    allowed_domains = ["douban.com"]
    start_urls = ['http://www.douban.com/group/%s/members' % x for x in ids]
    rules = [
            # Rule(LinkExtractor(allow=(r'/group/\w+/members', )), callback='get_members', follow=True),
            # 小组翻页规则
            Rule(LxmlLinkExtractor(allow = (r'/group/\w+/members\?start=\d+.*', ), unique = False), 
                callback = 'parse_group_page', follow = True,
                process_links = lambda links: [x for x in links if x.text == u'后页>']),
            
            # 书籍页翻页规则
            # http://book.douban.com/people/lovelyboy/collect?start=15&amp;sort=time&amp;rating=all&amp;filter=all&amp;mode=grid
            # Rule(LxmlLinkExtractor(allow = (r'/people/\w+/(collect|wish|do)\?start=\d+.*', ), unique = False), 
            #   callback = 'parse_user_collect', follow = True,
            #   process_links = 'process_book_page'),
        ]

    users = dict()
    books = set()
    user_count = 0

    def get_members(self, response):
        sel = Selector(response) 
        #print self.users, self.books
        for mem in sel.xpath('//div[@class="member-list"]//div[@class="name"]/a/@href'):
            m = mem.re('http://www.douban.com/group/people/(\w+)')
            if m: 
                user = {}
                user['book_count'] = 0
                user['book'] = []
                if m[0] not in self.users:
                    self.users[m[0]] = user
        self.log('======= %d / %d members fetched!' % (len(self.users) - 1, self.user_count), 
            level=scrapy.log.INFO)

    def parse_start_url(self, response):
        sel = Selector(response)
        # self.group_id = sel.xpath('//*[@id="content"]//div[@class="title"]/a/@href').re(r'http://www.douban.com/group/(\w+)/.*')[0]
        self.user_count = int(sel.xpath('//*[@id="content"]//span[@class="count"]/text()').re('(\d+)')[0])
        self.get_members(response)

    def parse_group_page(self, response):
        self.get_members(response)
        #if self.user_count - len(self.users) <= 10:
        for i in self.users:
            url = 'http://book.douban.com/people/%s/' % i
            #self.log( url, level=scrapy.log.DEBUG )
            req = scrapy.Request(url=url, callback=self.parse_user_home)
            req.meta['user_id'] = i
            yield req

    def parse_user_home(self, response):
        meta = response.meta
        sel = Selector(response)
        read = sel.xpath('//*[@id="content"]//*[@class="number-accumulated"]//*[@class="number"]/text()').extract()
        if read and int(read[0]) >= 15: 
            #print int(read[0])
            self.users[meta['user_id']]['book_count'] = int(read[0])
            req = scrapy.Request(url=response.url + 'collect', 
                callback=self.parse_user_collect)
            req.meta['user_id'] = meta['user_id']
            yield req
        else:
            del self.users[meta['user_id']]
    
    def parse_user_collect(self, response):
        meta = response.meta
        sel = Selector(response)
        for sub in sel.xpath('//*[@id="content"]//ul[@class="interest-list"]//li[@class="subject-item"]'):
            book_id = sub.xpath('*[@class="pic"]/a/@href').re(r'http://book.douban.com/subject/(\w+)/')[0]
            self.users[meta['user_id']]['book'].append(book_id)
            if book_id not in self.books:
                self.books.add(book_id)
                req = scrapy.Request(url='https://api.douban.com/v2/book/%s' % book_id, 
                    callback=self.parse_book_page)
                req.meta['user_id'] = meta['user_id']
                req.meta['book_id'] = book_id
                yield req
            else:
                self.log('@%s@ reads <<%s>>' % (meta['user_id'], book_id) ,level=scrapy.log.DEBUG)

            rate = sub.xpath('*[@class="info"]/*[@class="short-note"]//span[1]/@class').re(r'\w+?([1-5])-t')
            if rate:
                rate = rate[0]
            else:
                rate = 0
            date = sub.xpath('*[@class="info"]/*[@class="short-note"]//*[@class="date"]/text()').re(r'(\d{4}-\d{2}-\d{2})\s+\w+')[0]
            tags = sub.xpath('*[@class="info"]/*[@class="short-note"]//*[@class="tags"]/text()').re(r'.*: (.*)')
            if tags:
                tags = tags[0].split(' ')
            comment = sub.xpath('*[@class="info"]/*[@class="short-note"]/p/text()').extract()
            if comment == [u'\n  ']:
                comment = None
            else:
                comment = comment[0]
            self.log('%s +++ %s +++ %s +++ %s +++ %s' % (book_id, rate, date, tags, comment), 
                level=scrapy.log.DEBUG)

        # m = sel.re('/people/\w+/(collect|wish|do)\?start=\d+.*')
        fetched_books = len(self.users[meta['user_id']]['book'])
        total_books = self.users[meta['user_id']]['book_count']
        if fetched_books == total_books:
            self.log( '@%s@ done !' % meta['user_id'],  
                    level=scrapy.log.INFO )
        else:
            self.log('@%s@\'s collecting(%d/%d)' % (meta['user_id'], int(fetched_books), int(total_books)), 
                level=scrapy.log.INFO )
            
        # 翻页
        next_page = sel.xpath('//*[@id="content"]//span[@class="next"]/a/@href').extract()
        if next_page: 
            req = scrapy.Request(url=next_page[0], callback=self.parse_user_collect)
            req.meta['user_id'] = meta['user_id']
            yield req


    def parse_book_page(self, response):
        meta = response.meta
        book_info = json.loads(response.body)

