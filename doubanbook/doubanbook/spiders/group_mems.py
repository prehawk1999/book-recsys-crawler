# -*- coding: utf-8 -*-
import scrapy

from scrapy.selector import Selector
from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.contrib.linkextractors import LinkExtractor
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.contrib.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.utils.url import urljoin_rfc
from doubanbook.items import MemberItem, GroupItem

# 填入需要抓取的小组id
ids = ['BigData'] # MLPR 27885 BigData dm 503043

class GroupMemsSpider(CrawlSpider):
    
    name = "group_mems"
    allowed_domains = ["douban.com"]
    start_urls = ['http://www.douban.com/group/%s/members' % x for x in ids]
    rules = [
            # 小组翻页规则
            Rule(LxmlLinkExtractor(allow = (r'/group/\w+/members\?start=\d+.*', ), unique = False), 
                callback = 'parse_group_page', follow = True,
                process_links = lambda links: [x for x in links if x.text == u'后页>']),
        ]

    users = set() # users = set()
    prime_size = 1
    user_count = 0

    def get_members(self, response):
        sel = Selector(response) 
        #print self.users
        for mem in sel.xpath('//div[@class="member-list"]//div[@class="name"]/a/@href'):
            m = mem.re('http://www.douban.com/group/people/(\w+)')
            if m: 
                if m[0] not in self.users:
                    self.users.add(m[0])
        self.log('======= %d / %d members fetched!' % (len(self.users) - self.prime_size, self.user_count), 
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
            # url = 'http://book.douban.com/people/%s/' % i
            mem = MemberItem()
            mem['user_id'] = i
            mem['read']    = 0
            mem['crawled'] = 0
            yield mem

