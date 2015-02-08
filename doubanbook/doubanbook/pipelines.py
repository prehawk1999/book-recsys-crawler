# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pymongo
import datetime
from pymongo import MongoClient
from scrapy.exceptions import DropItem

from doubanbook.spiders import *
from doubanbook.items import *


conn = MongoClient('localhost',27017) 
db = conn.group_mems

class DoubanbookPipeline(object):

    def __init__(self):
        self.still_start_urls = []
        pass

    def process_item(self, item, spider):
        if isinstance(item, MemberItem):
            updoc = {"user_id":item['user_id'], "crawled":item['crawled'], "read":item['read'], "uptime": datetime.datetime.utcnow()}
            db.users.update({"user_id":item['user_id']}, updoc, upsert=True)
        elif isinstance(item, RateItem):
            db.users.update({"user_id":item['user_id']}, {"$addToSet":{"history":dict(item)}})
        elif isinstance(item, BookItem):
            db.books.update({'book_id':item['info']['id']}, dict(item['info']), upsert=True)
        elif isinstance(item, HistoryItem):
            # for i in range(1, 5):
            #     url = self.still_start_urls.pop()
            #     spider.start_urls.append(url)
            pass

    def open_spider(self, spider):
        users_in_db = db.users.find({})
        if isinstance(spider, group_mems.GroupMemsSpider):   
            for u in users_in_db:
                spider.users.add(u['user_id'])
            spider.prime_size = len(spider.users)
        elif isinstance(spider, user_books.UserBooksSpider):
            for u in users_in_db: 
                delta = datetime.datetime.utcnow() - u['uptime'] 
                if u['crawled'] == 0 or ( delta.days > 0 and u['read'] > 15 ):
                    url = 'http://book.douban.com/people/%s/' % u['user_id']
                    spider.start_urls.append(url)
                    #self.log('%s' % url, level=scrapy.log.INFO)
                    #print url
                    if 'history' in u:
                        #print 'collects %d/%d' % ( len(u['history']), u['read'] )
                        spider.history[u['user_id']] = len(u['history'])
                    elif u['crawled'] == 0:
                        spider.history[u['user_id']] = -1
                    else:
                        #print ' start_users_read: %d' % u['read']
                        spider.history[u['user_id']] = 0

                    # if spider.history[u['user_id']] - u['read'] != 0:
                    #     print '%d, collects %d/%d' % (u['crawled'], spider.history[u['user_id']], u['read'])
            # 剩下的重试很多次还存在的初始链接应该是不存在用户了            
            print '=== start_urls: %d ===' % len(spider.start_urls)
            books_in_db = db.books.find({}, {"id":1})
            for b in books_in_db:
                spider.books.add(b['id'])

    def close_spider(self, spider):
        pass