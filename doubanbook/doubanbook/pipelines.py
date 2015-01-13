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
        pass

    def process_item(self, item, spider):
        if isinstance(item, MemberItem):
            updoc = {"user_id":item['user_id'], "crawled":item['crawled'], "read":item['read'], "uptime": datetime.datetime.utcnow()}
            db.users.update({"user_id":item['user_id']}, updoc, upsert=True)
        elif isinstance(item, RateItem):
            db.users.update({"user_id":item['user_id']}, {"$addToSet":{"history":dict(item)}})
        elif isinstance(item, BookItem):
            db.books.update({'book_id':item['info']['id']}, dict(item['info']), upsert=True)

    def open_spider(self, spider):
        users_in_db = db.users.find({})
        if isinstance(spider, group_mems.GroupMemsSpider):   
            for u in users_in_db:
                spider.users.add(u['user_id'])
            spider.prime_size = len(spider.users)
        if isinstance(spider, user_books.UserBooksSpider):
            for u in users_in_db: 
                delta = datetime.datetime.utcnow() - u['uptime']
                if u['crawled'] == 0 or ( delta.days >= 0 and u['read'] > 15 ):
                    url = 'http://book.douban.com/people/%s/' % u['user_id']
                    spider.start_urls.append(url)    
                    if 'history' in u:
                        #print 'collects %d/%d' % ( len(u['history']), u['read'] )
                        spider.history[u['user_id']] = len(u['history'])
                    elif u['crawled'] == 0:
                        spider.history[u['user_id']] = -1
                    else:
                        #print ' start_users_read: %d' % u['read']
                        spider.history[u['user_id']] = 0
                    #print '%d, collects %d/%d' % (u['crawled'], spider.history[u['user_id']], u['read'])
            print '=== start_urls: %d ===' % len(spider.start_urls)
            books_in_db = db.books.find({}, {"id":1})
            for b in books_in_db:
                spider.books.add(b['id'])

    def close_spider(self, spider):
        pass