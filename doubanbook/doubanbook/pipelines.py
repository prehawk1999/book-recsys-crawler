# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pymongo
from pymongo import MongoClient
from scrapy.exceptions import DropItem

conn = pymongo.Connection('localhost',27017) 

class DoubanbookPipeline(object):

    def process_item(self, item, spider):
    	pass

    def open_spider(self, spider):
    	db = conn.group_mems
    	print db


    def close_spider(self, spider):
    	pass