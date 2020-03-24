# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.exceptions import DropItem
import pymysql

class DeleteNullTitlePipeline(object):
    def process_item(self, item, spider):
        title = item['title'] 
        if title:
            return item
        else:
            raise DropItem('found null title %s', item)

class DuplicatesTitlePipeline(object):
    def __init__(self):
        self.article = set()
    def process_item(self, item, spider):
        title = item['title'] 
        if title in self.article:
            raise DropItem('duplicates title found %s', item)
        self.article.add(title)
        return(item)

class MySqlPipeline(object):
    old_data_from_sql = []
    def filter_repeat_data(self, item):
        #filter exist
        if item['canonicalUrl'] not in self.old_data_from_sql:
            self.insert_to_mysql(item)

    def open_spider(self, spider):
        # Database Settings
        db = spider.settings.get('MYSQL_DB_NAME')
        host = spider.settings.get('MYSQL_DB_HOST')
        port = spider.settings.get('MYSQL_PORY')
        user = spider.settings.get('MYSQL_USER')
        password = spider.settings.get('MYSQL_PASSWORD')
        # Database Connecting
        self.connection = pymysql.connect(
            host = host,
            user = user,
            password= password,
            db = db,
            cursorclass= pymysql.cursors.DictCursor
        )

        with self.connection.cursor() as cursor:
            sql = '''CREATE TABLE  IF NOT EXISTS `posts` (
                    `title` text COLLATE utf8mb4_unicode_ci,
                    `authorId` text COLLATE utf8mb4_unicode_ci,
                    `authorName` text COLLATE utf8mb4_unicode_ci,
                    `publishedTime` datetime DEFAULT NULL,
                    `content` text COLLATE utf8mb4_unicode_ci,
                    `canonicalUrl` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
                    `createdTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    `updatedTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (`canonicalUrl`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
                    '''
            cursor.execute(sql)
            sql = '''CREATE TABLE  IF NOT EXISTS `comments` (
                    `commentId` text COLLATE utf8mb4_unicode_ci,
                    `commentContent` text COLLATE utf8mb4_unicode_ci,
                    `canonicalUrl` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                    `commentOrder` text COLLATE utf8mb4_unicode_ci,
                    `commentTime` datetime DEFAULT NULL,
                    KEY `canonicalUrl` (`canonicalUrl`),
                    CONSTRAINT `comments_ibfk_1` FOREIGN KEY (`canonicalUrl`) REFERENCES `posts` (`canonicalUrl`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
                    '''
            cursor.execute(sql)
            
                    
        with self.connection.cursor() as cursor:
            sql = "SELECT canonicalUrl FROM posts"
            cursor.execute(sql)
            self.old_data_from_sql = [row['canonicalUrl'] for row in cursor]
            
    def close_spider(self, spider):
        self.connection.close()
    def process_item(self, item, spider):
        #check the item is post or comment
        if 'authorId' in item.keys():
            self.filter_repeat_data(item)
        else:
            self.insert_to_mysql_comment(item)
        return item
    
    def insert_to_mysql_comment(self, item):
        values = (
            item['canonicalUrl'],
            item['commentId'],
            item['commentContent'],
            item['commentTime'],
            item['commentOrder']
        )
        
        with self.connection.cursor() as cursor:
            sql = 'INSERT INTO `comments` (`canonicalUrl`, `commentId`, `commentContent`, `commentTime`, `commentOrder`) VALUES (%s, %s, %s, %s, %s)'
            cursor.execute(sql, values)
            self.connection.commit()

    def insert_to_mysql(self, item):
        values = (
            item['title'],
            item['authorId'],
            item['authorName'],
            item['publishedTime'],
            item['content'],
            item['canonicalUrl'],
            item['content']
        )
        
        with self.connection.cursor() as cursor:
            sql = 'INSERT INTO `posts` (`title`, `authorId`, `authorName`, `publishedTime`, `content`, `canonicalUrl`) VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE content = %s'
            cursor.execute(sql, values)
            self.connection.commit()