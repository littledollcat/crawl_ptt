# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

#post item
class PostItem(scrapy.Item):
    title = scrapy.Field()
    authorId = scrapy.Field()
    authorName = scrapy.Field()
    publishedTime = scrapy.Field()
    content = scrapy.Field()
    canonicalUrl = scrapy.Field()

#comment item
class CommentItem(scrapy.Item):
    canonicalUrl = scrapy.Field()
    commentId = scrapy.Field()
    commentContent = scrapy.Field()
    commentTime = scrapy.Field()
    commentOrder = scrapy.Field()