# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ScraperItem(scrapy.Item):
    name = scrapy.Field()
    categories = scrapy.Field()
    product_url = scrapy.Field()
    file_urls = scrapy.Field()
    files = scrapy.Field()
