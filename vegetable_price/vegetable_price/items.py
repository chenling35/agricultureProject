# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class VegetablePriceItem(scrapy.Item):
    variety = scrapy.Field()
    area = scrapy.Field()
    market = scrapy.Field()
    price = scrapy.Field()
    previous_price = scrapy.Field()
    mom = scrapy.Field()
    time_price = scrapy.Field()
