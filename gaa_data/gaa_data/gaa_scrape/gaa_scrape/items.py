# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class MatchItem(scrapy.Item):
    FixtureID = scrapy.Field()
    Date = scrapy.Field()
    TeamA = scrapy.Field()
    TeamB = scrapy.Field()
    Time = scrapy.Field()
    Venue = scrapy.Field()
#   referee = scrapy.Field()
    TV = scrapy.Field()
    Sport = scrapy.Field()  # Add this line
