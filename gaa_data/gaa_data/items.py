# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html
import scrapy

class MatchItem(scrapy.Item):
    match_id = scrapy.Field()
    match_date = scrapy.Field()
    team_home = scrapy.Field()
    team_away = scrapy.Field()
    match_time = scrapy.Field()
    venue = scrapy.Field()
    referee = scrapy.Field()
    team_home_logo = scrapy.Field()
    broadcasting = scrapy.Field()  # Add this line
