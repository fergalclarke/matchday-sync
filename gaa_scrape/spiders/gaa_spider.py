# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

# gaa_scrape/spiders/gaa_spider.py
import scrapy
from gaa_scrape.items import MatchItem

class GaaSpider(scrapy.Spider):
    name = 'gaa_matches'
    start_urls = ['https://www.gaa.ie/fixtures-results']

    def parse(self, response):
        for group in response.css('div.gar-matches-list__group'):
            group_name = group.css('h3.gar-matches-list__group-name::text').get()
            for match in group.css('div.gar-match-item'):
                item = MatchItem()
                item['FixtureID'] = match.attrib['data-match-id']
                item['Date'] = match.attrib['data-match-date']
                item['Time'] = match.css('.gar-match-item__upcoming::text').get()
                item['Sport'] = group_name  # Assign the group name to the item
                item['TeamA'] = match.css('.gar-match-item__team.-home .gar-match-item__team-name::text').get()
                item['TeamB'] = match.css('.gar-match-item__team.-away .gar-match-item__team-name::text').get()
                item['Venue'] = match.css('.gar-match-item__venue::text').get().strip('Venue: ') if match.css('.gar-match-item__venue::text').get() else None
                item['TV'] = match.css('.gar-match-item__tv-provider img::attr(alt)').get()
                yield item
