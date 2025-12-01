# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html
# gaa_scrape/spiders/gaa_spider.py
import scrapy
try:
    # Original import (works in your local project structure)
    from gaa_scrape.items import MatchItem
except ModuleNotFoundError:
    # Fallback for the GitHub Actions project layout, where the package is named gaa_data
    from gaa_data.items import MatchItem

class GaaSpider(scrapy.Spider):
    name = 'gaa_matches'
    start_urls = ['https://www.gaa.ie/fixtures-results']

    def parse(self, response):
        for match in response.css('div.gar-match-item'):
            item = MatchItem()
            item['match_id'] = match.attrib['data-match-id']
            item['match_date'] = match.attrib['data-match-date']
            item['team_home'] = match.css('.gar-match-item__team.-home .gar-match-item__team-name::text').get()
            item['team_away'] = match.css('.gar-match-item__team.-away .gar-match-item__team-name::text').get()
            item['match_time'] = match.css('.gar-match-item__upcoming::text').get()
            item['venue'] = match.css('.gar-match-item__venue::text').get().strip('Venue: ')
            item['referee'] = match.css('.gar-match-item__referee::text').get().strip('Referee: ')
            item['team_home_logo'] = match.css('.gar-match-item__team.-home img::attr(src)').get()
            item['broadcasting'] = match.css('.gar-match-item__tv-provider img::attr(alt)').get()  # Add this line
            yield item
