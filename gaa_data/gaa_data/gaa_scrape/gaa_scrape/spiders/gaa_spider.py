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

                item["FixtureID"] = match.attrib.get("data-match-id")
                item["Date"] = match.attrib.get("data-match-date")

                time_txt = match.css('.gar-match-item__upcoming::text').get()
                item["Time"] = time_txt.strip() if time_txt else None

                item["Sport"] = group_name

                team_a = match.css('.gar-match-item__team.-home .gar-match-item__team-name::text').get()
                team_b = match.css('.gar-match-item__team.-away .gar-match-item__team-name::text').get()
                item["TeamA"] = team_a.strip() if team_a else None
                item["TeamB"] = team_b.strip() if team_b else None

                venue = match.css('.gar-match-item__venue::text').get()
                item["Venue"] = venue.replace("Venue: ", "").strip() if venue else None

                item["TV"] = match.css('.gar-match-item__tv-provider img::attr(alt)').get()

                yield item