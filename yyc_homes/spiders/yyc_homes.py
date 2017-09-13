import scrapy


class QuotesSpider(scrapy.Spider):
    name = "calgary-real-estate.com"
    start_urls = [
        (
            'http://www.calgary-real-estate.com/idx/search.html'
            '?refine=true'
            '&sortorder=ASC-ListingDOM'
            '&map%5Blongitude%5D=-114.04219630395845'
            '&map%5Blatitude%5D=51.04490546336535'
            '&map%5Bzoom%5D=12'
            # '&map%5Bradius%5D='
            '&map%5Bpolygon%5D=%5B%2251.09640733477302+-114.05387878417952%2C51.08659600445823+-114.0373992919915%2C51.066859224723+-114.02675628662001%2C51.045279344649+-114.01439666748001%2C51.03146294308+-113.99757385253997%2C51.010514604735+-114.00890350342002%2C51.007274325918+-114.02606964110998%2C50.993446592859+-114.03636932373001%2C50.993446592859+-114.09542083740001%2C51.000360974529+-114.11842346190997%2C51.00986656708059+-114.16271209716388%2C51.03297431267159+-114.1637420654256%2C51.03793989429925+-114.16494369506574%2C51.05283344668578+-114.16545867919814%2C51.06966387045857+-114.15893554687125%2C51.08390021955983+-114.12477493286053%2C51.095760497969934+-114.11533355713095%2C51.09597611124292+-114.08203124999983%2C51.09640733477302+-114.05387878417952%22%5D'
            # '&minimum_price='
            # '&maximum_price=850000'
            '&search_type={}'
            # '&idx=creb'
            '&minimum_bedrooms=2'
            # '&maximum_bedrooms='
            # '&minimum_bathrooms='
            # '&maximum_bathrooms='
            '&minimum_sqft=1000'
            # '&maximum_sqft='
            # '&minimum_acres='
            # '&maximum_acres='
            # '&minimum_year='
            # '&maximum_year='
            # '&search_mls='
        ).format(t) for t in ('Attached', 'Detached')
    ]
    # TODO: Add killswitch if data has been seen before
    # TODO: How to support updated listings?

    def parse(self, response):
        # Call scraping for individual postings
        for listing in response.css('article header a.info-links'):
            yield response.follow(listing, callback=self.parse_detail)

        # Get next page of posts
        next_page = response.css('div.pagination a.next')
        if next_page:
            yield response.follow(next_page[0], callback=self.parse)

    def parse_detail(self, response):
        data = dict([
            x.css('::text').extract()
            for x in response.css('.listing.detail .keyval')
        ])
        data['remarks'] = response.css('.remarks::text').extract_first()
        data['images'] = response.css('.gallery img::attr(data-src)').extract()

        # Add lat/lng
        re = '([-+]?[0-9]*\.?[0-9]+)'
        script = response.css('script:not([src]):contains(streetview)')
        data['lat'] = script.re_first('lat:{}'.format(re))
        data['lng'] = script.re_first('lng:{}'.format(re))
        data['source'] = response.url

        return data
