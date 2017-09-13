# -*- coding: utf-8 -*-
from datetime import datetime, date, timedelta

from scrapyelasticsearch.scrapyelasticsearch import ElasticSearchPipeline


class CleanData(object):

    @staticmethod
    def _clean_data(k, v):
        floats = ['acres', 'bathrooms', 'lat', 'lng']
        ints = [
            'baths', 'bedrooms', 'built', 'days_on_market', 'full_baths',
            'half_baths', 'price', 'sq_ft', 'square_footage', 'year_built',
            'condo_fees', 'condo_fee'
        ]

        k = k.lower()
        k = k.replace('.', '')
        k = k.replace('  ', ' ')
        k = k.replace('®', '')
        k = k.replace('#', '')
        k = k.strip()
        k = k.replace(' ', '_')

        if k in floats:
            v = float(v)
        if k in ints:
            v = v.replace('$', '')
            v = v.replace(',', '')
            v = int(v)
        if isinstance(v, str):
            if v.lower() == "yes":
                v = True
            elif v.lower() == "no":
                v = False
        return (k, v)

    def process_item(self, item, spider):
        data = dict([
            self._clean_data(k, v) for k, v in item.items()
        ])
        delta = timedelta(days=data.pop('days_on_market', 0))
        date_listed = date.today() - delta
        data.update({
            'created': datetime.now().isoformat(),
            'date_listed': date_listed.isoformat(),
            'location': {
                "lat": data.pop('lat'),
                "lon": data.pop('lng')
            },
        })
        return data


class ElasticSearchPipeline(ElasticSearchPipeline):
    def index_item(self, item):

        index_name = self.settings['ELASTICSEARCH_INDEX']
        index_suffix_format = self.settings.get(
            'ELASTICSEARCH_INDEX_DATE_FORMAT', None)
        index_suffix_key = self.settings.get(
            'ELASTICSEARCH_INDEX_DATE_KEY', None)
        index_suffix_key_format = self.settings.get(
            'ELASTICSEARCH_INDEX_DATE_KEY_FORMAT', None)

        if index_suffix_format:
            if index_suffix_key and index_suffix_key_format:
                dt = datetime.strptime(
                    item[index_suffix_key], index_suffix_key_format)
            else:
                dt = datetime.now()
            index_name += "-" + datetime.strftime(dt, index_suffix_format)
        elif index_suffix_key:
            index_name += "-" + index_suffix_key

        index_action = {
            '_index': index_name,
            '_type': self.settings['ELASTICSEARCH_TYPE'],
            '_source': dict(item)
        }

        if self.settings['ELASTICSEARCH_UNIQ_KEY'] is not None:
            item_unique_key = item[self.settings['ELASTICSEARCH_UNIQ_KEY']]
            unique_key = self.get_unique_key(item_unique_key)
            item_id = unique_key.decode()
            index_action['_id'] = item_id

        self.items_buffer.append(index_action)

        if len(self.items_buffer) >= self.settings.get(
            'ELASTICSEARCH_BUFFER_LENGTH', 500):
            self.send_items()
            self.items_buffer = []