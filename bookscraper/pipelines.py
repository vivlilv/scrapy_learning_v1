# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import time

class BookscraperPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        #stripping all leading/trailing spaces from strings
        field_names = adapter.field_names()
        for field_name in field_names:
            if field_name != 'description':
                value = adapter.get(field_name)
                adapter[field_name] = value[0].strip()

        #making values lowercase
        lowercase_keys = ['category', 'product_type']
        for lowercase_key in lowercase_keys:
            value = adapter.get(lowercase_key)
            adapter[lowercase_key] = value.lower()

        #convert prices to float
        price_keys = ['price', 'price_excl_tax', 'price_incl_tax', 'tax']
        for price_key in price_keys:
            value = adapter.get(price_key)
            value = value.replace('£','')
            adapter[price_key] = value

        # #availability-> select only number of items available
        availability_string = adapter.get('availability')
        split_string_arr = availability_string.split('(')
        if len(split_string_arr)<2:
            value = 0
        else:
            value = int(split_string_arr[1].split(' ')[0])
        adapter['availability'] = value

        #reviews -> convert num_of_reviews into int
        num_of_reviews = adapter.get('num_of_reviews')
        adapter['num_of_reviews'] = int(num_of_reviews)

        #stars -> convert to int
        star_rating = adapter.get('stars').lower()
        star_dict = {'one':1,'two':2,"three":3,'four':4,'five':5}
        adapter['stars'] = star_dict[star_rating]

        return item
