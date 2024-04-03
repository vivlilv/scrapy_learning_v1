# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

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
        num_of_reviews = adapter.get('num_reviews')
        adapter['num_reviews'] = int(num_of_reviews)

        #stars -> convert to int
        star_rating = adapter.get('stars').lower()
        star_dict = {'one':1,'two':2,"three":3,'four':4,'five':5}
        adapter['stars'] = star_dict[star_rating]

        return item
    

import mysql.connector

class SaveToMySQLPipeline:

    def __init__(self):
        self.conn = mysql.connector.connect(
            host = 'localhost',
            user = 'vivliv',
            password = 'pass',#your pass
            database = 'books'#db i created previously
        )

        #creating cursor, which is used to execute commands
        self.cur = self.conn.cursor()

        ## Create books table if none exists
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS books(
            id int NOT NULL auto_increment, 
            url VARCHAR(255),
            title text,
            product_type VARCHAR(255),
            price_excl_tax DECIMAL,
            price_incl_tax DECIMAL,
            tax DECIMAL,
            price DECIMAL,
            availability INTEGER,
            num_reviews INTEGER,
            stars INTEGER,
            category VARCHAR(255),
            description text,
            PRIMARY KEY (id)
        )
        """)

    def process_item(self, item, spider):

        ## Define insert statement
        self.cur.execute(""" insert into books (
        url, 
        title,  
        product_type, 
        price_excl_tax,
        price_incl_tax,
        tax,
        price,
        availability,
        num_reviews,
        stars,
        category,
        description
        ) values (
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s
        )""", (
        item["url"],
        item["title"],
        item["product_type"],
        item["price_excl_tax"],
        item["price_incl_tax"],
        item["tax"],
        item["price"],
        item["availability"],
        item["num_reviews"],
        item["stars"],
        item["category"],
        str(item["description"][0])
        ))

        # ## Execute insert of data into database
        self.conn.commit()
        return item


    def close_spider(self, spider):

        ## Close cursor & connection to database 
        self.cur.close()
        self.conn.close()


