import os
from urllib import parse
import psycopg2

add_record = "INSERT INTO oldlinks(link) VALUES('{}')"
get_all_records = "SELECT link FROM oldlinks"
delete_all_records = "DELETE FROM oldlinks"


class DataBaseConnect:

    def __init__(self):
        self.parse_object = parse.uses_netloc.append("postgres")
        self.url = parse.urlparse(os.environ["DATABASE_URL"])
        self.connection = psycopg2.connect(
                            database=self.url.path[1:],
                            user=self.url.username,
                            password=self.url.password,
                            host=self.url.hostname,
                            port=self.url.port
                            )
        self.connection.autocommit = True
        self.cursor = self.connection.cursor()

    def insert_new_record(self, record):
        self.cursor.execute(add_record.format(record))

    def get_all_links(self):
        links_array = []
        self.cursor.execute(get_all_records)
        result = self.cursor.fetchall()
        if result:
            links_array = [x[0] for x in result]
        return links_array

    def delete_records(self):
        self.cursor.execute(get_all_records)
        result = self.cursor.fetchall()
        offset = [x[0] for x in result][:1000]
        self.cursor.execute(delete_all_records)
        for i in offset:
            self.insert_new_record(i)
