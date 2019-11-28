"""Match WebScraper."""


import csv
from datetime import datetime
import pdb
# from selenium.webdriver.common.keys import Keys
import sqlite3
# from collections import namedtuple
# from threading import Thread
from os.path import isfile
# from selenium.webdriver.firefox.options import Options
from time import ctime, sleep

from bs4 import BeautifulSoup
from selenium.webdriver import Chrome


class MatchesDBAdapter():
    def __init__(self):
        self.db_connection = sqlite3.connect('web_scraper_data.db')
        self.cursor = self.db_connection.cursor()
        self.create_matches_table_query = '''
            CREATE TABLE IF NOT EXISTS matches
            (id INTEGER PRIMARY KEY,
             team1 TEXT NOT NULL,
             team2 TEXT NOT NULL,
             startDate timestamp);
            '''
        self.cursor.execute(self.create_matches_table_query)
        self.db_connection.commit()

        self.insert_matches_query = '''
            INSERT INTO 'matches'
            (team1, team2 , startDate) VALUES
            (?, ?, ? );
            '''

    def populate_data(self, row_data):
        self.cursor.execute(self.insert_matches_query, row_data)
        self.db_connection.commit()


class MatchScraper():
    def __init__(self):
        self.base_url = 'https://www.mackolik.com/futbol/canli-sonuclar'
        self.driver = Chrome()
        self.default_sleep_interval = 3 # In seconds
        self.database_adapter = MatchesDBAdapter()

    def clean_up(self):
        self.driver.close()

    def scrape(self):
        print("Match Scraping started.....")
        self.driver.get(self.base_url)
        # sleep(self.default_sleep_interval)

        matches = self.driver.find_elements_by_xpath('//div[contains(@class, "widget-livescore__title")]')
        match_links = set()
        for match in matches:
            anchor =  match.find_element_by_xpath('..').find_element_by_xpath('.//a')
            match_links.add(anchor.get_attribute('href'))

        for match_link in match_links:
            print("matching link : ", match_link)
            # print(match.text)
            # anchor =  match.find_element_by_xpath('..').find_element_by_xpath('.//a')
            # print(anchor.get_attribute('href'))
            self.driver.get(match_link)
            self.driver.find_element_by_partial_link_text('FIKSTÜR').click()
            # print("Here")
            ol = self.driver.find_elements_by_xpath('//ol[contains(@class, "p0c-competition-match-list__days")]')

            for i in ol:
                print(i.tag_name)
                for j in i.find_elements_by_xpath('li'):
                    print(j.tag_name)
                    date_str = j.find_element_by_xpath('div')
                    print(date_str)
                    try:
                        print(date_str.text.split(' ')[1])
                    except Exception as e:
                        import pdb; pdb.set_trace()
                        print(e)

                    date_str = date_str.text.split(' ')[1]
                    print(datetime.strptime(date_str, '%d.%m.%Y'))
                    for k in j.find_elements_by_xpath('ol/li'):
                        print('1_1', k.tag_name, k.text, "1_1")
                        try:
                            split_text = k.text.split('\n')
                            date_time_str = date_str + ' '+ split_text[0].strip()
                            print(datetime.strptime(date_time_str, '%d.%m.%Y %H:%M'))
                            date = datetime.strptime(date_time_str, '%d.%m.%Y %H:%M')

                        except ValueError as e:
                            date = datetime.strptime(date_str, '%d.%m.%Y')
                        team1 = split_text[1].strip()
                        team2 = split_text[3].strip()
                        data = (team1, team2, date)
                        self.database_adapter.populate_data(data)

        self.clean_up()
        print("Match Scraping completed.....")


if __name__ == '__main__':
    match_scraper = MatchScraper()
    match_scraper.scrape()