"""Match WebScraper."""

import csv
import logging
import pdb
import sqlite3
from datetime import datetime
# from collections import namedtuple
# from threading import Thread
from os.path import isfile
# from selenium.webdriver.firefox.options import Options
from time import ctime, sleep

from bs4 import BeautifulSoup
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.webdriver import Chrome
from selenium.webdriver.common.keys import Keys

FORMAT = '%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s'

logging.basicConfig(format=FORMAT, level=logging.DEBUG, filename='match_logs.log')

logger = logging.getLogger("football_match_logger")
logger.warning("Football Match Logger")
logger.warning("-"*80)
logger.warning("Starting at : " + datetime.now().strftime("%d:%m:%Y $H:%M:%S"))
logger.warning("-"*80)

class MatchesDBAdapter():
    def __init__(self):
        self.db_connection = sqlite3.connect('football_matches.db')
        self.cursor = self.db_connection.cursor()
        self.create_matches_table_query = '''
            CREATE TABLE IF NOT EXISTS matches
            (id INTEGER PRIMARY KEY,
             is_stats_scraped INTEGER,
             team1 TEXT NOT NULL,
             team2 TEXT NOT NULL,
             startDate timestamp,
             scraped_at timestamp,
             match_stats_link TEXT,
             tople_left TEXT,
             tople_right TEXT,
             kormer_left TEXT,
             kormer_right TEXT,
             gol_left TEXT,
             gol_right TEXT,
             kirmizi_left TEXT,
             kirmizi_right TEXT,
             orta_left TEXT,
             orta_right TEXT,
             CONSTRAINT unique_matches UNIQUE(team1,team2,startDate));
            '''

        self.cursor.execute(self.create_matches_table_query)
        self.db_connection.commit()
        logger.info("Match Table Created")

        self.insert_matches_query = '''
            INSERT INTO 'matches'
            (team1, team2 , startDate, tople_left, tople_right, kormer_left, kormer_right, gol_left, gol_right, kirmize_left, kirmizi_right, orta_left, orta_right, scraped_at, match_stats_link) VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            '''

        self.insert_matches_basic_query = '''
            INSERT INTO 'matches'
            (team1, team2 , startDate, scraped_at, match_stats_link) VALUES
            (?, ?, ?, ?, ?);
            '''

    def populate_data(self, row_data):
        logger.info("Stats Details populated")
        self.cursor.execute(self.insert_matches_query, row_data)
        self.db_connection.commit()

    def populate_data_basic(self, row_data):
        logger.info("Basic details populated")
        self.cursor.execute(self.insert_matches_basic_query, row_data)
        self.db_connection.commit()


class MatchScraper():
    def __init__(self):
        self.base_url = 'https://www.mackolik.com/futbol/canli-sonuclar'
        self.driver = Chrome()
        self.default_sleep_interval = 3 # In seconds
        self.database_adapter = MatchesDBAdapter()

    def clean_up(self):
        self.driver.close()

    def start_scraping(self):
        print("Match Scraping started.....")
        self.driver.get(self.base_url)
        # sleep(self.default_sleep_interval)

        matches = self.driver.find_elements_by_xpath('//div[contains(@class, "widget-livescore__title")]')
        match_links = set()
        print("Orginal Matches : ", len(matches))

        for match in matches:
            anchor =  match.find_element_by_xpath('..').find_element_by_xpath('.//a')
            print("Anchor : ", anchor.get_attribute('href'))
            match_links.add(anchor.get_attribute('href'))

        print("Match Links Found ...")
        print('\n'.join(match_links))

        for match_link in match_links:
            print("matching link : ", match_link)
            # print(match.text)
            # anchor =  match.find_element_by_xpath('..').find_element_by_xpath('.//a')
            # print(anchor.get_attribute('href'))
            self.driver.get(match_link)
            sleep(self.default_sleep_interval)
            # main_window = self.driver.current_window_handle
            scraped_at = datetime.now()
            self.driver.find_element_by_partial_link_text('FIKSTÜR').click()
            sleep(self.default_sleep_interval)
            # print("Here")
            ol = self.driver.find_elements_by_xpath('//ol[contains(@class, "p0c-competition-match-list__days")]')

            for i in ol:
                print(i.tag_name)
                for j in i.find_elements_by_xpath('li'):
                    print(j.tag_name)
                    date_str = j.find_element_by_xpath('div')
                    print(date_str)
                    try:
                        # Needs improvement
                        print(date_str.text.split(' '))
                    except Exception as e:
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
                        match_stats_link = k.find_element_by_xpath('//a/span[@data-dateformat="time"]').find_element_by_xpath('..').get_attribute('href')

                        data = (team1, team2, date, scraped_at, match_stats_link)
                        self.database_adapter.populate_data_basic(data)
            self.driver.find_element_by_tag_name('body').send_keys(Keys.CONTROL + 'w')
        self.clean_up()
        print("Match Scraping completed.....")

    def scrape(self):
        try:
            self.start_scraping()
        except Exception as e:
            print("Error : ", str(e))
            logging.critical("Scraping halted ...!!!!!!!!!!!!!!")
            logging.critical(str(e))
            # Uncomment the line to close the browser in the end in case of error
            # self.driver.quit()



if __name__ == '__main__':
    # exit()
    match_scraper = MatchScraper()
    match_scraper.scrape()
