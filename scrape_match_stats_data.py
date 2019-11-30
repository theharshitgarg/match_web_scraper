"""Match WebScraper."""

import csv
import logging
import pdb
import sqlite3
from sqlite3 import IntegrityError as SqliteIntegrityError
from sqlite3 import Error as SqliteError
from datetime import datetime
# from collections import namedtuple
# from threading import Thread
from os.path import isfile
# from selenium.webdriver.firefox.options import Options
from time import ctime, sleep

from bs4 import BeautifulSoup
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver import Chrome
from selenium.webdriver.common.keys import Keys

FORMAT = '%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s'

logging.basicConfig(format=FORMAT, level=logging.INFO, filename='match_stats_logs.log')

logger = logging.getLogger("football_match_stats_logger")
logger.warning("Football Match Logger")
logger.warning("-"*80)
logger.warning("Starting at : " + datetime.now().strftime("%d:%m:%Y $H:%M:%S"))
logger.warning("-"*80)

class MatchesDBAdapter():
    def __init__(self):
        self.db_connection = sqlite3.connect('football_matches.db')
        self.cursor = self.db_connection.cursor()

        self.update_stats_query = '''
        UPDATE 'matches' SET tople_left = ?, tople_right = ?, kormer_left = ?, kormer_right = ?, gol_left = ?, gol_right = ?, kirmizi_left = ?, kirmizi_right = ?, orta_left = ?, orta_right = ? WHERE id = ?'''

        self.select_stats_pending_records = '''
            SELECT * FROM 'matches' WHERE is_stats_scraped = 0;
            '''

    def populate_stats_data(self, row_data, record):
        # import pdb; pdb.set_trace()
        logger.info("Stats details populating")
        self.cursor.execute(self.update_stats_query, row_data)
        self.db_connection.commit()

        logger.info("Stats details populated")

    def get_stats_pending_records(self):
        self.cursor.execute(self.select_stats_pending_records)
        records = self.cursor.fetchall()
        return records


class MatchScraper():
    def __init__(self):
        self.driver = Chrome()
        self.default_sleep_interval = 3 # In seconds
        self.database_adapter = MatchesDBAdapter()

    def clean_up(self):
        self.driver.close()

    def get_record_stats(self, record):
        stats = []
        self.driver.get(record[6])

        # parse and populate
        try:
            self.driver.find_element_by_xpath('//a[@data-item-id="stats"]').click()
            sleep(self.default_sleep_interval)
        except Exception as e:
            print("No stats Found. Moving to next")
            logger.warning("No stats Found. Moving to next")
            return stats

        try:
            try:
                self.driver.find_element_by_partial_link_text('Genel').click()
                tople_left = self.driver.find_element_by_xpath('/html/body/div[6]/div[2]/main/div/div[2]/div/div/div[1]/div/div/div/div/div/ul/li[1]/div/table/tbody/tr[2]/td[1]').text
                tople_right = self.driver.find_element_by_xpath('/html/body/div[6]/div[2]/main/div/div[2]/div/div/div[1]/div/div/div/div/div/ul/li[1]/div/table/tbody/tr[2]/td[3]').text

                kormer_left = self.driver.find_element_by_xpath('/html/body/div[6]/div[2]/main/div/div[2]/div/div/div[1]/div/div/div/div/div/ul/li[1]/div/table/tbody/tr[10]/td[1]').text
                kormer_right = self.river.find_element_by_xpath('/html/body/div[6]/div[2]/main/div/div[2]/div/div/div[1]/div/div/div/div/div/ul/li[1]/div/table/tbody/tr[10]/td[3]').text
            except Exception as e:
                print("Error in Genel : ", str(e))
                logger.warning("Error in Stats Data :" + str(e))
                raise Exception("Error in Genel")

            try:
                self.driver.find_element_by_partial_link_text('Hücum').click()
                gol_left = self.driver.find_element_by_xpath('/html/body/div[6]/div[2]/main/div/div[2]/div/div/div[1]/div/div/div/div/div/ul/li[3]/div/table/tbody/tr[2]/td[1]').text
                gol_right = self.driver.find_element_by_xpath('/html/body/div[6]/div[2]/main/div/div[2]/div/div/div[1]/div/div/div/div/div/ul/li[3]/div/table/tbody/tr[2]/td[3]').text
            except Exception as e:
                print("Error in Hücum : ", str(e))
                logger.warning("Error in Stats Data :" + str(e))
                raise Exception("Error in Hücum")

            try:
                self.driver.find_element_by_partial_link_text('Faul').click()
                kirmizi_left = self.driver.find_element_by_xpath('/html/body/div[6]/div[2]/main/div/div[2]/div/div/div[1]/div/div/div/div/div/ul/li[5]/div/table/tbody/tr[6]/td[1]').text
                kirmizi_right = self.driver.find_element_by_xpath('/html/body/div[6]/div[2]/main/div/div[2]/div/div/div[1]/div/div/div/div/div/ul/li[5]/div/table/tbody/tr[6]/td[3]').text
            except Exception as e:
                print("Error in Faul", str(e))
                logger.warning("Error in Stats Data :" + str(e))
                raise Exception("Error in Faul")

            try:
                self.driver.find_element_by_partial_link_text('Pas').click()
                orta_left = self.driver.find_element_by_xpath('/html/body/div[6]/div[2]/main/div/div[2]/div/div/div[1]/div/div/div/div/div/ul/li[3]/div/table/tbody/tr[2]/td[1]').text
                orta_right = self.driver.find_element_by_xpath('/html/body/div[6]/div[2]/main/div/div[2]/div/div/div[1]/div/div/div/div/div/ul/li[3]/div/table/tbody/tr[2]/td[3]').text
            except Exception as e:
                print("Error in Pas : ", str(e))
                logger.warning("Error in Stats Data :" + str(e))
                print("Exception ", str(e))
                raise Exception("Error in Pas")

        except Exception as e:
            print("Error in stats", str(e))

        else:
            stats = [tople_left, tople_right, kormer_left, kormer_right, gol_left, gol_right, kirmizi_left, kirmizi_right, orta_left, orta_right]

        stats.append(record[0]) # add the record primary id

        return tuple(stats)

    def start_stats_scraping(self):
        print("Match Scraping started.....")
        records = self.database_adapter.get_stats_pending_records()
        print("{} : Records Found".format(len(records)))

        for record in records:
            stats = self.get_record_stats(record)
            if len(stats) == 11:
                self.database_adapter.populate_stats_data(stats, record)


        self.clean_up()
        print("Match Scraping completed.....")
        logger.info("Match Scraping completed.....")

    def scrape(self):
        logger.info("___________________RUN STARTED____________________")
        self.start_stats_scraping()
        try:
            self.start_stats_scraping()
        except Exception as e:
            print("Error : ", str(e))
            logging.critical("Scraping halted ...!!!!!!!!!!!!!!")
            logging.critical(str(e))
            # Uncomment the line to close the browser in the end in case of error
            # self.driver.quit()

        logger.info("___________________RUN COMPLETED____________________")


if __name__ == '__main__':
    # exit()
    match_scraper = MatchScraper()
    match_scraper.scrape()
