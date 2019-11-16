from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
import urllib.request
import urllib.error as err
from termcolor import colored
import time
import sys
import pandas as pd
import datetime


class SeleniumParser:

    def __init__(self, target_url=None, start_parsing=True, no_css=False):
        self.target_url = target_url
        firefox_profile = FirefoxProfile()
        if no_css:
            firefox_profile.set_preference('permissions.default.stylesheet', 2)
        firefox_profile.set_preference('permissions.default.image', 2)
        firefox_profile.set_preference('javascript.enabled', False)
        firefox_profile.set_preference('dom.ipc.plugins.enabled.libflashplayer.so', 'false')
        firefox_profile.set_preference("http.response.timeout", 5)
        firefox_profile.set_preference("dom.max_script_run_time", 10)
        self.driver = webdriver.Firefox(firefox_profile)
        if start_parsing and target_url:
            self.__start_parsing()

    def __start_parsing(self):
        time.sleep(1)
        try:
            urllib.request.urlopen(self.target_url)
        except err.HTTPError as ex:
            print(colored(ex, 'yellow'))
            time.sleep(1)
            return False
        self.driver.get(self.target_url)
        return True

    def get_xpath(self, xpath):
        return self.driver.find_elements_by_xpath(xpath)

    @staticmethod
    def get_xpath_from(web_element, xpath, single=True):
        if single:
            return web_element.find_element_by_xpath(xpath)
        return web_element.find_elements_by_xpath(xpath)

    def click(self, xpath):
        self.driver.find_element_by_xpath(xpath).click()

    def get_url(self):
        return self.driver.current_url

    def reconnect(self, target_url, start_parsing=True):
        self.target_url = target_url
        if start_parsing:
            connected = self.__start_parsing()
            return connected

    def disconnect(self):
        self.driver.close()


class ProgressController(object):
    def __init__(self, parser: SeleniumParser, save_path: str = "parsing/parse_plan.csv"):
        self.parser = parser
        self.get_xpath_from = SeleniumParser.get_xpath_from
        self.save_path = save_path
        self.parse_plan = pd.DataFrame()

    def load_plan(self, start: dict, end: dict = None, verbose=True):
        if verbose:
            print("Starting plan loading\nStart game : {}\nEnd game : {}".format(start, end))
        blank_url = "https://www.basketball-reference.com/leagues/NBA_{year}_games-{month}.html"
        games_list_xpath = "//table[@id='schedule']/tbody/tr[not(@class)]"

        all_months = ['september',
                      'october',
                      'november',
                      'december',
                      'january',
                      'february',
                      'march',
                      'april',
                      'may',
                      'june',
                      'july',
                      'august']

        years = list(range(start['year'], end['year'] + 1))
        for year in years:
            rows_list = []
            start_month_idx = start['month'] if start['month'] and (year == years[0]) else 0
            end_month_idx = end['month'] if end['month'] and (year == years[-1]) else 11

            # if verbose:
            #     print("Year : {}".format(year))

            for month_idx in range(start_month_idx, end_month_idx + 1):
                # if verbose:
                #     print("|- Month : {}".format(month_idx))

                url = blank_url.format(year=year, month=all_months[month_idx])
                status = self.parser.reconnect(url)

                if not status:
                    continue

                games_info = self.parser.get_xpath(games_list_xpath)

                start_game_idx = start['game'] if start['game'] and (year == years[0]) else 0
                end_game_idx = end['game'] if end['game'] and (year == years[-1]) else len(games_info) - 1

                for game_idx in range(start_game_idx, end_game_idx + 1):
                    game_info = games_info[game_idx]
                    row_date = self.get_xpath_from(game_info, 'th[1]').text

                    try:
                        game_url = self.get_xpath_from(game_info,
                                                       'td/a[normalize-space(text())="Box Score"]').get_attribute(
                            'href')
                    except NoSuchElementException:
                        continue

                    game_date = datetime.datetime.strptime(row_date, "%a, %b %d, %Y")

                    game_row = {'url': game_url,
                                'date': game_date,
                                'num': game_idx + 1,
                                'completed': False,
                                'error': False}
                    rows_list.append(game_row)
                    if verbose:
                        msg_blank = "\rYear : {}, month : {} , progress : {}/{} ({:.3f}%)"
                        sys.stdout.write(msg_blank.format(year,
                                                          month_idx,
                                                          game_idx,
                                                          end_game_idx,
                                                          (game_idx / end_game_idx) * 100))
                        if game_idx == end_game_idx:
                            print('')
            self.update_df(pd.DataFrame(rows_list).set_index('url'))
        # self.parse_plan = pd.DataFrame(rows_list).set_index('url')

    def load_csv(self, file_path: str = None):
        if not file_path:
            file_path = self.save_path
        self.parse_plan = pd.read_csv(file_path, sep=';')

    def save_csv(self, file_path: str = None):
        if not file_path:
            file_path = self.save_path
        self.parse_plan.to_csv(file_path, sep=';')

    def update_df(self, partial_df: pd.DataFrame):
        if self.parse_plan.empty:
            self.parse_plan = partial_df
        else:
            self.parse_plan = pd.concat((self.parse_plan,
                                         partial_df.loc[set(partial_df.index)-set(self.parse_plan.index)]))
        self.save_csv()
