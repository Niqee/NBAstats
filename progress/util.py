from selenium.common.exceptions import NoSuchElementException
from parsing.parse_functions import parse_game
from termcolor import colored
from database.util import DBAdapter
import warnings
import sys
import pandas as pd
import datetime
import time
import pickle
from parsing.util import SeleniumParser


class ProgressController(object):
    def __init__(self,
                 parser: SeleniumParser,
                 save_path: str = "progress/parse_plan.csv",
                 load=True):
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
        self.parse_plan = pd.read_csv(file_path, sep=';', index_col='url')

    def save_csv(self, file_path: str = None):
        if not file_path:
            file_path = self.save_path
        self.parse_plan.to_csv(file_path, sep=';')

    def update_df(self, partial_df: pd.DataFrame):
        if self.parse_plan.empty:
            self.parse_plan = partial_df
        else:
            self.parse_plan = pd.concat((self.parse_plan,
                                         partial_df.loc[set(partial_df.index) - set(self.parse_plan.index)]))
        self.save_csv()

    # noinspection PyBroadException
    def synchronize_with_database(self, db_adapter: DBAdapter):
        self.parse_plan.at[:, 'completed'] = False
        try:
            loaded_urls = list(map(lambda x: x[0], db_adapter.select_from_table('Games',
                                                                                columns='TRIM(url)',
                                                                                condition='loaded = 1')))
        except Exception:
            warnings.warn('Nothing to synchronize with')
            return

        for loaded_url in loaded_urls:
            if loaded_url in self.parse_plan.index:
                self.parse_plan.at[loaded_url, 'completed'] = True
            else:
                print(colored("{} is not in plan but founded in db".format(loaded_url), 'yellow'))
        self.save_csv()

    def get_new_task(self, with_error=False):
        if with_error:
            task_urls = self.parse_plan.loc[self.parse_plan['completed'] == False].index
        else:
            task_urls = self.parse_plan.loc[
                (self.parse_plan['completed'] == False) & (self.parse_plan['error'] != True)].index
        if task_urls.empty:
            return None
        return task_urls[0]

    # noinspection PyBroadException
    def start_working(self,
                      db_adapter: DBAdapter,
                      log_ok_path='progress/log_ok.txt',
                      log_err_path='progress/log_err.txt'):
        task_url = self.get_new_task()
        while task_url:
            time.sleep(2)
            try:
                debug_tm = parse_game(task_url, self.parser, db_adapter)
                self.parse_plan.at[task_url, 'completed'] = True
                # TODO: make change_csv method (autosave)
                self.save_csv()
                with open(log_ok_path, 'a+') as log_file:
                    log_file.write("{} | {} | {}\n".format(datetime.datetime.now(), task_url, debug_tm))
            except Exception as e:
                self.parse_plan.at[task_url, 'error'] = True
                self.save_csv()
                with open(log_err_path, 'a+') as log_file:
                    log_file.write("{} | {} | {}\n".format(datetime.datetime.now(), task_url, e))
            task_url = self.get_new_task()
