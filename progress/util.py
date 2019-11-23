from termcolor import colored
import pandas as pd
import sys

from logger.util import Logger
from database.util import CSVAdapter
from parsing.util import SoupParser, ConnectionFailed


class ProgressController(object):
    def __init__(self,
                 plan_data_path: str = "progress/plan_data.csv",
                 load: bool = True):
        self.plan_data_path = plan_data_path
        self.parse_plan = self.load_plan_data(inplace=False) if load else pd.DataFrame()

    def setup_plan(self, parser: SoupParser):
        blank_url = r'https://www.basketball-reference.com/leagues/NBA_{year}_games-{month}.html'
        months = ['september',
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
        years = list(range(1950, 2019))

        rows_list = list()
        for year in years:
            print(year)
            for month in months:
                try:
                    parser.connect(blank_url.format(year=year, month=month))
                except ConnectionFailed:
                    print(blank_url.format(year=year, month=month))
                else:
                    soup = parser.soup
                    games = list(map(lambda x: x['href'],
                                     soup.find_all(lambda tag: 'Box Score' in str(tag.string) and tag.name == 'a')))
                    for game_url in games:
                        game_row = {'Url': r"https://www.basketball-reference.com" + game_url,
                                    'Completed': False,
                                    'Error': False}
                        rows_list.append(game_row)
        pd.DataFrame(rows_list).set_index('Url').to_csv(self.plan_data_path, sep=';')

    def load_plan_data(self, inplace: bool = True):
        loaded_df = pd.read_csv(self.plan_data_path, sep=';', index_col='Url')
        if inplace:
            self.parse_plan = loaded_df
        else:
            return loaded_df

    def save_plan_data(self):
        self.parse_plan.to_csv(self.plan_data_path, sep=';')

    # noinspection PyBroadException
    def synchronize_with_csv(self, csv_adapter: CSVAdapter):
        self.parse_plan.at[:, 'Completed'] = False
        self.parse_plan.at[:, 'Error'] = False

        processed_urls = csv_adapter.get_processed_urls()

        for url in processed_urls:
            if url in self.parse_plan.index:
                self.set_status_success(url, save=False)
            else:
                print(colored("{} is not in plan but founded in csv files".format(url), 'yellow'))
        self.save_plan_data()

    # noinspection PyBroadException
    # TODO: Add max_iter, max_time
    def start_working(self, parse_function, parser: SoupParser, db_adapter: CSVAdapter, logger: Logger):
        task_url = self.get_next_task_url()
        while task_url:
            try:
                parse_function(task_url, parser, db_adapter)
            except Exception as e:
                logger.log_error(task_url, str(e))
                self.set_status_error(task_url)
            else:
                logger.log_success(task_url)
                self.set_status_success(task_url)
            sys.stdout.write('\rProgress : {:.4f}%'.format(100 * self.get_progress()))
            task_url = self.get_next_task_url(with_errors=False)

    def get_next_task_url(self, with_errors=True):
        if with_errors:
            next_tasks = self.parse_plan.loc[~self.parse_plan['Completed'] | self.parse_plan['Error']]
        else:
            next_tasks = self.parse_plan.loc[~self.parse_plan['Completed'] & ~self.parse_plan['Error']]
        if next_tasks.empty:
            return None
        return next_tasks.index[0]

    def set_status_success(self, url, save=True):
        self.parse_plan.at[url, 'Completed'] = True
        if save:
            self.save_plan_data()

    def set_status_error(self, url, save=True):
        self.parse_plan.at[url, 'Error'] = True
        if save:
            self.save_plan_data()

    def get_progress(self):
        all_tasks_num = len(self.parse_plan.index)
        processed_tasks_num = len(self.parse_plan.loc[self.parse_plan['Completed'] | self.parse_plan['Error']].index)
        return processed_tasks_num/all_tasks_num
