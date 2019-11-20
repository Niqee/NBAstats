from termcolor import colored
import pandas as pd

from logger.util import Logger
from database.util import CSVAdapter


class ProgressController(object):
    def __init__(self,
                 plan_data_path: str = "progress/plan_data.csv",
                 load: bool = True):
        self.plan_data_path = plan_data_path
        self.parse_plan = self.load_plan_data(inplace=False) if load else pd.DataFrame()

    def setup_plan(self):
        pass

    def load_plan_data(self, inplace: bool = True):
        loaded_df = pd.read_csv(self.plan_data_path, sep=';', index_col='url')
        if inplace:
            self.parse_plan = loaded_df
        else:
            return loaded_df

    def save_plan_data(self):
        self.parse_plan.to_csv(self.plan_data_path, sep=';')

    # noinspection PyBroadException
    def synchronize_with_csv(self, csv_adapter: CSVAdapter):
        self.parse_plan.at[:, 'completed'] = False
        self.parse_plan.at[:, 'error'] = False

        completed_urls = csv_adapter.get_completed_urls()
        error_urls = csv_adapter.get_failed_urls()
        processed_urls = csv_adapter.get_processed_urls()

        for url in processed_urls:
            if url in self.parse_plan.index:
                if url in completed_urls:
                    self.set_status_success(url, save=False)
                elif url in error_urls:
                    self.set_status_error(url, save=False)
            else:
                print(colored("{} is not in plan but founded in csv files".format(url), 'yellow'))
        self.save_plan_data()

    # noinspection PyBroadException
    # TODO: Add max_iter, max_time
    def start_working(self, parse_function, logger: Logger):
        task_url = self.get_next_task_url()
        while task_url:
            try:
                parse_function(task_url)
            except Exception as e:
                logger.log_error(task_url, str(e))
                self.set_status_error(task_url)
            else:
                logger.log_success(task_url)
                self.set_status_success(task_url)

    def get_next_task_url(self, with_errors=True):
        if with_errors:
            next_tasks = self.parse_plan.loc[~self.parse_plan['completed'] | self.parse_plan['error']]
        else:
            next_tasks = self.parse_plan.loc[~self.parse_plan['completed']]
        if next_tasks.empty:
            return None
        return next_tasks.index[0]

    def set_status_success(self, url, save=True):
        self.parse_plan.at[url, 'completed'] = True
        if save:
            self.save_plan_data()

    def set_status_error(self, url, save=True):
        self.parse_plan.at[url, 'error'] = True
        if save:
            self.save_plan_data()
