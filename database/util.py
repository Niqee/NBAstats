import pyodbc
import pandas as pd
import warnings
import os


class DBAdapter(object):

    def __init__(self,
                 driver="{SQL Server}",
                 server="DESKTOP-7L5ELCB",
                 database="nba_stats",
                 trusted_connection="yes"):

        settings_blank = 'Driver={};Server={};Database={};Trusted_Connection={};'
        conn = pyodbc.connect(settings_blank.format(driver,
                                                    server,
                                                    database,
                                                    trusted_connection),
                              autocommit=True)
        self.cursor = conn.cursor()

    def exec(self,
             query: str,
             no_return: bool = False,
             verbose: bool = False):

        if verbose:
            print(query)
        self.cursor.execute(query)
        if not no_return:
            return list(self.cursor)

    def create_table(self,
                     table_name: str,
                     params: list,
                     extra_params: str = None,
                     verbose: bool = False):

        main_blank = "CREATE TABLE [{name}] ({columns} {extra_params});"
        column_blank = "{name} {type} {attrs}, "
        columns_part = ""

        if self.table_exists(table_name):
            raise Exception('Table "{}" is already exists'.format(table_name))

        table_columns_params = [{"name": name, "type": v_type, "attrs": attrs} for name, v_type, attrs in params]

        for t_column_params in table_columns_params:
            t_column = column_blank.format(**t_column_params)
            columns_part += t_column
        if not extra_params:
            main_command = main_blank.format(name=table_name, columns=columns_part, extra_params="")
        else:
            main_command = main_blank.format(name=table_name, columns=columns_part, extra_params=extra_params)

        self.exec(main_command, no_return=True, verbose=verbose)

    def save_to_table(self,
                      table_name: str,
                      data_df: pd.DataFrame,
                      use_into=False,
                      create_table_params: list = None,
                      create_table_extra_params: str = None,
                      verbose=False):

        command_blank = "INSERT {into} [{table_name}] {columns} VALUES ({values})"

        column_names = data_df.columns

        if self.table_not_exists(table_name):
            self.create_table(table_name, params=create_table_params, extra_params=create_table_extra_params)

        for idx in data_df.index:
            values_str = ""
            columns_str = ""
            for column_name in column_names:
                value = data_df.loc[idx, column_name]
                if pd.notna(value):
                    if type(value) == str:
                        value = value.replace("'", "''")
                        value = "'" + value + "'"
                    values_str = "{prev}{value}, ".format(prev=values_str, value=value)
                    columns_str = "{prev}{name}, ".format(prev=columns_str, name=column_name)
            values_str = values_str[:-2]
            columns_str = columns_str[:-2]
            if use_into:
                columns_str = '(' + columns_str + ')'
                command = command_blank.format(into='INTO',
                                               table_name=table_name,
                                               columns=columns_str,
                                               values=values_str)
            else:
                command = command_blank.format(into='',
                                               table_name=table_name,
                                               columns='',
                                               values=values_str)
            try:
                self.exec(command, no_return=True, verbose=verbose)
            except pyodbc.IntegrityError:
                warnings.warn("Cant exec command : '{}'".format(command))

    def select_from_table(self,
                          table_name: str,
                          verbose: bool = False,
                          **params):

        command_blank = 'SELECT {columns} FROM [{table}] {where} {condition}'
        if ('columns' not in params.keys()) or (not params['columns']):
            params['columns'] = '*'

        if self.table_not_exists(table_name):
            raise Exception('Can`t find table with name "{name}"'.format(name=table_name))

        if ('condition' in params.keys()) and (params['condition']):
            command = command_blank.format(columns=params['columns'],
                                           table=table_name,
                                           where='WHERE',
                                           condition=params['condition'])
        else:
            command = command_blank.format(columns=params['columns'],
                                           table=table_name,
                                           where='',
                                           condition='')
        return self.exec(command, verbose=verbose)

    def table_not_exists(self, table_name: str):
        return not self.exec("SELECT * FROM information_schema.tables WHERE table_name = '{}'".format(table_name))

    def table_exists(self, table_name: str):
        return not self.table_not_exists(table_name)


class CSVAdapter(object):

    def __init__(self, files_dir="database/files"):
        self.files_dir = files_dir + r"/{file_name}.csv"

    def save_data(self, file_name, rows_list, index):
        save_path = self.files_dir.format(file_name=file_name)
        new_df = pd.DataFrame(rows_list).set_index(index)
        if os.path.isfile(save_path):
            cache_df = pd.read_csv(save_path, sep=';', index_col=index)
            new_df = cache_df.combine_first(new_df)
        new_df.to_csv(save_path, sep=';')

    def update_data(self, file_name, index, row, column, value):
        update_path = self.files_dir.format(file_name=file_name)
        cache_df = pd.read_csv(update_path, sep=';').set_index(index)
        cache_df.at[row, column] = value
        cache_df.to_csv(update_path, sep=';')

    def get_processed_urls(self):
        games_path = self.files_dir.format(file_name='Games')
        if not os.path.isfile(games_path):
            return list()
        cache_df = pd.read_csv(games_path, sep=';', index_col='Url')
        return cache_df.loc[cache_df['Completed']].index

    # TODO: Add warn
    def has_value_in_file(self, val, file_name, column):
        file_path = self.files_dir.format(file_name=file_name)
        if not os.path.isfile(file_path):
            # No such file warn
            return False
        target_df = pd.read_csv(file_path, sep=';')
        # TODO: Fix '\n' symbols in target_df.loc[:, column]
        return val in list(map(lambda x: x.strip('\n'), target_df.loc[:, column]))
