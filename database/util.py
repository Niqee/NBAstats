import pyodbc
import pandas as pd
import warnings


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

    def exec(self, query: str, no_return: bool = False):
        self.cursor.execute(query)
        if not no_return:
            return list(self.cursor)

    def create_table(self, table_name: str, params: list):
        main_blank = "CREATE TABLE {name} ({columns});"
        column_blank = "{name} {type} {attrs}, "
        columns_part = ""

        table_columns_params = [{"name": name, "type": v_type, "attrs": attrs} for name, v_type, attrs in params]

        for t_column_params in table_columns_params:
            t_column = column_blank.format(**t_column_params)
            columns_part += t_column
        main_command = main_blank.format(name=table_name, columns=columns_part)
        self.exec(main_command, no_return=True)

    def save_to_table(self,
                      table_name: str,
                      data_df: pd.DataFrame,
                      use_into=False,
                      create_table_params=None):
        command_blank = "INSERT {into} {table_name} {columns} VALUES ({values})"

        column_names = data_df.columns

        if not self.cursor.tables(table=table_name, tableType='TABLE').fetchone():
            self.create_table(table_name, create_table_params)

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
            print(command)
            try:
                self.exec(command, no_return=True)
            except pyodbc.IntegrityError:
                warnings.warn("Cant exec command : '{}'".format(command))
