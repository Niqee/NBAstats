import pyodbc


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

    def save_to_table(self, data_df):
        pass
