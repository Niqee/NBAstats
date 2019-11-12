from parsing.parse_functions import parse_game, parse_players, parse_teams, parse_some_games
from parsing.util import SeleniumParser
from database.util import DBAdapter
import pandas as pd

# Test

parser = SeleniumParser()
# # parse_game("https://www.basketball-reference.com/boxscores/201910220TOR.html", parser)
# # parse_some_games('october', 2020, parser)
# parse_players(parser)
# parser.disconnect()

db_adapter = DBAdapter()
# name = "Test"
# params = [['ProductName', 'NVARCHAR(30)', 'PRIMARY KEY'],
#           ['Cost', 'INT', 'NOT NULL']]
# df = pd.DataFrame([{'ProductName': 'A', 'Cost': 10},
#                    {'ProductName': 'F', 'Cost': 30}])
#
# db_adapter.save_to_table(name, df, create_table_params=params)

parse_players(parser, db_adapter)
parser.disconnect()
