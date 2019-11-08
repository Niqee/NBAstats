from parsing.util import Parser
import pandas as pd


def parse_players(save_link, verbose=True):
    url = "https://stats.nba.com/players/list/"
    xpath = "//section[@class='row collapse players-list__section']//li[@class='players-list__name']/a"

    parser = Parser(url)
    parser.start_parsing()
    res = parser.get_xpath(xpath)

    if verbose:
        print("Number of results :", len(res))

    rows_list = []
    for item in res:
        player_id = item.attrs['href'].split('/')[2]
        player_name = item.text
        player_link = "https://stats.nba.com" + item.attrs['href']

        row_dict = {'PlayerId': player_id,
                    'PlayerName': player_name,
                    'PlayerLink': player_link}
        rows_list.append(row_dict)
    players_dataframe = pd.DataFrame(rows_list)
    players_dataframe.set_index('PlayerId', inplace=True)
    players_dataframe.to_csv(save_link, sep=";")