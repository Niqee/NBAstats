from parsing.util import SeleniumParser
from database.util import DBAdapter
import string
import datetime
import os
import pandas as pd


# TODO: Get more columns
def parse_players(parser: SeleniumParser, db_adapter: DBAdapter):
    main_url = "https://www.basketball-reference.com/players/{letter}"
    players_xpath = "//tbody//th[@data-stat='player']//a"

    all_letters = set(string.ascii_lowercase)
    all_letters.remove('x')
    all_players_suburls = [main_url.format(letter=letter) for letter in all_letters]

    rows_list = list()

    for players_suburl in all_players_suburls:
        parser.reconnect(players_suburl)
        players_info = parser.get_xpath(players_xpath)
        for player_info in players_info:
            p_url = player_info.get_attribute('href')
            p_name = player_info.text
            p_row = {"url": p_url,
                     "name": p_name}
            rows_list.append(p_row)

    players_df = pd.DataFrame(rows_list)

    table_name = "Players"
    table_params = [['url', 'NCHAR(100)', 'PRIMARY KEY'],
                    ['name', 'NCHAR(50)', 'NOT NULL']]
    db_adapter.save_to_table(table_name, players_df, create_table_params=table_params)


# TODO: Get more columns
def parse_teams(parser: SeleniumParser, db_adapter: DBAdapter):
    main_url = "https://www.basketball-reference.com/teams/"
    teams_xpath = "//div[@id='all_teams_active']//tbody//tr[@class='full_table']/th/a"

    parser.reconnect(main_url)
    res = parser.get_xpath(teams_xpath)

    rows_list = list()
    for team_info in res:
        t_url = team_info.get_attribute('href')
        t_full_name = team_info.text
        t_short_name = t_url.split('/')[-2]

        t_row = {'url': t_url,
                 'full_name': t_full_name,
                 'short_name': t_short_name}
        rows_list.append(t_row)

    teams_df = pd.DataFrame(rows_list)

    table_name = "Teams"
    table_params = [['url', 'NCHAR(100)', 'PRIMARY KEY'],
                    ['full_name', 'NCHAR(50)', 'NOT NULL'],
                    ['short_name', 'NCHAR(10)', 'NOT NULL']]
    # teams_df.to_csv("parsing/results/TeamsList.csv", sep=";")
    db_adapter.save_to_table(table_name, teams_df, create_table_params=table_params)


def parse_some_games(month, year, parser: SeleniumParser):
    blank_url = "https://www.basketball-reference.com/leagues/NBA_{year}_games-{month}.html"
    xpath = "//tbody/tr/td[@data-stat='box_score_text']/a"
    game_list_url = blank_url.format(year=year, month=month)

    parser.reconnect(game_list_url)
    res = parser.get_xpath(xpath)

    game_links = [game_info.get_attribute('href') for game_info in res]

    for idx, game_link in enumerate(game_links):
        parse_game(game_link, parser)
        print('{}/{}'.format(idx + 1, len(game_links)))


# noinspection PyBroadException
def parse_game(game_url, parser: SeleniumParser):
    parser.reconnect(game_url)

    teams_xpath = "//a[@itemprop='name']"
    date_xpath = "//div[@class='scorebox_meta']/div[1]"
    scores_xpath = "//div[@class='score']"

    players_basics_blank_xpath = "//div[contains(@id, 'all_box-{team}-game-basic')]//tbody/" \
                                 "tr[not(@class)]"
    players_advanced_blank_xpath = "//div[contains(@id, 'all_box-{team}-game-advanced')]//tbody/" \
                                   "tr[not(@class)]"

    team1_short = parser.get_xpath(teams_xpath)[0].get_attribute("href").split('/')[-2]
    team2_short = parser.get_xpath(teams_xpath)[1].get_attribute("href").split('/')[-2]

    team1_players_basics_xpath = players_basics_blank_xpath.format(team=team1_short)
    team2_players_basics_xpath = players_basics_blank_xpath.format(team=team2_short)

    team1_players_advanced_xpath = players_advanced_blank_xpath.format(team=team1_short)
    team2_players_advanced_xpath = players_advanced_blank_xpath.format(team=team2_short)

    g_date = datetime.datetime.strptime(parser.get_xpath(date_xpath)[0].text,
                                        "%I:%M %p, %B %d, %Y").strftime("%d/%m/%Y")
    teams = parser.get_xpath(teams_xpath)
    scores = parser.get_xpath(scores_xpath)

    g_t1_link = teams[0].get_attribute('href')
    g_t2_link = teams[1].get_attribute('href')
    g_t1_score = scores[0].text
    g_t2_score = scores[1].text

    game_row = {'GameUrl': game_url,
                'Date': g_date,
                'TeamLink1': g_t1_link,
                'TeamLink2': g_t2_link,
                'TeamScore1': g_t1_score,
                'TeamScore2': g_t2_score}

    t1_basic_stats = parser.get_xpath(team1_players_basics_xpath)
    t1_advanced_stats = parser.get_xpath(team1_players_advanced_xpath)
    t2_basic_stats = parser.get_xpath(team2_players_basics_xpath)
    t2_advanced_stats = parser.get_xpath(team2_players_advanced_xpath)

    # for item in t1_basic_stats:
    #     print(item.text)
    # print("----")
    # for item in t2_basic_stats:
    #     print(item.text)

    # Loop fot team 1

    rows_number = len(t1_basic_stats)

    for player_num in range(rows_number):
        p_basic_stats = t1_basic_stats[player_num].find_elements_by_xpath('child::*')
        p_advanced_stats = t1_advanced_stats[player_num].find_elements_by_xpath('child::*')

        p_url = p_basic_stats[0].find_element_by_xpath('child::*').get_attribute('href')

        if len(p_basic_stats) > 2:
            p_team = g_t1_link

            # Basic

            p_mp = try_except(p_basic_stats[1].text.split(':')[0], 0)
            p_fg = try_except(p_basic_stats[2].text)
            p_fga = try_except(p_basic_stats[3].text)
            p_fgp = try_except(p_basic_stats[4].text)
            p_3p = try_except(p_basic_stats[5].text)
            p_3pa = try_except(p_basic_stats[6].text)
            p_3pp = try_except(p_basic_stats[7].text)
            p_ft = try_except(p_basic_stats[8].text)
            p_fta = try_except(p_basic_stats[9].text)
            p_ftp = try_except(p_basic_stats[10].text)
            p_orb = try_except(p_basic_stats[11].text)
            p_drb = try_except(p_basic_stats[12].text)
            p_ast = try_except(p_basic_stats[14].text)
            p_stl = try_except(p_basic_stats[15].text)
            p_blk = try_except(p_basic_stats[16].text)
            p_tov = try_except(p_basic_stats[17].text)
            p_pf = try_except(p_basic_stats[18].text)
            p_pts = try_except(p_basic_stats[19].text)
            p_pm = try_except(p_basic_stats[20].text)

            # Advanced

            p_tsp = try_except(p_advanced_stats[2].text)
            p_efgp = try_except(p_advanced_stats[3].text)
            p_3par = try_except(p_advanced_stats[4].text)
            p_ftr = try_except(p_advanced_stats[5].text)
            p_orbp = try_except(p_advanced_stats[6].text)
            p_drbp = try_except(p_advanced_stats[7].text)
            p_trbp = try_except(p_advanced_stats[8].text)
            p_astp = try_except(p_advanced_stats[9].text)
            p_stlp = try_except(p_advanced_stats[10].text)
            p_blkp = try_except(p_advanced_stats[11].text)
            p_tovp = try_except(p_advanced_stats[12].text)
            p_usgp = try_except(p_advanced_stats[13].text)
            p_ortg = try_except(p_advanced_stats[14].text)
            p_drtg = try_except(p_advanced_stats[15].text)

            p_stats_row = {'GameUrl': game_url,
                           'GameDate': g_date,
                           'Team': p_team,

                           'MP': p_mp,
                           'FG': p_fg,
                           'FGA': p_fga,
                           'FGP': p_fgp,
                           '3P': p_3p,
                           '3PA': p_3pa,
                           '3PP': p_3pp,
                           'FT': p_ft,
                           'FTA': p_fta,
                           'FTP': p_ftp,
                           'ORB': p_orb,
                           'DRB': p_drb,
                           'AST': p_ast,
                           'STL': p_stl,
                           'BLK': p_blk,
                           'TOV': p_tov,
                           'PF': p_pf,
                           'PTS': p_pts,
                           'PM': p_pm,

                           'TSP': p_tsp,
                           'EFGP': p_efgp,
                           '3PAR': p_3par,
                           'FTR': p_ftr,
                           'ORBP': p_orbp,
                           'DRBP': p_drbp,
                           'TRBP': p_trbp,
                           'ASTP': p_astp,
                           'STLP': p_stlp,
                           'BLKP': p_blkp,
                           'TOVP': p_tovp,
                           'USGP': p_usgp,
                           'ORTG': p_ortg,
                           'DRTG': p_drtg}

            path = '/'.join(p_url.split('/')[-2:])
            player_file_path = "parsing/results/players/{path}.csv".format(path=path)
            if not os.path.isfile(player_file_path):
                stats_df = pd.DataFrame([p_stats_row]).set_index('GameUrl')
            else:
                new_row_df = pd.DataFrame([p_stats_row]).set_index('GameUrl')
                stats_df = pd.read_csv(player_file_path, sep=';', index_col=0)
                if game_url not in stats_df.index:
                    stats_df = stats_df.append(new_row_df)
            stats_df.to_csv(player_file_path, sep=';')

    # Loop for team 2

    rows_number = len(t2_basic_stats)

    for player_num in range(rows_number):
        p_basic_stats = t2_basic_stats[player_num].find_elements_by_xpath('child::*')
        p_advanced_stats = t2_advanced_stats[player_num].find_elements_by_xpath('child::*')

        p_url = p_basic_stats[0].find_element_by_xpath('child::*').get_attribute('href')

        if len(p_basic_stats) > 2:
            p_team = g_t2_link

            # Basic

            p_mp = try_except(p_basic_stats[1].text.split(':')[0], 0)
            p_fg = try_except(p_basic_stats[2].text)
            p_fga = try_except(p_basic_stats[3].text)
            p_fgp = try_except(p_basic_stats[4].text)
            p_3p = try_except(p_basic_stats[5].text)
            p_3pa = try_except(p_basic_stats[6].text)
            p_3pp = try_except(p_basic_stats[7].text)
            p_ft = try_except(p_basic_stats[8].text)
            p_fta = try_except(p_basic_stats[9].text)
            p_ftp = try_except(p_basic_stats[10].text)
            p_orb = try_except(p_basic_stats[11].text)
            p_drb = try_except(p_basic_stats[12].text)
            p_ast = try_except(p_basic_stats[14].text)
            p_stl = try_except(p_basic_stats[15].text)
            p_blk = try_except(p_basic_stats[16].text)
            p_tov = try_except(p_basic_stats[17].text)
            p_pf = try_except(p_basic_stats[18].text)
            p_pts = try_except(p_basic_stats[19].text)
            p_pm = try_except(p_basic_stats[20].text)

            # Advanced

            p_tsp = try_except(p_advanced_stats[2].text)
            p_efgp = try_except(p_advanced_stats[3].text)
            p_3par = try_except(p_advanced_stats[4].text)
            p_ftr = try_except(p_advanced_stats[5].text)
            p_orbp = try_except(p_advanced_stats[6].text)
            p_drbp = try_except(p_advanced_stats[7].text)
            p_trbp = try_except(p_advanced_stats[8].text)
            p_astp = try_except(p_advanced_stats[9].text)
            p_stlp = try_except(p_advanced_stats[10].text)
            p_blkp = try_except(p_advanced_stats[11].text)
            p_tovp = try_except(p_advanced_stats[12].text)
            p_usgp = try_except(p_advanced_stats[13].text)
            p_ortg = try_except(p_advanced_stats[14].text)
            p_drtg = try_except(p_advanced_stats[15].text)

            p_stats_row = {'GameUrl': game_url,
                           'GameDate': g_date,
                           'Team': p_team,

                           'MP': p_mp,
                           'FG': p_fg,
                           'FGA': p_fga,
                           'FGP': p_fgp,
                           '3P': p_3p,
                           '3PA': p_3pa,
                           '3PP': p_3pp,
                           'FT': p_ft,
                           'FTA': p_fta,
                           'FTP': p_ftp,
                           'ORB': p_orb,
                           'DRB': p_drb,
                           'AST': p_ast,
                           'STL': p_stl,
                           'BLK': p_blk,
                           'TOV': p_tov,
                           'PF': p_pf,
                           'PTS': p_pts,
                           'PM': p_pm,

                           'TSP': p_tsp,
                           'EFGP': p_efgp,
                           '3PAR': p_3par,
                           'FTR': p_ftr,
                           'ORBP': p_orbp,
                           'DRBP': p_drbp,
                           'TRBP': p_trbp,
                           'ASTP': p_astp,
                           'STLP': p_stlp,
                           'BLKP': p_blkp,
                           'TOVP': p_tovp,
                           'USGP': p_usgp,
                           'ORTG': p_ortg,
                           'DRTG': p_drtg}

            path = '/'.join(p_url.split('/')[-2:])
            player_file_path = "parsing/results/players/{path}.csv".format(path=path)
            if not os.path.isfile(player_file_path):
                stats_df = pd.DataFrame([p_stats_row]).set_index('GameUrl')
            else:
                new_row_df = pd.DataFrame([p_stats_row]).set_index('GameUrl')
                stats_df = pd.read_csv(player_file_path, sep=';', index_col=0)
                if game_url not in stats_df.index:
                    stats_df = stats_df.append(new_row_df)
            stats_df.to_csv(player_file_path, sep=';')

    if not os.path.isfile("parsing/results/GamesList.csv"):
        games_df = pd.DataFrame([game_row]).set_index('GameUrl')
    else:
        new_row_df = pd.DataFrame([game_row]).set_index('GameUrl')
        games_df = pd.read_csv("parsing/results/GamesList.csv", sep=';', index_col=0)
        if game_url not in games_df.index:
            games_df = games_df.append(new_row_df)
    games_df.to_csv("parsing/results/GamesList.csv", sep=";")


# noinspection PyBroadException
def try_except(success, failure=0):
    try:
        return float(success)
    except Exception:
        return failure
