from parsing.util import SeleniumParser
from database.util import DBAdapter
import string
import datetime
import pandas as pd


# TODO: Get more columns
def parse_players(parser: SeleniumParser, db_adapter: DBAdapter, verbose=True):
    start_msg = "Players parsing"

    if verbose:
        print(start_msg)

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


def parse_some_games(month, year, parser: SeleniumParser, db_adapted: DBAdapter, verbose=True):
    blank_url = "https://www.basketball-reference.com/leagues/NBA_{year}_games-{month}.html"
    xpath = "//tbody/tr/td[@data-stat='box_score_text']/a"
    game_list_url = blank_url.format(year=year, month=month)

    parser.reconnect(game_list_url)
    res = parser.get_xpath(xpath)

    game_links = [game_info.get_attribute('href') for game_info in res]

    for idx, game_link in enumerate(game_links):
        parse_game(game_link, parser, db_adapted)
        if verbose:
            print('{}/{}'.format(idx + 1, len(game_links)))


# noinspection PyBroadException
def parse_game(game_url, parser: SeleniumParser, db_adapter: DBAdapter):
    name_xpath = "//a[@itemprop='name']"
    date_xpath = "//div[@class='scorebox_meta']/div[1]"
    total_scores_xpath = "//div[@class='score']"
    quarter_scores_xpath = "//table[@id='line_score']/tbody/tr/td[position()>1 and position()<6]"
    players_basics_blank_xpath = "//div[contains(@id, 'all_box-{team}-game-basic')]//tbody/" \
                                 "tr[not(@class)]"
    players_advanced_blank_xpath = "//div[contains(@id, 'all_box-{team}-game-advanced')]//tbody/" \
                                   "tr[not(@class)]"

    parser.reconnect(game_url)

    teams_stats_urls = list(map(lambda x: x.get_attribute("href"), parser.get_xpath(name_xpath)))
    teams_urls = list(map(lambda x: x[:x.rindex('/') + 1], teams_stats_urls))

    team1_info = db_adapter.select_from_table('Teams',
                                              columns='TRIM(url), TRIM(short_name)',
                                              condition="url='{t_url}'".format(t_url=teams_urls[0]))
    team2_info = db_adapter.select_from_table('Teams',
                                              columns='TRIM(url), TRIM(short_name)',
                                              condition="url='{t_url}'".format(t_url=teams_urls[1]))
    teams_info = [team1_info[0], team2_info[0]]

    teams_short = [item[1] for item in teams_info]

    teams_players_basics_xpath = list(map(lambda x: players_basics_blank_xpath.format(team=x), teams_short))
    teams_players_advanced_xpath = list(map(lambda x: players_advanced_blank_xpath.format(team=x), teams_short))

    game_datetime = datetime.datetime.strptime(parser.get_xpath(date_xpath)[0].text, "%I:%M %p, %B %d, %Y")

    game_teams_links = [item[0] for item in teams_info]

    game_total_teams_scores = list(map(lambda x: x.text, parser.get_xpath(total_scores_xpath)))
    game_quarters_teams_scores = list(map(lambda x: x.text, parser.get_xpath(quarter_scores_xpath)))
    game_quarters_team1_scores = game_quarters_teams_scores[:4]
    game_quarters_team2_scores = game_quarters_teams_scores[4:]

    winner_url = game_teams_links[0] if game_total_teams_scores[0] > game_total_teams_scores[1] else game_teams_links[1]

    game_row = {'url': game_url,
                'datetime': game_datetime.strftime("%d/%m/%Y %H:%M:00"),
                'team1_url': game_teams_links[0],
                'team2_url': game_teams_links[1],
                'home_team_url': game_teams_links[1],
                'winner_team_url': winner_url,
                'team1_total_score': game_total_teams_scores[0],
                'team2_total_score': game_total_teams_scores[1],
                'team1_q1_score': game_quarters_team1_scores[0],
                'team1_q2_score': game_quarters_team1_scores[1],
                'team1_q3_score': game_quarters_team1_scores[2],
                'team1_q4_score': game_quarters_team1_scores[3],
                'team2_q1_score': game_quarters_team2_scores[0],
                'team2_q2_score': game_quarters_team2_scores[1],
                'team2_q3_score': game_quarters_team2_scores[2],
                'team2_q4_score': game_quarters_team2_scores[3],
                }

    table_name = 'Games'
    table_params = [['url', 'NCHAR(100)', 'PRIMARY KEY'],
                    ['datetime', 'DATETIME', ''],
                    ['team1_url', 'NCHAR(100)', 'REFERENCES Teams (url) ON DELETE NO ACTION'],
                    ['team2_url', 'NCHAR(100)', 'REFERENCES Teams (url) ON DELETE NO ACTION'],
                    ['home_team_url', 'NCHAR(100)', 'REFERENCES Teams (url) ON DELETE NO ACTION'],
                    ['winner_team_url', 'NCHAR(100)', 'REFERENCES Teams (url) ON DELETE NO ACTION'],
                    ['team1_total_score', 'TINYINT', ''],
                    ['team2_total_score', 'TINYINT', ''],
                    ['team1_q1_score', 'TINYINT', ''],
                    ['team1_q2_score', 'TINYINT', ''],
                    ['team1_q3_score', 'TINYINT', ''],
                    ['team1_q4_score', 'TINYINT', ''],
                    ['team2_q1_score', 'TINYINT', ''],
                    ['team2_q2_score', 'TINYINT', ''],
                    ['team2_q3_score', 'TINYINT', ''],
                    ['team2_q4_score', 'TINYINT', '']]
    db_adapter.save_to_table(table_name,
                             pd.DataFrame([game_row]),
                             use_into=True,
                             create_table_params=table_params)

    basic_stats = list(map(lambda x: parser.get_xpath(x), teams_players_basics_xpath))
    advanced_stats = list(map(lambda x: parser.get_xpath(x), teams_players_advanced_xpath))

    for team_idx in range(2):
        rows_number = len(basic_stats[team_idx])
        for player_num in range(rows_number):
            p_basic_stats = basic_stats[team_idx][player_num].find_elements_by_xpath('child::*')
            p_advanced_stats = advanced_stats[team_idx][player_num].find_elements_by_xpath('child::*')

            p_url = p_basic_stats[0].find_element_by_xpath('child::*').get_attribute('href')

            if len(p_basic_stats) > 2:
                p_team = game_teams_links[team_idx]

                # Basic

                p_mp = assign_if_no_errors(p_basic_stats[1].text, lambda x: '00:{}'.format(x))
                p_fg = assign_if_no_errors(p_basic_stats[2].text, int)
                p_fga = assign_if_no_errors(p_basic_stats[3].text, int)
                p_fgp = assign_if_no_errors(p_basic_stats[4].text, float)
                p_3p = assign_if_no_errors(p_basic_stats[5].text, int)
                p_3pa = assign_if_no_errors(p_basic_stats[6].text, int)
                p_3pp = assign_if_no_errors(p_basic_stats[7].text, float)
                p_ft = assign_if_no_errors(p_basic_stats[8].text, int)
                p_fta = assign_if_no_errors(p_basic_stats[9].text, int)
                p_ftp = assign_if_no_errors(p_basic_stats[10].text, float)
                p_orb = assign_if_no_errors(p_basic_stats[11].text, int)
                p_drb = assign_if_no_errors(p_basic_stats[12].text, int)
                p_trb = assign_if_no_errors(p_basic_stats[13].text, int)
                p_ast = assign_if_no_errors(p_basic_stats[14].text, int)
                p_stl = assign_if_no_errors(p_basic_stats[15].text, int)
                p_blk = assign_if_no_errors(p_basic_stats[16].text, int)
                p_tov = assign_if_no_errors(p_basic_stats[17].text, int)
                p_pf = assign_if_no_errors(p_basic_stats[18].text, int)
                p_pts = assign_if_no_errors(p_basic_stats[19].text, int)
                p_pm = assign_if_no_errors(p_basic_stats[20].text, int)

                # Advanced

                p_tsp = assign_if_no_errors(p_advanced_stats[2].text, float)
                p_efgp = assign_if_no_errors(p_advanced_stats[3].text, float)
                p_3par = assign_if_no_errors(p_advanced_stats[4].text, float)
                p_ftr = assign_if_no_errors(p_advanced_stats[5].text, float)
                p_orbp = assign_if_no_errors(p_advanced_stats[6].text, float)
                p_drbp = assign_if_no_errors(p_advanced_stats[7].text, float)
                p_trbp = assign_if_no_errors(p_advanced_stats[8].text, float)
                p_astp = assign_if_no_errors(p_advanced_stats[9].text, float)
                p_stlp = assign_if_no_errors(p_advanced_stats[10].text, float)
                p_blkp = assign_if_no_errors(p_advanced_stats[11].text, float)
                p_tovp = assign_if_no_errors(p_advanced_stats[12].text, float)
                p_usgp = assign_if_no_errors(p_advanced_stats[13].text, float)
                p_ortg = assign_if_no_errors(p_advanced_stats[14].text, int)
                p_drtg = assign_if_no_errors(p_advanced_stats[15].text, int)

                p_stats_row = {'player_url': p_url,
                               'game_url': game_url,
                               'team_url': p_team,

                               'mp': p_mp,
                               'fg': p_fg,
                               'fga': p_fga,
                               'fgp': p_fgp,
                               'thp': p_3p,
                               'thpa': p_3pa,
                               'thpp': p_3pp,
                               'ft': p_ft,
                               'fta': p_fta,
                               'ftp': p_ftp,
                               'orb': p_orb,
                               'drb': p_drb,
                               'trb': p_trb,
                               'ast': p_ast,
                               'stl': p_stl,
                               'blk': p_blk,
                               'tov': p_tov,
                               'pf': p_pf,
                               'pts': p_pts,
                               'pm': p_pm,

                               'tsp': p_tsp,
                               'efgp': p_efgp,
                               'thpar': p_3par,
                               'ftr': p_ftr,
                               'orbp': p_orbp,
                               'drbp': p_drbp,
                               'trbp': p_trbp,
                               'astp': p_astp,
                               'stlp': p_stlp,
                               'blkp': p_blkp,
                               'tovp': p_tovp,
                               'usgp': p_usgp,
                               'ortg': p_ortg,
                               'drtg': p_drtg}

                table_name = "PlayersStats"
                table_extra_params = "PRIMARY KEY(player_url, game_url)"
                table_params = [['player_url', 'NCHAR(100)', 'REFERENCES Players (url) ON DELETE NO ACTION'],
                                ['game_url', 'NCHAR(100)', 'REFERENCES Games (url) ON DELETE NO ACTION'],
                                ['team_url', 'NCHAR(100)', 'REFERENCES Teams (url) ON DELETE NO ACTION'],

                                ['mp', 'TIME(0)', ''],
                                ['fg', 'TINYINT', ''],
                                ['fga', 'TINYINT', ''],
                                ['fgp', 'REAL', ''],
                                ['thp', 'TINYINT', ''],
                                ['thpa', 'TINYINT', ''],
                                ['thpp', 'REAL', ''],
                                ['ft', 'TINYINT', ''],
                                ['fta', 'TINYINT', ''],
                                ['ftp', 'REAL', ''],
                                ['orb', 'TINYINT', ''],
                                ['drb', 'TINYINT', ''],
                                ['trb', 'TINYINT', ''],
                                ['ast', 'TINYINT', ''],
                                ['stl', 'TINYINT', ''],
                                ['blk', 'TINYINT', ''],
                                ['tov', 'TINYINT', ''],
                                ['pf', 'TINYINT', ''],
                                ['pts', 'TINYINT', ''],
                                ['pm', 'SMALLINT', ''],

                                ['tsp', 'REAL', ''],
                                ['efgp', 'REAL', ''],
                                ['thpar', 'REAL', ''],
                                ['ftr', 'REAL', ''],
                                ['orbp', 'REAL', ''],
                                ['drbp', 'REAL', ''],
                                ['trbp', 'REAL', ''],
                                ['astp', 'REAL', ''],
                                ['stlp', 'REAL', ''],
                                ['blkp', 'REAL', ''],
                                ['tovp', 'REAL', ''],
                                ['usgp', 'REAL', ''],
                                ['ortg', 'REAL', ''],
                                ['drtg', 'REAL', '']]
                db_adapter.save_to_table(table_name,
                                         pd.DataFrame([p_stats_row]),
                                         use_into=True,
                                         create_table_params=table_params,
                                         create_table_extra_params=table_extra_params)


# noinspection PyBroadException
def assign_if_no_errors(input_val, function=None, failure=None):
    try:
        output_val = function(input_val)
        return output_val
    except Exception:
        return failure
