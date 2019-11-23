from parsing.util import SoupParser, SeleniumParser, ConnectionFailed
from database.util import DBAdapter, CSVAdapter
import string
from datetime import datetime
import sys
import re
import pandas as pd
from bs4 import Comment


class NoSuchTeam(Exception):
    pass


# TODO: Get more columns
def parse_players(parser: SoupParser, db_adapter: CSVAdapter, verbose=False):
    verbose_msgs = {'start': 'Start players parsing:\n',
                    'progress': '\rProgress : {}/{} ({:.3f}%)',
                    'errors': 'Errors:'}
    error_list = list()
    main_url = "https://www.basketball-reference.com/players/{letter}"

    if verbose:
        sys.stdout.write(verbose_msgs['start'])
    all_players_suburls = [main_url.format(letter=letter) for letter in set(string.ascii_lowercase)]

    rows_list = list()

    for idx, players_suburl in enumerate(all_players_suburls):
        if verbose:
            sys.stdout.write(verbose_msgs['progress'].format(idx + 1,
                                                             len(all_players_suburls),
                                                             (100 * (idx + 1)) / len(all_players_suburls)))
        try:
            parser.connect(players_suburl)
        except ConnectionFailed as e:
            if verbose:
                error_list.append(e)
        soup = parser.soup
        players_info = list(map(lambda tag: tag.contents,
                                soup.find_all(lambda tag: (tag.name == "tr") and
                                                          (tag.parent.name == "tbody") and
                                                          (not tag.has_attr("class")))))
        for player_info in players_info:
            p_row = {'Url': player_info[0].find('a')['href'],
                     'Name': get_if_no_errors(player_info, lambda x: x[0].find('a').string),
                     'FirstYear': get_if_no_errors(player_info, lambda x: int(x[1].string)),
                     'LastYear': get_if_no_errors(player_info, lambda x: int(x[2].string)),
                     'Position': get_if_no_errors(player_info, lambda x: x[3].string),
                     'Height': get_if_no_errors(player_info, lambda x: height_to_meters(x[4].string)),
                     'Weight': get_if_no_errors(player_info, lambda x: float(x[5].string) * 0.4536),
                     'BirthDate': get_if_no_errors(player_info, lambda x: datetime.strptime(x[6].string, '%B %d, %Y')),
                     'BirthDateUrl': get_if_no_errors(player_info, lambda x: x[6].find('a')['href']),
                     'College': get_if_no_errors(player_info, lambda x: x[7].string),
                     'CollegeUrl': get_if_no_errors(player_info, lambda x: x[7].find('a')['href'])}
            rows_list.append(p_row)

    if verbose and error_list:
        sys.stdout.write('\n')
        sys.stdout.write(verbose_msgs['errors'])
        sys.stdout.write('\n')
        for error_info in error_list:
            sys.stdout.write(str(error_info))
            sys.stdout.write('\n')
            sys.stdout.write('-------')
    db_adapter.save_data("Players", rows_list, 'Url')


# TODO: Get more columns
def parse_teams(parser: SoupParser, db_adapter: CSVAdapter, verbose=False):
    verbose_msgs = {'start': 'Start players parsing:\n',
                    'progress': '\rProgress : {}/{} ({:.3f}%)',
                    'end': '\nDone\n'}
    main_url = "https://www.basketball-reference.com/teams/"
    if verbose:
        sys.stdout.write(verbose_msgs['start'])

    parser.connect(main_url)
    teams_info = list(map(lambda tag: tag.contents,
                          parser.soup.find_all(lambda tag: (tag.name == 'tr') and
                                                           (tag.parent.name == 'tbody') and
                                                           ('full_table' in tag['class']))))

    rows_list = list()
    for idx, team_info in enumerate(teams_info):
        t_row = {'Url': team_info[0].find('a')['href'],
                 'FullName': get_if_no_errors(team_info, lambda x: x[0].string),
                 'ShortName': get_if_no_errors(team_info, lambda x: x[0].find('a')['href'].split('/')[-2]),
                 'League': get_if_no_errors(team_info, lambda x: x[1].string),
                 'FirstYear': get_if_no_errors(team_info, lambda x: x[2].string),
                 'LastYear': get_if_no_errors(team_info, lambda x: x[3].string),
                 'Years': get_if_no_errors(team_info, lambda x: int(x[4].string)),
                 'Games': get_if_no_errors(team_info, lambda x: int(x[5].string)),
                 'Wins': get_if_no_errors(team_info, lambda x: int(x[6].string)),
                 'Losses': get_if_no_errors(team_info, lambda x: int(x[7].string)),
                 'WLp': get_if_no_errors(team_info, lambda x: float(x[8].string)),
                 'Plyfs': get_if_no_errors(team_info, lambda x: int(x[9].string)),
                 'Div': get_if_no_errors(team_info, lambda x: int(x[10].string)),
                 'Conf': get_if_no_errors(team_info, lambda x: int(x[11].string)),
                 'Champ': get_if_no_errors(team_info, lambda x: int(x[12].string))}
        rows_list.append(t_row)
        if verbose:
            sys.stdout.write(
                verbose_msgs['progress'].format(idx + 1, len(teams_info), (100 * (idx + 1)) / len(teams_info)))
    db_adapter.save_data('Teams', rows_list, index='Url')
    if verbose:
        sys.stdout.write(verbose_msgs['end'])


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

    # TODO: Fix Charlotte Hornets in some cases is CHH (in db it is CHA)
    # CHH -> CHA
    # SEA -> OKC
    # VAN -> MEM
    teams_urls = list(map(lambda x: x.replace('CHH', 'CHA'), teams_urls))
    teams_urls = list(map(lambda x: x.replace('SEA', 'OKC'), teams_urls))
    teams_urls = list(map(lambda x: x.replace('VAN', 'MEM'), teams_urls))

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

    # TODO: fix error while parsing datetime in 1999 and earlier
    game_datetime = datetime.strptime(parser.get_xpath(date_xpath)[0].text, "%I:%M %p, %B %d, %Y")

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
                    ['team2_q4_score', 'TINYINT', ''],
                    ['loaded', 'BIT', 'DEFAULT 0']]
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

                p_mp = get_if_no_errors(p_basic_stats[1].text, lambda x: '00:{}'.format(x))
                p_fg = get_if_no_errors(p_basic_stats[2].text, int)
                p_fga = get_if_no_errors(p_basic_stats[3].text, int)
                p_fgp = get_if_no_errors(p_basic_stats[4].text, float)
                p_3p = get_if_no_errors(p_basic_stats[5].text, int)
                p_3pa = get_if_no_errors(p_basic_stats[6].text, int)
                p_3pp = get_if_no_errors(p_basic_stats[7].text, float)
                p_ft = get_if_no_errors(p_basic_stats[8].text, int)
                p_fta = get_if_no_errors(p_basic_stats[9].text, int)
                p_ftp = get_if_no_errors(p_basic_stats[10].text, float)
                p_orb = get_if_no_errors(p_basic_stats[11].text, int)
                p_drb = get_if_no_errors(p_basic_stats[12].text, int)
                p_trb = get_if_no_errors(p_basic_stats[13].text, int)
                p_ast = get_if_no_errors(p_basic_stats[14].text, int)
                p_stl = get_if_no_errors(p_basic_stats[15].text, int)
                p_blk = get_if_no_errors(p_basic_stats[16].text, int)
                p_tov = get_if_no_errors(p_basic_stats[17].text, int)
                p_pf = get_if_no_errors(p_basic_stats[18].text, int)
                p_pts = get_if_no_errors(p_basic_stats[19].text, int)
                p_pm = get_if_no_errors(p_basic_stats[20].text, int)

                # Advanced

                p_tsp = get_if_no_errors(p_advanced_stats[2].text, float)
                p_efgp = get_if_no_errors(p_advanced_stats[3].text, float)
                p_3par = get_if_no_errors(p_advanced_stats[4].text, float)
                p_ftr = get_if_no_errors(p_advanced_stats[5].text, float)
                p_orbp = get_if_no_errors(p_advanced_stats[6].text, float)
                p_drbp = get_if_no_errors(p_advanced_stats[7].text, float)
                p_trbp = get_if_no_errors(p_advanced_stats[8].text, float)
                p_astp = get_if_no_errors(p_advanced_stats[9].text, float)
                p_stlp = get_if_no_errors(p_advanced_stats[10].text, float)
                p_blkp = get_if_no_errors(p_advanced_stats[11].text, float)
                p_tovp = get_if_no_errors(p_advanced_stats[12].text, float)
                p_usgp = get_if_no_errors(p_advanced_stats[13].text, float)
                p_ortg = get_if_no_errors(p_advanced_stats[14].text, int)
                p_drtg = get_if_no_errors(p_advanced_stats[15].text, int)

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

    # TODO: Implement update method
    db_adapter.exec("UPDATE Games SET loaded = 1 WHERE url = '{}'".format(game_url), no_return=True)


# noinspection PyBroadException
def parse_game_new(game_url, parser: SoupParser, db_adapter: CSVAdapter):
    # TODO: Fix Charlotte Hornets in some cases is CHH (in db it is CHA)
    # CHH -> CHA
    # CHO -> CHA
    # SEA -> OKC
    # VAN -> MEM
    # TRI -> ATL
    # ROC -> SAC
    # MNL -> LAL
    replaces = [('CHH', 'CHA'),
                ('CHO', 'CHA'),
                ('SEA', 'OKC'),
                ('VAN', 'MEM'),
                ('TRI', 'ATL'),
                ('ROC', 'SAC'),
                ('MNL', 'LAL'),
                ('PHW', 'GSW'),
                ('FTW', 'DET'),
                ('SYR', 'PHI'),
                ('MLH', 'ATL'),
                ('STL', 'ATL'),
                ('CIN', 'SAC'),
                ]

    # TODO: fix error while parsing datetime in 1999 and earlier

    parser.connect(game_url)
    soup = parser.soup

    t1_url = reformat_href(soup.find_all('a', itemprop='name')[0]['href'])
    t2_url = reformat_href(soup.find_all('a', itemprop='name')[1]['href'])
    t1_short = t1_url.split('/')[-2]
    t2_short = t2_url.split('/')[-2]
    for repl in replaces:
        t1_url = t1_url.replace(repl[0], repl[1])
        t2_url = t2_url.replace(repl[0], repl[1])

    t1_score = int(soup.find_all('div', class_='score')[0].string)
    t2_score = int(soup.find_all('div', class_='score')[1].string)
    winner_url = t1_url if t1_score > t2_score else t2_url

    try:
        line_score, four_factors = soup.find('div', class_='content_grid').find_all(
            string=lambda text: isinstance(text, Comment))
    except ValueError:
        line_score = None
        four_factors = None

    try:
        t1_q_scores = list(map(lambda x: int(x.string), list(
            filter(lambda x: x.string != '\n', SoupParser.parse_str(line_score.string).find_all('tr')[2].contents))[
                                                        1:-1]))
        t2_q_scores = list(map(lambda x: int(x.string), list(
            filter(lambda x: x.string != '\n', SoupParser.parse_str(line_score.string).find_all('tr')[3].contents))[
                                                        1:-1]))
    except AttributeError:
        t1_q_scores = None
        t2_q_scores = None

    try:
        t1_ff = list(
            map(lambda x: float(x.string), SoupParser.parse_str(four_factors.string).find_all('tr')[2].contents[2:-1]))
        t2_ff = list(
            map(lambda x: float(x.string), SoupParser.parse_str(four_factors.string).find_all('tr')[3].contents[2:-1]))
    except (AttributeError, TypeError):
        t1_ff = None
        t2_ff = None

    # Checking if teams urls in Teams.csv file
    if not db_adapter.has_value_in_file(t1_url, 'Teams', 'Url'):
        raise NoSuchTeam('\nCan`t find team in Teams.csv\n'
                         'Url : {url}\n'.format(url=t1_url))
    if not db_adapter.has_value_in_file(t2_url, 'Teams', 'Url'):
        raise NoSuchTeam('Can`t find team in Teams.csv\n'
                         'Url : {url}'.format(url=t2_url))

    # 1) Get all data about game
    game_row = {'Url': game_url,
                'Date': get_if_no_errors(soup,
                                         lambda x: string_to_date(
                                             x.find('div', class_='scorebox_meta').contents[1].string)),
                'Team1Url': t1_url,
                'Team2Url': t2_url,
                'HomeTeamUrl': t2_url,
                'WinnerUrl': winner_url,
                'OTNumber': get_if_no_errors(t1_q_scores, lambda x: len(x) - 4),
                'Team1SeriesWins': get_if_no_errors(soup,
                                                    lambda x: int(
                                                        x.find_all('div', class_='scores')[0].next_sibling.string.split(
                                                            '-')[0])),
                'Team1SeriesLosses': get_if_no_errors(soup,
                                                      lambda x: int(
                                                          x.find_all('div', class_='scores')[
                                                              0].next_sibling.string.split(
                                                              '-')[1])),
                'Team2SeriesWins': get_if_no_errors(soup,
                                                    lambda x: int(
                                                        x.find_all('div', class_='scores')[1].next_sibling.string.split(
                                                            '-')[0])),
                'Team2SeriesLosses': get_if_no_errors(soup,
                                                      lambda x: int(
                                                          x.find_all('div', class_='scores')[
                                                              1].next_sibling.string.split(
                                                              '-')[1])),
                'Team1TotalScore': t1_score,
                'Team2TotalScore': t2_score,
                'Team1Q1Score': get_if_no_errors(t1_q_scores, lambda x: x[0]),
                'Team1Q2Score': get_if_no_errors(t1_q_scores, lambda x: x[1]),
                'Team1Q3Score': get_if_no_errors(t1_q_scores, lambda x: x[2]),
                'Team1Q4Score': get_if_no_errors(t1_q_scores, lambda x: x[3]),
                'Team1OT1Score': get_if_no_errors(t1_q_scores, lambda x: x[4]),
                'Team1OT2Score': get_if_no_errors(t1_q_scores, lambda x: x[5]),
                'Team1OT3Score': get_if_no_errors(t1_q_scores, lambda x: x[6]),
                'Team1OT4Score': get_if_no_errors(t1_q_scores, lambda x: x[7]),
                'Team1OT5MScore': get_if_no_errors(t1_q_scores, lambda x: x[8] + sum(x[9:])),
                'Team2Q1Score': get_if_no_errors(t2_q_scores, lambda x: x[0]),
                'Team2Q2Score': get_if_no_errors(t2_q_scores, lambda x: x[1]),
                'Team2Q3Score': get_if_no_errors(t2_q_scores, lambda x: x[2]),
                'Team2Q4Score': get_if_no_errors(t2_q_scores, lambda x: x[3]),
                'Team2OT1Score': get_if_no_errors(t2_q_scores, lambda x: x[4]),
                'Team2OT2Score': get_if_no_errors(t2_q_scores, lambda x: x[5]),
                'Team2OT3Score': get_if_no_errors(t2_q_scores, lambda x: x[6]),
                'Team2OT4Score': get_if_no_errors(t2_q_scores, lambda x: x[7]),
                'Team2OT5MScore': get_if_no_errors(t2_q_scores, lambda x: x[8] + sum(x[9:])),
                'Team1eFGp': get_if_no_errors(t1_ff, lambda x: x[0]),
                'Team1TOVp': get_if_no_errors(t1_ff, lambda x: x[1]),
                'Team1ORBp': get_if_no_errors(t1_ff, lambda x: x[2]),
                'Team1OFTFGA': get_if_no_errors(t1_ff, lambda x: x[3]),
                'Team2eFGp': get_if_no_errors(t2_ff, lambda x: x[0]),
                'Team2TOVp': get_if_no_errors(t2_ff, lambda x: x[1]),
                'Team2ORBp': get_if_no_errors(t2_ff, lambda x: x[2]),
                'Team2OFTFGA': get_if_no_errors(t2_ff, lambda x: x[3]),
                'Attendance': get_if_no_errors(soup, lambda x: int(
                    str(x.find(lambda tag: 'Attendance' in str(tag.string)).next_sibling).replace(',', ''))),
                'GameTime': get_if_no_errors(soup, lambda x: datetime.strptime(
                    str(x.find(lambda tag: 'Time of Game' in str(tag.string)).next_sibling), '%H:%M').time()),
                'Completed': False
                }

    # 2) Save to "Games.csv" (loaded = False)
    db_adapter.save_data("Games", [game_row], index='Url')
    # 3) Get all players stats
    stats_tables = soup.find_all(
        id=re.compile(r'^box-({t1_short}|{t2_short})-game-(basic|advanced)$'.format(t1_short=t1_short,
                                                                                    t2_short=t2_short)))
    if len(stats_tables) == 2:
        teams_stats = [(stats_tables[0], None), (stats_tables[1], None)]
    elif len(stats_tables) == 4:
        teams_stats = [(stats_tables[0], stats_tables[1]), (stats_tables[2], stats_tables[3])]
    else:
        raise Exception('len(stats_tables) == {}'.format(len(stats_tables)))

    players_stats_list = list()
    for team_idx, team_tables in enumerate(teams_stats):
        players_basic_rows = team_tables[0].find('tbody').find_all(lambda tag: (not tag.has_attr('class')) and
                                                                               (tag.name == 'tr'))
        try:
            players_advanced_rows = team_tables[1].find('tbody').find_all(lambda tag: (not tag.has_attr('class')) and
                                                                                      (tag.name == 'tr'))
        except AttributeError:
            players_advanced_rows = None
            all_rows = zip(players_basic_rows, [None] * len(players_basic_rows))
        else:
            all_rows = zip(players_basic_rows, players_advanced_rows)

        for player_stats in all_rows:
            player_basic = player_stats[0].contents
            try:
                player_advanced = player_stats[1].contents
            except AttributeError:
                player_advanced = None
            p_url = player_basic[0].find('a')['href']
            if not db_adapter.has_value_in_file(p_url, 'Players', 'Url'):
                # TODO: No such player in Players.csv warn
                continue
            player_row = {'Url': p_url,
                          'GameUrl': game_url,
                          'TeamUrl': t1_url if team_idx == 0 else t2_url,
                          'MinutesPlayed': get_if_no_errors(player_basic, lambda x: string_to_time(x[1].get_text())),

                          # Basic

                          'FG': get_if_no_errors(player_basic, lambda x: int(x[2].get_text())),
                          'FGA': get_if_no_errors(player_basic, lambda x: int(x[3].get_text())),
                          'FGp': get_if_no_errors(player_basic, lambda x: float(x[4].get_text())),
                          'THP': get_if_no_errors(player_basic, lambda x: int(x[5].get_text())),
                          'THPA': get_if_no_errors(player_basic, lambda x: int(x[6].get_text())),
                          'THPp': get_if_no_errors(player_basic, lambda x: float(x[7].get_text())),
                          'FT': get_if_no_errors(player_basic, lambda x: int(x[8].get_text())),
                          'FTA': get_if_no_errors(player_basic, lambda x: int(x[9].get_text())),
                          'FTp': get_if_no_errors(player_basic, lambda x: float(x[10].get_text())),
                          'ORB': get_if_no_errors(player_basic, lambda x: int(x[11].get_text())),
                          'DRB': get_if_no_errors(player_basic, lambda x: int(x[12].get_text())),
                          'TRB': get_if_no_errors(player_basic, lambda x: int(x[13].get_text())),
                          'AST': get_if_no_errors(player_basic, lambda x: int(x[14].get_text())),
                          'STL': get_if_no_errors(player_basic, lambda x: int(x[15].get_text())),
                          'BLK': get_if_no_errors(player_basic, lambda x: int(x[16].get_text())),
                          'TOV': get_if_no_errors(player_basic, lambda x: int(x[17].get_text())),
                          'PF': get_if_no_errors(player_basic, lambda x: int(x[18].get_text())),
                          'PTS': get_if_no_errors(player_basic, lambda x: int(x[19].get_text())),
                          'PM': get_if_no_errors(player_basic, lambda x: int(x[20].get_text())),

                          # Advanced

                          'TSp': get_if_no_errors(player_advanced, lambda x: float(x[2].get_text())),
                          'eFGp': get_if_no_errors(player_advanced, lambda x: float(x[3].get_text())),
                          'THPAr': get_if_no_errors(player_advanced, lambda x: float(x[4].get_text())),
                          'FTr': get_if_no_errors(player_advanced, lambda x: float(x[5].get_text())),
                          'ORBp': get_if_no_errors(player_advanced, lambda x: float(x[6].get_text())),
                          'DRBp': get_if_no_errors(player_advanced, lambda x: float(x[7].get_text())),
                          'TRBp': get_if_no_errors(player_advanced, lambda x: float(x[8].get_text())),
                          'ASTp': get_if_no_errors(player_advanced, lambda x: float(x[9].get_text())),
                          'STLp': get_if_no_errors(player_advanced, lambda x: float(x[10].get_text())),
                          'BLKp': get_if_no_errors(player_advanced, lambda x: float(x[11].get_text())),
                          'TOVp': get_if_no_errors(player_advanced, lambda x: float(x[12].get_text())),
                          'USGp': get_if_no_errors(player_advanced, lambda x: float(x[13].get_text())),
                          'ORtg': get_if_no_errors(player_advanced, lambda x: int(x[14].get_text())),
                          'DRtg': get_if_no_errors(player_advanced, lambda x: int(x[15].get_text()))
                          }
            players_stats_list.append(player_row)

    # 4) Save them into "PlayersStats.csv"
    db_adapter.save_data('PlayersStats', players_stats_list, ['Url', 'GameUrl'])
    # 5) Update game status (completed = True)
    db_adapter.update_data('Games', 'Url', game_url, 'Completed', True)


# noinspection PyBroadException
def get_if_no_errors(input_val, function=None, failure=None):
    try:
        output_val = function(input_val)
        return output_val
    except Exception:
        return failure


def height_to_meters(input_str):
    height_list = input_str.split('-')
    inches = float(height_list[0]) * 12 + float(height_list[1])
    return inches * 0.0254


def string_to_date(input_str):
    patterns = ["%I:%M %p, %B %d, %Y",
                "%B %d, %Y"]
    for pattern in patterns:
        try:
            return datetime.strptime(input_str, pattern)
        except ValueError:
            pass
    return None


def string_to_time(input_str):
    return datetime.strptime(input_str, '%M:%S').time()


def reformat_href(input_str):
    return input_str[:input_str.rfind('/') + 1]
