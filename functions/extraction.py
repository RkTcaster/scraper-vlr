from bs4 import BeautifulSoup, Comment
from urllib.request import urlopen
import csv
import re
import time
import random
import pandas as pd
import os


# Structure for file folders and save csv
def get_folder_path(folder_name, normalized_tournament, file_prefix):
    """Create the file path for the csv

    Args:
        folder_name (string): folder name for the csv
        normalized_tournament (string): tournament in the path format
        file_prefix (string): prefix for the file name

    Returns:
        string: string with the path for the save to csv files
    """
    if file_prefix is None:
        print("Add file suffix")

    folder_path = os.path.join(folder_name, normalized_tournament)
    os.makedirs(folder_path, exist_ok=True)

    file_path = os.path.join(folder_path, f'{file_prefix}_{normalized_tournament}.csv')

    return file_path


def normalize_filename(name):
    """normalize the file name for the path

    Args:
        name (string): string (usually tournamnet name)

    Returns:
        string: normalized tournament name
    """
    name = name.lower()
    name = re.sub(r'[^\w\s-]', '', name)
    name = re.sub(r'\s+', '_', name)
    return name.strip('_')


def save_draft_to_csv(draft, url, folder="csv", encoding='utf-8'):
    """save the get_picks_bans() dictionary to csv

    Args:
        draft (dict): draft dict from
        url (string): url from a vlr match
        folder (str, optional): name of the default folder for the export. Defaults to "csv".
        encoding (str, optional): encoding for the csv file. Defaults to 'utf-8'.
    """

    tournament_name = draft['team_A'][-1]
    normalized_tournament = normalize_filename(tournament_name)

    file_path = get_folder_path(folder_name=folder, normalized_tournament=normalized_tournament, file_prefix="draft")

    header = draft["header"] + ["source_url"]
    file_exists = os.path.isfile(file_path)

    with open(file_path, mode='a', newline='', encoding=encoding) as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(header)
        writer.writerow(draft["team_A"] + [url])
        writer.writerow(draft["team_B"] + [url])


def save_round_detail_to_csv(detail_round_dict, folder="csv", encoding='utf-8'):  # stats from the teams
    """save the get_round_detail() dictionary to csv

    Args:
        detail_round_dict (dict): get_round_detail dict
        folder (str, optional): name of the default folder for the export. Defaults to "csv".
        encoding (str, optional): encoding for the csv file. Defaults to 'utf-8'.
    """
    tournament_name = detail_round_dict["event"][0]  # Medio raro esto
    normalized_tournament = normalize_filename(tournament_name)

    file_path = get_folder_path(folder_name=folder, normalized_tournament=normalized_tournament,
                                file_prefix="round_detail")

    header = list(detail_round_dict)
    file_exists = os.path.isfile(file_path)

    with open(file_path, "a", newline="", encoding=encoding) as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(header)

        writer.writerows(zip(*detail_round_dict.values()))
        f.close()


def save_player_performance_to_csv(player_performance_dict, folder="csv", encoding='utf-8'):
    """save the get_player_performance() dict to csv

    Args:
        player_performance_dict (dict): get_player_performance dict
        folder (str, optional): name of the default folder for the export. Defaults to "csv".
        encoding (str, optional): encoding for the csv file. Defaults to 'utf-8'.
    """
    tournament_name = player_performance_dict["event"][0]
    normalized_tournament = normalize_filename(tournament_name)

    file_path = get_folder_path(folder_name=folder, normalized_tournament=normalized_tournament,
                                file_prefix="player_performance")

    header = player_performance_dict.keys()
    file_exists = os.path.isfile(file_path)

    with open(file_path, "a", newline="", encoding=encoding) as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(header)

        writer.writerows(zip(*player_performance_dict.values()))
        f.close()


def save_team_economy(economy_dict, folder="csv", encoding="utf-8"):
    """save the get_team_economy() dict to csv

    Args:
        economy_dict (dict): get_team_economy dict
        folder (str, optional): name of the default folder for the export. Defaults to "csv".
        encoding (str, optional): encoding for the csv file. Defaults to 'utf-8'.
    """
    tournament_name = economy_dict["event"][0]
    normalized_tournament = normalize_filename(tournament_name)

    file_path = get_folder_path(folder_name=folder, normalized_tournament=normalized_tournament,
                                file_prefix="team_economy")

    header = economy_dict.keys()
    file_exists = os.path.isfile(file_path)

    with open(file_path, "a", newline="", encoding=encoding) as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(header)

        writer.writerows(zip(*economy_dict.values()))
        f.close()


def save_player_stats_to_csv(player_stats_dict, folder="csv", encoding='utf-8'):
    """save the get_player_stats() dict to csv

    Args:
        player_stats_dict (dict): get_player_stats() dict
        folder (str, optional): name of the default folder for the export. Defaults to "csv".
        encoding (str, optional): encoding for the csv file. Defaults to 'utf-8'.
    """
    tournament_name = player_stats_dict["event"][0]
    normalized_tournament = normalize_filename(tournament_name)

    file_path = get_folder_path(folder_name=folder, normalized_tournament=normalized_tournament,
                                file_prefix="player_stats")

    header = player_stats_dict.keys()
    file_exists = os.path.isfile(file_path)

    with open(file_path, "a", newline="", encoding=encoding) as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(header)

        writer.writerows(zip(*player_stats_dict.values()))
        f.close()


def save_match_error(match_error_dict, folder="csv", encoding='utf-8'):
    """save the matchs raising errors

    Args:
        match_error_dict (dict): match error dict
        folder (str, optional): name of the default folder for the export. Defaults to "csv".
        encoding (str, optional): encoding for the csv file. Defaults to 'utf-8'.
    """
    tournament_name = match_error_dict["event"][0]
    normalized_tournament = normalize_filename(tournament_name)

    file_path = get_folder_path(folder_name=folder, normalized_tournament=normalized_tournament,
                                file_prefix="error_match")

    header = match_error_dict.keys()
    file_exists = os.path.isfile(file_path)

    with open(file_path, "a", newline="", encoding=encoding) as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(header)

        writer.writerows(zip(*match_error_dict.values()))
        f.close()


def soup_open(url=None, decode="iso-8859-1"):
    """Open a url with BeautifulSoup and return a bs4.BeautifulSoup

    Args:
        url (str, optional): vlr match url. Defaults to None.
        decode (str, optional): decode for the BeautifulSoup. Defaults to "iso-8859-1".

    Returns:
        bs4.BeautifulSoup: BeautifulSoup object with the HTML info
    """
    if url is None:
        print("Add a url")

    page = urlopen(url)
    html = page.read().decode(decode)
    soup = BeautifulSoup(html, "html.parser")

    return soup


def get_basic_match_info(soup):
    """extract the basic match info from the vlr match page, used in other functions and for check the match status:
        ["team_a"
        "team_b"
        "team_a_tricode"
        "team_b_tricode"
        "event"
        "status"
        "bo"
        "date"
        "patch"
        "tournament_instance"
        "type"]


    Args:
        soup (bs4.BeautifulSoup): BeautifulSoup object with the HTML info

    Returns:
        dict: basict match info from dict
    """
    basic_match_info = {
        "teams": None,
        "event": None,
        "tournament_instance": None,
        "type": None,
    }

    event_text = soup.find("title").get_text(strip=True)
    regex = r"^([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)$"
    result = re.search(regex, event_text)

    for index, key in enumerate(basic_match_info.keys(), 1):
        basic_match_info[key] = result.group(index).strip()

    team_dict = {
        "team_a": None,
        "team_b": None,
        "team_a_tricode": None,
        "team_b_tricode": None,
        "event": None,
        "status": None,
        "bo": None,
        "date": None,
        "patch": None,
        "tournament_instance": None,
        "type": None,
    }

    event = basic_match_info["event"]

    team_dict["tournament_instance"] = basic_match_info["tournament_instance"]
    team_dict["type"] = basic_match_info["type"]

    teams_string = basic_match_info["teams"]
    pattern = r"^(.+?)\s+vs\.\s+(.+)$"

    result = re.findall(pattern, teams_string)

    teams = list(result[0]) if result else []
    team_tricodes = soup.find_all("div", {"class": "team"})

    if team_dict["team_a"] is not None:
        pass

    else:
        teamA_tricode = team_tricodes[2].get_text(strip=True)

        team_dict["team_a"] = teams[0]
        team_dict["team_a_tricode"] = teamA_tricode.strip()
        team_dict["event"] = event

    if team_dict["team_b"] is not None:
        pass
    else:
        teamB_tricode = team_tricodes[3].get_text(strip=True)

        team_dict["team_b"] = teams[1]
        team_dict["team_b_tricode"] = teamB_tricode.strip()

    match_notes = soup.find_all("div", {"class": "match-header-vs-note"})
    team_dict["status"] = match_notes[0].get_text().strip()
    team_dict["bo"] = match_notes[1].get_text().strip()[-1]

    # Header info
    header = soup.find("div", {"class": "match-header-super"})

    date = soup.find_all("div", class_="moment-tz-convert")[0].get("data-utc-ts")
    try:
        patch = header.find("div", style="font-style: italic;").get_text(strip=True)
    except Exception as e:
        print(f"Error in patch{e}")
        patch = "No patch"

    team_dict["date"] = date
    team_dict["patch"] = patch

    return team_dict


# Map draft:
def get_map_draft(soup):
    """pre step to process

    Args:
        soup (bs4.BeautifulSoup): BeautifulSoup object with the HTML info

    Returns:
        list: list with all the maps
    """
    try:
        pick_bans = soup.find(
            "div", {"class": "match-header-note"}
        ).get_text(strip=True).split(sep=";")

        pick_bans = [x.strip() for x in pick_bans]

    except Exception as e:
        print(f"Error en get_map_draft: {e}")

    return pick_bans


def get_picks_bans(soup, basic_match_info=None):
    """get the picks and bans from a vlr match page

    Args:
        soup (bs4.BeautifulSoup): BeautifulSoup object with the HTML info
        basic_match_info (dict, optional): basic match info dict. Defaults to None.

    Returns:
        dict: daft dict
    """
    if basic_match_info is None:
        print("Add basic_match_info dict")

    picks_bans = get_map_draft(soup)

    dict_picks_bans = {
        "header": [
            "team",
            "rival",
            "team_1_select_1",
            "team_2_select_1",
            "team_1_select_2",
            "team_2_select_2",
            "team_1_select_3",
            "team_2_select_3",
            "decider",
            "order",
            "bo",
            "date",
            "event",
        ],
        "team_A": [],
        "team_B": [],
    }

    team_a = basic_match_info["team_a_tricode"]
    team_b = basic_match_info["team_b_tricode"]

    dict_picks_bans["team_A"].append(team_a)
    dict_picks_bans["team_A"].append(team_b)
    dict_picks_bans["team_B"].append(team_b)
    dict_picks_bans["team_B"].append(team_a)

    for element in picks_bans:
        list_element = element.split()
        if len(list_element) == 3:
            dict_picks_bans["team_A"].append(list_element[-1])
        if len(list_element) == 2:
            dict_picks_bans["team_A"].append(list_element[0])

    order_team_B = [1, 0, 3, 2, 5, 4, 6]  # for order swaping
    maps_teamA = dict_picks_bans["team_A"][2:]

    try:
        map_order = [maps_teamA[i] for i in order_team_B]
    except IndexError:
        print(f"IndexError in get_picks_bans: {maps_teamA}")
        return None

    for map in map_order:
        dict_picks_bans["team_B"].append(map)

    dict_picks_bans["team_A"].append("A")
    dict_picks_bans["team_B"].append("B")

    bo = basic_match_info["bo"]

    dict_picks_bans["team_A"].append(bo)
    dict_picks_bans["team_B"].append(bo)

    date = basic_match_info["date"]
    dict_picks_bans["team_A"].append(date)
    dict_picks_bans["team_B"].append(date)

    event = basic_match_info["event"]

    dict_picks_bans["team_A"].append(event)
    dict_picks_bans["team_B"].append(event)

    return dict_picks_bans


# Round detail

def round_detail_to_dict(round_detail, folder="csv", encoding="utf-8"):
    """process the get_round_detail() dict to a valid format and save the csv

    Args:
        round_detail (dict): dict from get_round_detail()
        folder (str, optional): folder to save the csv. Defaults to "csv".
        encoding (str, optional): encoding to save the csv. Defaults to "utf-8".
    """
    round_detail_for_csv = {
        "teamA": [],
        "map": [],
        "side": [],
        "teamB": [],
        "rndA": [],
        "rndB": [],
        "round": [],
        "winCon": [],
        "date": [],
        "map_order": [],
        'event': []
    }

    for count, rondaAtk in enumerate(round_detail["teamATT"]):
        round_detail_for_csv["teamA"].append(round_detail["team_a"])
        round_detail_for_csv["teamB"].append(round_detail["team_b"])
        round_detail_for_csv["side"].append("atk")
        round_detail_for_csv["rndA"].append(rondaAtk)
        round_detail_for_csv["rndB"].append(round_detail["teamBCT"][count])
        round_detail_for_csv["map"].append(round_detail["map"])
        round_detail_for_csv["round"].append(round_detail["ratk"][count])
        round_detail_for_csv["winCon"].append(round_detail["winConAtk"][count])
        round_detail_for_csv["date"].append(round_detail["date"])
        round_detail_for_csv["map_order"].append(round_detail["map_order"])
        round_detail_for_csv["event"].append(round_detail["event"])

    for count, rondaDef in enumerate(round_detail["teamACT"]):
        round_detail_for_csv["teamA"].append(round_detail["team_a"])
        round_detail_for_csv["teamB"].append(round_detail["team_b"])
        round_detail_for_csv["side"].append("def")
        round_detail_for_csv["rndA"].append(rondaDef)
        round_detail_for_csv["rndB"].append(round_detail["teamBTT"][count])
        round_detail_for_csv["map"].append(round_detail["map"])
        round_detail_for_csv["round"].append(round_detail["rdef"][count])
        round_detail_for_csv["winCon"].append(round_detail["winConDef"][count])
        round_detail_for_csv["date"].append(round_detail["date"])
        round_detail_for_csv["map_order"].append(round_detail["map_order"])
        round_detail_for_csv["event"].append(round_detail["event"])

    save_round_detail_to_csv(round_detail_for_csv, folder=folder, encoding=encoding)

    team_b_prespective = {
        "teamA": round_detail_for_csv["teamB"],
        "map": round_detail_for_csv["map"],
        "side": ["def" if "atk" in x else "atk" for x in round_detail_for_csv["side"]],
        "teamB": round_detail_for_csv["teamA"],
        "rndA": round_detail_for_csv["rndB"],
        "rndB": round_detail_for_csv["rndA"],
        "round": round_detail_for_csv["round"],
        "winCon": round_detail_for_csv["winCon"],
        "date": round_detail_for_csv["date"],
        "map_order": round_detail_for_csv["map_order"],
        'event': round_detail_for_csv["event"]
    }

    save_round_detail_to_csv(team_b_prespective, folder=folder, encoding=encoding)


def get_round_detail(soup, basic_match_info=None, folder="csv", encoding="utf-8"):
    """extract round info from a vlr match.

    Args:
        soup (bs4.BeautifulSoup): BeautifulSoup object with the HTML info
        basic_match_info (dict, optional): basic match info dict. Defaults to None.
        folder (str, optional): folder to save the extracted data. Defaults to "csv".
        encoding (str, optional): encoding to save the extracted data. Defaults to "utf-8".

    Returns:
        dict: round info dict
    """
    if basic_match_info is None:
        print("basic_match_info required")

    round_info = {
        "team_a": None,
        "team_b": None,
        "map": None,
        "teamACT": [],
        "teamATT": [],
        "teamBCT": [],
        "teamBTT": [],
        "ratk": [],
        "rdef": [],
        "winConAtk": [],
        "winConDef": [],
        "date": None,
        "map_order": None,
        "event": None,
    }

    maps = []

    map_div = soup.find_all("div", class_="map")

    for map in map_div:
        map_name_span = map.find("span", attrs={"style": "position: relative;"})
        map_name = map_name_span.find(string=True, recursive=False).strip()
        maps.append(map_name)

    bloques = soup.find_all("div", class_="vlr-rounds-row-col")
    control_value = 0
    mapNumber = 0

    round_info["date"] = basic_match_info["date"]
    round_info["map"] = maps[mapNumber]
    round_info["map_order"] = mapNumber

    round_info["event"] = basic_match_info["event"]

    for count, ronda in enumerate(bloques):
        try:
            round_info["team_a"] = basic_match_info["team_a_tricode"]
            round_info["team_b"] = basic_match_info["team_b_tricode"]
            value = int(ronda.find_all("div", class_="rnd-num")[0].text.strip())
            imgUrl = str(ronda.find_all("img")[0])
            victory_condition = imgUrl[0:-3].split("/")[-1].rstrip(".webp")

            if value >= control_value:
                control_value = value
                round_for_eval = re.findall(r"rnd-sq(.*)", str(bloques[count]))
                if round_for_eval[0] == ' mod-win mod-ct">':
                    round_info["teamACT"].append(1)
                    round_info["teamBTT"].append(0)
                    round_info["rdef"].append(value)
                    round_info["winConDef"].append(victory_condition)
                elif round_for_eval[0] == ' mod-win mod-t">':
                    round_info["teamATT"].append(1)
                    round_info["teamBCT"].append(0)
                    round_info["ratk"].append(value)
                    round_info["winConAtk"].append(victory_condition)
                if round_for_eval[1] == ' mod-win mod-ct">':
                    round_info["teamBCT"].append(1)
                    round_info["teamATT"].append(0)
                    round_info["ratk"].append(value)
                    round_info["winConAtk"].append(victory_condition)
                elif round_for_eval[1] == ' mod-win mod-t">':
                    round_info["teamBTT"].append(1)
                    round_info["teamACT"].append(0)
                    round_info["rdef"].append(value)
                    round_info["winConDef"].append(victory_condition)

            else:
                mapNumber += 1
                control_value = value
                round_detail_to_dict(round_info, folder=folder, encoding=encoding)
                round_info = {
                    "team_a": None,
                    "team_b": None,
                    "map": None,
                    "teamACT": [],
                    "teamATT": [],
                    "teamBCT": [],
                    "teamBTT": [],
                    "ratk": [],
                    "rdef": [],
                    "winConAtk": [],
                    "winConDef": [],
                    "date": None,
                    "map_order": None,
                    "event": None,
                }

                round_info["team_a"] = basic_match_info["team_a_tricode"]
                round_info["team_b"] = basic_match_info["team_b_tricode"]

                round_info["map_order"] = mapNumber
                round_info["map"] = maps[mapNumber]
                round_for_eval = re.findall(r"rnd-sq(.*)", str(bloques[count]))
                imgUrl = str(ronda.find_all("img")[0])
                victory_condition = imgUrl[0:-3].split("/")[-1].rstrip(".webp")

                round_info["date"] = basic_match_info["date"]
                round_info["event"] = basic_match_info["event"]

                if round_for_eval[0] == ' mod-win mod-ct">':
                    round_info["teamACT"].append(1)
                    round_info["teamBTT"].append(0)
                    round_info["rdef"].append(value)
                    round_info["winConDef"].append(victory_condition)
                elif round_for_eval[0] == ' mod-win mod-t">':
                    round_info["teamATT"].append(1)
                    round_info["teamBCT"].append(0)
                    round_info["ratk"].append(value)
                    round_info["winConAtk"].append(victory_condition)
                if round_for_eval[1] == ' mod-win mod-ct">':
                    round_info["teamBCT"].append(1)
                    round_info["teamATT"].append(0)
                    round_info["ratk"].append(value)
                    round_info["winConAtk"].append(victory_condition)
                elif round_for_eval[1] == ' mod-win mod-t">':
                    round_info["teamBTT"].append(1)
                    round_info["teamACT"].append(0)
                    round_info["rdef"].append(value)
                    round_info["winConDef"].append(victory_condition)

        except:
            pass
    round_detail_to_dict(round_info, folder=folder, encoding=encoding)
    return round_info


def get_player_performance(url, basic_match_info):
    """extract the player performance from a vlr match performance tab

    Args:
        url (str): BeautifulSoup object with the HTML info
        basic_match_info (dict, optional): basic match info dict. Defaults to None.

    Returns:
        dict: player performance dict
    """
    performance_dict = {
        "player": [],
        'team': [],
        "2K": [],
        "3K": [],
        "4K": [],
        "5K": [],
        "1v1": [],
        "1v2": [],
        "1v3": [],
        "1v4": [],
        "1v5": [],
        "ECON": [],
        "PL": [],
        "DE": [],
        "map": [],
        "date": [],
        'event': []
    }

    performance_tab = '/?game=all&tab=performance'

    url_performance = url + performance_tab

    soup_performance = soup_open(url_performance)

    bo = int(basic_match_info["bo"])  # Could be not necesary to do this check

    status = basic_match_info["status"]

    if status == "final" and (bo == 3 or bo == 5):
        get_games_id = soup_performance.find_all("div", {"class": "vm-stats-game"})
        game_ids = [
            div.get("data-game-id") for div in get_games_id if div.has_attr("data-game-id")
        ]

        map_list = ["all"]

        maps = soup_performance.find_all(
            "div", {"class": "vm-stats-gamesnav-item js-map-switch"}
        )
        for map in maps:
            map_list.append(map.get_text(strip=True)[1:])

        for index, id in enumerate(game_ids):
            div = soup_performance.find("div", {"class": "vm-stats-game", "data-game-id": id})
            test_div = div.find_all("tr")[1:]
            pre_process = []
            for element in test_div:
                if len(element) > 13:
                    pre_process.append(element)

            filas = pre_process[1:]

            for fila in filas:
                celdas = fila.find_all("td")

                if len(celdas) > 0:  # Check if info is valid (map is played)

                    jugador_div = celdas[0].find("div").find_all("div")[0]

                    nombre_jugador = jugador_div.get_text().split()

                    def extraer_numero(text):
                        match = re.match(r"^\d+", text)
                        return int(match.group()) if match else 0

                    performance_dict["player"].append(nombre_jugador[0])
                    performance_dict["team"].append(nombre_jugador[1])
                    performance_dict["2K"].append(
                        extraer_numero(celdas[2].get_text(strip=True))
                    )
                    performance_dict["3K"].append(
                        extraer_numero(celdas[3].get_text(strip=True))
                    ),
                    performance_dict["4K"].append(
                        extraer_numero(celdas[4].get_text(strip=True))
                    ),
                    performance_dict["5K"].append(
                        extraer_numero(celdas[5].get_text(strip=True))
                    ),
                    performance_dict["1v1"].append(
                        extraer_numero(celdas[6].get_text(strip=True))
                    ),
                    performance_dict["1v2"].append(
                        extraer_numero(celdas[7].get_text(strip=True))
                    ),
                    performance_dict["1v3"].append(
                        extraer_numero(celdas[8].get_text(strip=True))
                    ),
                    performance_dict["1v4"].append(
                        extraer_numero(celdas[9].get_text(strip=True))
                    ),
                    performance_dict["1v5"].append(
                        extraer_numero(celdas[10].get_text(strip=True))
                    ),
                    performance_dict["ECON"].append(
                        extraer_numero(celdas[11].get_text(strip=True))
                    ),
                    performance_dict["PL"].append(
                        extraer_numero(celdas[12].get_text(strip=True))
                    ),
                    performance_dict["DE"].append(
                        extraer_numero(celdas[13].get_text(strip=True))
                    )
                    performance_dict["date"].append(basic_match_info["date"])
                    performance_dict["event"].append(basic_match_info["event"])
                    performance_dict["map"].append(map_list[index])

    return performance_dict


def get_team_economy(url, basic_match_info):
    """extract the team economy

    Args:
        url (str): vlr match url
        basic_match_info (dict): basic match info dict. Defaults to None.
    """
    economy_dict = {
        "team_a": [],
        "team_b": [],
        "team_a_economy": [],
        "team_b_economy": [],
        "round": [],
        "team_a_bank": [],
        "team_b_bank": [],
        "map": [],
        "date": [],
        'event': [],
    }
    economy_page = url + "/?game=all&tab=economy"

    soup = soup_open(url)
    soup_economy = soup_open(economy_page)

    get_games_id = soup_economy.find_all("div", {"class": "vm-stats-game"})
    game_ids = [
        div.get("data-game-id") for div in get_games_id if div.has_attr("data-game-id")
    ]

    map_dict = {}

    map_nav_items = soup.select(".vm-stats-gamesnav-item.js-map-switch")

    for item in map_nav_items:
        game_id = item.get("data-game-id")
        map_name = item.get_text(strip=True)[1:]  # Remueve el símbolo inicial como 🗺️

        if game_id:  # Filtramos los que tienen ID válido
            map_dict[game_id] = map_name

    event = basic_match_info["event"]
    date = basic_match_info["date"]

    for value, id in enumerate(game_ids[:len(map_dict) - 1]):

        div = soup_economy.find("div", {"class": "vm-stats-game", "data-game-id": id})
        test_div = div.find_all("tr")[1:]

        teams = []
        round = 0
        comments = div.find_all(string=lambda text: isinstance(text, Comment))

        both_team_economy = []
        for comment in comments:
            comment_soup = BeautifulSoup(comment, 'html.parser')
            div = comment_soup.find('div')
            if div and div.text.strip():
                both_team_economy.append(div.text.strip())

        for index, element in enumerate(both_team_economy):
            if index % 2 != 0:
                economy_dict["team_a_economy"].append(element)

            else:
                economy_dict["team_b_economy"].append(element)

        for fila in test_div[1:]:
            celdas = fila.find_all("td")
            if len(teams) < 2:
                teams.append(
                    celdas[0]
                    .find_all("div", {"class": "team"})[0]
                    .get_text(strip=True)
                )
            for bank in celdas:
                team_bank = bank.find_all("div", {"class": "bank"})
                if len(team_bank) > 0:
                    round += 1
                    economy_dict["team_a"].append(teams[0])
                    economy_dict["team_b"].append(teams[1])
                    economy_dict["team_a_bank"].append(
                        team_bank[0].get_text(strip=True)
                    )
                    economy_dict["team_b_bank"].append(
                        team_bank[1].get_text(strip=True)
                    )
                    economy_dict["round"].append(round)
                    economy_dict["map"].append(map_dict.get(id, "Unknown"))
                    economy_dict["date"].append(date)
                    economy_dict['event'].append(event)

        team_b_economy_dict = {
            "team_a": economy_dict["team_b"],
            "team_b": economy_dict["team_a"],
            "team_a_economy": economy_dict["team_b_economy"],
            "team_b_economy": economy_dict["team_a_economy"],
            "round": economy_dict["round"],
            "team_a_bank": economy_dict["team_b_bank"],
            "team_b_bank": economy_dict["team_a_bank"],
            "map": economy_dict["map"],
            "date": economy_dict["date"],
            'event': economy_dict["event"],
        }

    return [economy_dict, team_b_economy_dict]


def get_player_stats(soup, basic_match_info):
    """extract player stats from a vlr match and return a dict

    Args:
        soup (bs4.BeautifulSoup): BeautifulSoup object with the HTML info
        basic_match_info (dict, optional): basic match info dict. Defaults to None.

    Returns:
        dict: player stats dict
    """
    map_tracker = 0
    teams_check_set = set()
    last_team = None
    map_list = []

    maps = soup.find_all(
        "div", {"class": "vm-stats-gamesnav-item js-map-switch"}
    )
    for map in maps:
        map_list.append(map.get_text(strip=True)[1:])

    map_list.insert(1, "all")

    player_stats = {
        "team": [],
        "player": [],
        "agent": [],
        "ratingBoth": [],
        "ratingT": [],
        "rating-ct": [],
        "acsBoth": [],
        "acsT": [],
        "acsCT": [],
        "killsBoth": [],
        "killsT": [],
        "killsCT": [],
        "deadBoth": [],
        "deadT": [],
        "deadCT": [],
        "assistsBoth": [],
        "assistsT": [],
        "assistsCT": [],
        "k-dBoth": [],
        "k-dT": [],
        "k-dCT": [],
        "kastBoth": [],
        "kastT": [],
        "kastCT": [],
        "adrBoth": [],
        "adrT": [],
        "adrCT": [],
        "hsBoth": [],
        "hsT": [],
        "hsCT": [],
        "fkBoth": [],
        "fkT": [],
        "fkCT": [],
        "fdBoth": [],
        "fdT": [],
        "fdCT": [],
        "fk-fdBoth": [],
        "fk-fdT": [],
        "fk-fdCT": [],
        "map": [],
        "date": [],
        "event": [],
    }

    date = basic_match_info["date"]
    event = basic_match_info["event"]

    extractPlayer = soup.find_all(
        "td", class_="mod-player"
    )

    for name in extractPlayer:
        player = name.find(class_="text-of").get_text()
        player = player.strip().split(";")
        team_name = name.find(class_="ge-text-light").get_text(strip=True)

        player_stats["player"].append(player[0])
        player_stats["team"].append(team_name)
        player_stats["date"].append(date)
        player_stats["event"].append(event)

        if last_team is None:
            last_team = team_name
            teams_check_set.add(team_name)

        if team_name == last_team:
            player_stats["map"].append(map_list[map_tracker])

        elif len(teams_check_set) != 2 and team_name != last_team:
            teams_check_set.add(team_name)
            last_team = team_name
            player_stats["map"].append(map_list[map_tracker])

        elif len(teams_check_set) == 2 and team_name != last_team:
            map_tracker += 1
            last_team = team_name
            teams_check_set = set()
            teams_check_set.add(last_team)
            player_stats["map"].append(map_list[map_tracker])

    # Extract agent played
    extractAgent = soup.find_all("td", class_="mod-agents")
    for agent in extractAgent:
        agent = agent.find("img").get("title")
        player_stats["agent"].append(agent)

    # Extract VLR rating, ACS, Kills both sides
    extratRating = soup.find_all("td", class_="mod-stat")
    contador = 0
    for mod in extratRating:
        try:
            if contador == 0:
                mod = mod.find(class_="side mod-side mod-both").get_text()
                player_stats["ratingBoth"].append(float(mod))
                contador += 1
            if contador == 1:
                mod = mod.find(class_="side mod-side mod-both").get_text()
                player_stats["acsBoth"].append(float(mod))
                contador += 1
            if contador == 2:
                mod = mod.find(class_="side mod-side mod-both").get_text()
                player_stats["killsBoth"].append(float(mod))
                contador = 0
        except:
            pass

    # Extract VLR rating, ACS, Kills attack side
    contador = 0
    for mod in extratRating:
        try:
            if contador == 0:
                mod = mod.find(class_="side mod-side mod-t").get_text()
                player_stats["ratingT"].append(float(mod))
                contador += 1
            if contador == 1:
                mod = mod.find(class_="side mod-side mod-t").get_text()
                player_stats["acsT"].append(float(mod))
                contador += 1
            if contador == 2:
                mod = mod.find(class_="side mod-side mod-t").get_text()
                player_stats["killsT"].append(float(mod))
                contador = 0
        except:
            pass

    # Extract VLR rating, ACS, Kills defend side
    contador = 0
    for mod in extratRating:
        try:
            if contador == 0:
                mod = mod.find(class_="side mod-side mod-ct").get_text()
                player_stats["rating-ct"].append(float(mod))
                contador += 1
            if contador == 1:
                mod = mod.find(class_="side mod-side mod-ct").get_text()
                player_stats["acsCT"].append(float(mod))
                contador += 1
            if contador == 2:
                mod = mod.find(class_="side mod-side mod-ct").get_text()
                player_stats["killsCT"].append(float(mod))
                contador = 0
        except:
            pass

    # Stats Both Kills, assits,k-d,kast,adr,hs,fk,fd,fk-fd
    extractText = soup.find_all("td", class_="mod-stat")
    contador = 0
    for mod in extractText:
        try:
            valorTemp = mod.find(
                True,
                {
                    "class": [
                        "side mod-both",
                        "side mod-both mod-positive",
                        "side mod-both mod-negative",
                    ]
                },
            ).get_text()

            if contador == 0:
                player_stats["deadBoth"].append(float(valorTemp))
                contador += 1
            elif contador == 1:
                player_stats["assistsBoth"].append(float(valorTemp))
                contador += 1
            elif contador == 2:
                player_stats["k-dBoth"].append(float(valorTemp))
                contador += 1
            elif contador == 3:
                valor = valorTemp.replace('\xa0', '0%')
                player_stats["kastBoth"].append(valor)
                contador += 1
            elif contador == 4:
                player_stats["adrBoth"].append(float(valorTemp))
                contador += 1
            elif contador == 5:
                valor = valorTemp.replace('\xa0', '0%')
                player_stats["hsBoth"].append(valor)
                contador += 1
            elif contador == 6:
                player_stats["fkBoth"].append(float(valorTemp))
                contador += 1
            elif contador == 7:
                player_stats["fdBoth"].append(float(valorTemp))
                contador += 1
            elif contador == 8:
                player_stats["fk-fdBoth"].append(float(valorTemp))
                contador = 0
        except:
            pass

    contador = 0
    for mod in extractText:
        try:
            valorTemp = mod.find(
                True,
                {
                    "class": [
                        "side mod-t",
                        "side mod-t mod-positive",
                        "side mod-t mod-negative",
                    ]
                },
            ).get_text()
            if contador == 0:
                player_stats["deadT"].append(float(valorTemp))
                contador += 1
            elif contador == 1:
                player_stats["assistsT"].append(float(valorTemp))
                contador += 1
            elif contador == 2:
                player_stats["k-dT"].append(float(valorTemp))
                contador += 1
            elif contador == 3:
                valor = valorTemp.replace('\xa0', '0%')
                player_stats["kastT"].append(valor)
                contador += 1
            elif contador == 4:
                player_stats["adrT"].append(float(valorTemp))
                contador += 1
            elif contador == 5:
                valor = valorTemp.replace('\xa0', '0%')
                player_stats["hsT"].append(valor)
                contador += 1
            elif contador == 6:
                player_stats["fkT"].append(float(valorTemp))
                contador += 1
            elif contador == 7:
                player_stats["fdT"].append(float(valorTemp))
                contador += 1
            elif contador == 8:
                player_stats["fk-fdT"].append(float(valorTemp))
                contador = 0
        except:
            pass

    contador = 0
    for mod in extractText:
        try:
            valorTemp = mod.find(
                True,
                {
                    "class": [
                        "side mod-ct",
                        "side mod-ct mod-positive",
                        "side mod-ct mod-negative",
                    ]
                },
            ).get_text()
            if contador == 0:
                player_stats["deadCT"].append(float(valorTemp))
                contador += 1
            elif contador == 1:
                player_stats["assistsCT"].append(float(valorTemp))
                contador += 1
            elif contador == 2:
                player_stats["k-dCT"].append(float(valorTemp))
                contador += 1
            elif contador == 3:
                valor = valorTemp.replace('\xa0', '0%')
                player_stats["kastCT"].append(valor)
                contador += 1
            elif contador == 4:
                player_stats["adrCT"].append(float(valorTemp))
                contador += 1
            elif contador == 5:
                valor = valorTemp.replace('\xa0', '0%')
                player_stats["hsCT"].append(valor)
                contador += 1
            elif contador == 6:
                player_stats["fkCT"].append(float(valorTemp))
                contador += 1
            elif contador == 7:
                player_stats["fdCT"].append(float(valorTemp))
                contador += 1
            elif contador == 8:
                player_stats["fk-fdCT"].append(float(valorTemp))
                contador = 0
        except:
            pass

    return player_stats

def check_valid_match(soup):
    """Check match validity

    Args:
        soup (bs4.BeautifulSoup): BeautifulSoup object with the HTML info

    Returns:
        bool: False for a valid match
    """
    event_text = soup.find("title").get_text(strip=True)
    regex = r"^([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)$"
    result = re.search(regex, event_text)

    match_notes = soup.find_all("div", {"class": "match-header-vs-note"})
    status = match_notes[0].get_text().strip()

    if result.group(3).strip() == "Showmatch" or status != "final":
        valid_match = False
    else:
        valid_match = True
    return valid_match


def link_extractor(url):
    """extrack all the matches from a vlr tournament match page

    Args:
        url (str): vlr tournament match page

    Returns:
        list: list with the url of all the matchs in the matches page
    """
    soup = soup_open(url)

    tempLink = []
    urlLinkExtract = []
    for a in soup.find_all("a", href=True):
        tempLink.append(a["href"])
        filtered_links = [link for link in tempLink if re.match(r"^/\d+", link)]

    for cleanLink in filtered_links:
        urlLinkExtract.append("https://www.vlr.gg" + cleanLink)

    return urlLinkExtract


def get_draft_file_path(basic_match_info, folder="csv"):
    """from basic_match_info dict get the tournament name and create the path using the folder

    Args:
        basic_match_info (dict, optional): basic match info dict. Defaults to None.
        folder (str, optional): folder name. Defaults to "csv".

    Returns:
        str: path with the file name
    """
    normalized_tournament = normalize_filename(basic_match_info["event"])
    folder_path = os.path.join(folder, normalized_tournament)
    os.makedirs(folder_path, exist_ok=True)
    return os.path.join(folder_path, f"draft_{normalized_tournament}.csv")


def was_url_already_processed(file_path, url):
    """check if the url was already process

    Args:
        file_path (str): file path for draft
        url (str): url to verify

    Returns:
        bool: True if ulr is already process
    """
    if not os.path.exists(file_path):
        return False
    df = pd.read_csv(file_path)
    return url in set(df.source_url)


def process_match(url, folder="csv", encoding="utf-8"):
    """main function to process match url

    Args:
        url (str): match url from vlr
        folder (str, optional): folder name. Defaults to "csv".
        encoding (str, optional): encoding. Defaults to "utf-8".
    """
    time.sleep(random.randint(1, 2))
    soup = soup_open(url)
    error_url = {"event": [], "url": [], "error": []}
    if check_valid_match(soup):
        # print(f"processing: {url}")
        basic_match_info = get_basic_match_info(soup)
        path = get_draft_file_path(basic_match_info=basic_match_info, folder=folder)
        # Check if match is processed
        not_processed = not was_url_already_processed(file_path=path, url=url)
        if not_processed:
            try:
                # Draft
                draft = get_picks_bans(soup=soup, basic_match_info=basic_match_info)
                save_draft_to_csv(draft, url, folder=folder, encoding=encoding)

                # Round detail
                get_round_detail(
                    soup=soup,
                    basic_match_info=basic_match_info,
                    folder=folder,
                    encoding=encoding,
                )

                # Player performance
                performance_dict = get_player_performance(
                    url=url, basic_match_info=basic_match_info
                )
                save_player_performance_to_csv(
                    player_performance_dict=performance_dict,
                    folder=folder,
                    encoding=encoding,
                )

                # Team economy
                team_economy_dict = get_team_economy(
                    url, basic_match_info=basic_match_info
                )
                save_team_economy(
                    team_economy_dict[0], folder=folder, encoding=encoding
                )
                save_team_economy(
                    team_economy_dict[1], folder=folder, encoding=encoding
                )

                # Player stats
                player_stats_dict = get_player_stats(
                    soup=soup, basic_match_info=basic_match_info
                )
                save_player_stats_to_csv(
                    player_stats_dict, folder=folder, encoding=encoding
                )
            except Exception as e:
                print(f"error processing {url}: {e}")
                error_url["event"].append(basic_match_info["event"])
                error_url["url"].append(url)
                error_url["error"].append(e)
                save_match_error(match_error_dict=error_url,folder=folder,encoding=encoding)

        else:
            print(f"already processed: {url}")

    else:
        print(f"Not valid match: {url}")