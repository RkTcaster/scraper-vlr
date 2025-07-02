import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os


def convert_k(valor):
    "remove k in money columns and change the format to int"
    if 'k' in valor:
        return int(float(valor.replace('k', '')) * 1000)


def find_files_by_prefix(root_folder, prefix):
    matched_files = []
    for dirpath, _, filenames in os.walk(root_folder):
        for file in filenames:
            if file.startswith(prefix):
                full_path = os.path.join(dirpath, file)
                matched_files.append(full_path)
    return matched_files


def concat_from_list(file_list, encoding='iso-8859-1'):
    dataframes = []
    for file in file_list:
        try:
            df = pd.read_csv(file, encoding=encoding)
            if not df.empty:
                dataframes.append(df)
            else:
                print(f"empty file: {file}")
        except Exception as e:
            print(f"Error reading {file}: {e}")

    if dataframes:
        return pd.concat(dataframes, ignore_index=True)
    else:
        print("Load file fail")
        return pd.DataFrame()


def concat_csv_from_different_folders(folder="csv", prefix=None):
    if prefix is None:
        print("Add a prefix")

    file_list = find_files_by_prefix(root_folder=folder, prefix=prefix)
    df_concat = concat_from_list(file_list)
    return df_concat


def get_game_instance(value):
    last_char = value.split("-")[-1]
    return last_char


def text_to_index(df, name, number=0, extra_id=""):
    name_id = name + '_id'
    df[name_id] = df.index + number
    df[name_id] = name + "_" + df[name_id].astype(str) + extra_id
    return df


def tournament_names(folder='csv'):
    tournament_list = []
    for name in os.listdir(folder):
        path = os.path.join(folder, name)
        if os.path.isdir(path):
            tournament_list.append(name)
    return tournament_list


def region_by_id(touranment_name, region):
    for _, row in region.iterrows():
        if row['region'].lower() in touranment_name.lower():
            return row['reg_id']
    return "reg_4"


def create_draft_table(df):
    filas_transformadas = []

    for index, row in df.iterrows():
        picks = [
            row['team_1_select_1'], row['team_2_select_1'],
            row['team_1_select_2'], row['team_2_select_2'],
            row['team_1_select_3'], row['team_2_select_3'],
            row['decider']
        ]

        for pick_num, map_name in enumerate(picks, start=1):
            filas_transformadas.append({
                'team': row['team'],
                'series_id': row['series_id'],
                'order': row['order'],
                'bo': row['bo'],
                'pick': pick_num,
                'map_name': map_name,
                "match_instance": row["match_instance"]
            })

    new_df = pd.DataFrame(filas_transformadas)
    return new_df


def first_ban(row):
    if (row['match_instance'] != "gf"):
        if (row['pick'] == 1):
            return 1
        else:
            return 0
    elif (row['match_instance'] == "gf"):
        if (row['pick'] == 1 or row['pick'] == 2):
            return 1
        else:
            return 0

    else:
        return 0


def second_ban(row):
    if row['pick'] == 5 and row['bo'] == 3:
        return 1
    else:
        return 0


def first_pick(row):
    if row['pick'] == 3:
        return 1
    else:
        return 0


def second_pick(row):
    if row['pick'] == 5 and row['bo'] == 5:
        return 1
    else:
        return 0


def decider_pick(row):
    if row['pick'] == 7:
        return 0.5
    else:
        return 0

