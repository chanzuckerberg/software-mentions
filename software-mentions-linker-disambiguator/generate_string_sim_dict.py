#!/usr/bin/env python3

"""Generates strin_synonyms_dict by concatenating files containing string similarity pairs and confidences
Assumes these files already exist. The files can be created by running the generate_synonyms_string_sim.py script.

Usage:
    python generate_string_sim_dict.py 

Author:
    Ana-Maria Istrate
"""

import pandas as pd
import pickle
import argparse
import ast
import time
import os

ROOT_DIR = "/dbfs/FileStore/meta/user/aistrate/software_entity_linking/"

if __name__ == '__main__':
  string_sim_files = [filename for filename in os.listdir(ROOT_DIR) if filename.startswith("synonym_string_similarity_")]
  string_sim_dfs = []
  for file in string_sim_files:
    print(file)
    df = pd.read_csv(ROOT_DIR + file, index_col = 0)
    string_sim_dfs.append(df)
  string_sim_df = pd.concat(string_sim_dfs)
  string_similarity_dict = {}
  for software_mention, synonyms, synonyms_confs in zip(string_sim_df['software_mention'].values, string_sim_df['synonyms'].values, string_sim_df['synonyms_confs'].values):
    if software_mention and synonyms == synonyms:
      string_similarity_dict[software_mention] = (ast.literal_eval(synonyms), ast.literal_eval(synonyms_confs))
  pickle.dump(string_similarity_dict, open(ROOT_DIR + 'string_similarity_dict_latest.pkl', 'wb+'))