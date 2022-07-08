#!/usr/bin/env python3

"""Generates synonyms for a batch of software mentions by using the Jaro Winkler string similarity algorithm

Usage:
    python generate_synonyms_string_similartiy.py --input-file <input_file>

Author:
    Ana-Maria Istrate
"""

import pandas as pd
import pickle
import argparse
import ast
import time
import os
import textdistance

ROOT_DIR = "data/"

def save_result_to_file(synonym_map, synonym_confidences, software_mentions_df, ID_start, ID_end):
  """
  Saves result to file.
  
  :param synonym_map: map from {software_mention : synonyms}
  :param synonym_confidences: map from {software_mention : synonym_confidences}
  :param mentions_batch_df: software_mentions_df to augment with synonyms
  :param ID_start: ID_start for mentions in software_mentions_df
  :param ID_end: ID_end for mentions in software_mentions_df
  """
  for software, synonyms in synonym_map.items():
    synonyms_confs = synonym_confidences[software]
    software_mentions_df.loc[software_mentions_df['software_mention'] == software, 'synonyms'] = str(synonyms)
    software_mentions_df.loc[software_mentions_df['software_mention'] == software, 'synonyms_confs'] = str(synonyms_confs)
  software_mentions_df.to_csv(ROOT_DIR + 'synonym_string_similarity_' + str(ID_start) + "_" + str(ID_end) + '.csv')
  
def save_result_to_file_spark(result, software_mentions_df, ID_start, ID_end):
  """
  Converts Spark result to df and saves it to file.
  
  :param result: result to save
  :param software_mentions_df: software_mentions_df to augment with synonyms
  :param ID_start: ID_start for mentions in software_mentions_df
  :param ID_end: ID_end for mentions in software_mentions_df
  """
  for entry in result:
    if len(entry[0]) > 0:
      software = list(entry[0].keys())[0]
      synonyms = str(entry[0][software])
      synonyms_confs = str(entry[1][software])
      software_mentions_df.loc[software_mentions_df['software_mention'] == software, 'synonyms'] = synonyms
      software_mentions_df.loc[software_mentions_df['software_mention'] == software, 'synonyms_confs'] = synonyms_confs
  software_mentions_df.to_csv(ROOT_DIR + 'synonym_string_similarity_' + str(ID_start) + "_" + str(ID_end) + '.csv')
  
def get_string_similarity_synonyms(software_mentions, all_software_mentions, threshold = 0.9):
  """
   Generates synonyms for a given list of packages using the Jaro Winkler string similarity algorithm. 
   Only keeps synonyms with a confidence of at least 0.9.
  
  :param software_mentions: the list of software packages to get synonyms for
  :param all_software_mentions: the full list of software packages to choose synonyms from
  
  :return synonym_map: mapping from {software_mention : synonyms}
  :return synonym_confidences: mapping from {software_mention : synonyms_confidences}
  """
  jaro_winkler_distance = textdistance.JaroWinkler(qval = 2).normalized_similarity
  synonym_map = {}
  synonym_confidences = {}
  all_software_mentions_clean = [x for x in all_software_mentions if (x == x and len(x) >=2)]
  for software_mention in software_mentions:
    if software_mention != software_mention:
      continue
    distances = [jaro_winkler_distance(software_mention, y) for y in all_software_mentions_clean]
    for synonym, distance in zip(all_software_mentions_clean, distances):
      if distance >= threshold:
        if software_mention in synonym_map:
          synonym_map[software_mention].append(synonym)
          synonym_confidences[software_mention].append(distance)
        else:
          synonym_map[software_mention] = [synonym]
          synonym_confidences[software_mention] = [distance]
  return synonym_map, synonym_confidences

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  
  parser.add_argument('--mention2ID-file', type=str, default = ROOT_DIR + 'mention2ID.pkl')
  parser.add_argument('--ID_start', type=int, default = 0)
  parser.add_argument('--ID_end', type=int, default = 10)
  parser.add_argument('--use-spark', type=bool, default = False)

  args, _ = parser.parse_known_args()
  
  ID_start = args.ID_start
  ID_end = args.ID_end
  
  mention2ID = pickle.load(open(args.mention2ID_file, 'rb'))
  all_software_mentions = list(mention2ID.keys())
  
  mentions_batch = all_software_mentions[ID_start:ID_end]
  mentions_batch_IDs = [mention2ID[x] for x in mentions_batch]
  mentions_batch_df = pd.DataFrame({'ID' : mentions_batch_IDs, 'software_mention' : mentions_batch})
  num_mentions_batch = len(mentions_batch)
  
  # Non-Spark implementation
  if args.use_spark:
     # Spark implementation
    distData = sc.parallelize(mentions_batch, 2)
    rdd = distData.map(lambda x: get_string_similarity_synonyms([x], all_software_mentions, threshold = 0.9))
    result = rdd.collect()
    save_result_to_file_spark(result, mentions_batch_df, ID_start, ID_end)
  else:
    synonym_map, synonym_confidences = get_string_similarity_synonyms(mentions_batch, all_software_mentions, threshold = 0.9)
    save_result_to_file(synonym_map, synonym_confidences, mentions_batch_df, ID_start, ID_end)