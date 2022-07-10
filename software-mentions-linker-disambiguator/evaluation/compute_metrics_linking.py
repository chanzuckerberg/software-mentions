#!/usr/bin/env python3

"""Computes metrics for linking based on evaluation_linking.py file

Usage:
    python compute_metrics_linking.py

Author:
    Ana-Maria Istrate
"""

import pandas as pd
import argparse

ROOT_DIR = "../data/curation/"

def get_metrics(linking_df):
  """
  Compute metrics for links as available in linking_df
  
  """
  num_correct = len(linking_df[linking_df['link_label'] == 'correct'])
  num_incorrect = len(linking_df[linking_df['link_label'] == 'incorrect'])
  num_unclear = len(linking_df[linking_df['link_label'] == 'unclear'])
  num_total = num_correct + num_incorrect + num_unclear
  
  print(num_correct, 'correct links, for a total of', num_correct / num_total)
  print(num_incorrect, 'incorrect links, for a total of', num_incorrect / num_total)
  print(num_unclear, 'unclear links, for a total of', num_unclear / num_total)

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  
  parser.add_argument("--linking-evaluation-file", type=str, help="Location of the linking evaluation file", default = ROOT_DIR + 'evaluation_linking.csv', required = False)

  args, _ = parser.parse_known_args()
  
  # Compute metrics for all links
  print('Linking Evaluation')
  print("="*30)
  linking_df = pd.read_csv(args.linking_evaluation_file)[['software_mention', 'link_label']].drop_duplicates()
  get_metrics(linking_df)
  
  # Compute metrics for all links other than Github
  print('Linking Evaluation - without Github')
  print("="*30)
  linking_df_no_github = pd.read_csv(args.linking_evaluation_file)
  linking_df_no_github = linking_df_no_github[linking_df_no_github['source_y'] != 'Github API'][['software_mention', 'link_label']]
  get_metrics(linking_df_no_github)