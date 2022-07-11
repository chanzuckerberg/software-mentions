#!/usr/bin/env python3

"""Computes metrics for disambiguation based on evaluation_disambituation.py file

Usage:
    python compute_metrics_disambiguation.py

Author:
    Ana-Maria Istrate
"""

import pandas as pd
import argparse

ROOT_DIR = "../data/curation/"

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  
  parser.add_argument("--disambiguation-evaluation-file", type=str, help="Location of the disambiguation evaluation file", default = ROOT_DIR + 'evaluation_disambiguation.csv', required = False)

  args, _ = parser.parse_known_args()
  
  # Compute metrics for all links
  print('Disambiguation Evaluation')
  print("="*30)
  label_field = 'synonym_label'
  disambiguated_df = pd.read_csv(args.disambiguation_evaluation_file)[['software_mention', 'synonym', label_field]].drop_duplicates()
  correct = len(disambiguated_df[disambiguated_df[label_field].isin(['Exact', 'Narrow'])])
  unclear = len(disambiguated_df[disambiguated_df[label_field].isin(['Unclear'])])
  incorrect = len(disambiguated_df[disambiguated_df[label_field].isin(['Not synonym'])])
  not_software = len(disambiguated_df[disambiguated_df[label_field].isin(['Not software'])])
  num_total = correct + unclear + incorrect + not_software
  
  correct_perc = correct / num_total * 100
  unclear_perc = unclear / num_total * 100
  incorrect_perc = incorrect / num_total * 100
  not_software_perc = not_software / (num_total + not_software) * 100
  
  print('Correct', correct, correct_perc)
  print('Incorrect', incorrect, incorrect_perc)
  print('Unclear', unclear, unclear_perc)
  print('Not Software', not_software, not_software_perc)