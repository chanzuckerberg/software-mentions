#!/usr/bin/env python3

"""Computes metrics for the curated datasets. 

Usage:
    python compute_metrics_extraction.py

Author:
    Ana-Maria Istrate
"""

import pandas as pd
import argparse
import pickle

ROOT_DIR = "../data/curation/"
  
if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  
  parser.add_argument("--curated-terms-top1k", type=str, help="File containing curated terms", default = ROOT_DIR + 'curation_top1k_mentions_multi_label.csv', required = False)
  parser.add_argument("--curated-terms-top10k", type=str, help="File containing curated terms", default = ROOT_DIR + 'curation_top1k_mentions_multi_label.csv', required = False)

  args, _ = parser.parse_known_args()
  curated_top10k_terms = pd.read_csv(args.curated_terms_top10k)
  num_labeled_software = len(curated_top10k_terms[curated_top10k_terms['label'] == 'software'])
  num_labeled_not_software = len(curated_top10k_terms[curated_top10k_terms['label'] == 'not_software'])
  num_labeled_unclear = len(curated_top10k_terms[curated_top10k_terms['label'] == 'unclear'])
  num_total = len(curated_top10k_terms)
  
  print('Binary label metrics for the top 10k most frequent mentions')
  print("=" * 30)
  print('% software:', (num_labeled_software / num_total) * 100)
  print('% not-software:', (num_labeled_not_software / num_total) * 100)
  print('% unclear:', (num_labeled_unclear / num_total) * 100)
  
  curated_top1k_terms = pd.read_csv(args.curated_terms_top1k)
  num_labeled_software = len(curated_top1k_terms[curated_top1k_terms['multi_label'] == 'software'])
  num_labeled_algorithm = len(curated_top1k_terms[curated_top1k_terms['multi_label'] == 'algorithm'])
  num_labeled_hardware = len(curated_top1k_terms[curated_top1k_terms['multi_label'] == 'hardware'])
  num_labeled_database = len(curated_top1k_terms[curated_top1k_terms['multi_label'] == 'database'])
  num_labeled_web_platform = len(curated_top1k_terms[curated_top1k_terms['multi_label'] == 'web platform'])
  num_labeled_not_software = len(curated_top1k_terms[curated_top1k_terms['multi_label'] == 'other'])
  num_labeled_unclear = len(curated_top1k_terms[curated_top1k_terms['multi_label'] == 'unclear'])
  num_total = len(curated_top1k_terms)
  
  print('\nMulti-label metrics for the top 1k most frequent mentions')
  print("=" * 30)
  print('% software:', num_labeled_software, (num_labeled_software / num_total) * 100)
  print('% algorithm:', num_labeled_algorithm, (num_labeled_algorithm / num_total) * 100)
  print('% hardware:', num_labeled_hardware, (num_labeled_hardware / num_total) * 100)
  print('% database:', num_labeled_database, (num_labeled_database / num_total) * 100)
  print('% web platform:', num_labeled_web_platform, (num_labeled_web_platform / num_total) * 100)
  print('% not-software:', num_labeled_not_software, (num_labeled_not_software / num_total) * 100)
  print('% unclear:', num_labeled_unclear, (num_labeled_unclear / num_total) * 100)