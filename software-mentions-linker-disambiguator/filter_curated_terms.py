#!/usr/bin/env python3

"""Filters curated terms from raw dataset of software mentions. Assumes an existing mention2ID correspondence. 

Usage:
    python filter_curated_terms.py

Author:
    Ana-Maria Istrate
"""

import pandas as pd
import argparse
import pickle

ROOT_DIR = "data/"

def get_curation_label(ID, software_IDs, not_software_IDs, unclear_IDs):
  """
  Returns a curation label for a given software mention ID
  
  :param ID: ID of software mention
  :param software_IDs: IDs of mentions curated as software
  :param not_software_IDs: IDs of mentions curated as not_software
  :param unclear_IDs: IDs of mentions curated as unclear
  
  :return: curated label
  """
  if ID in software_IDs:
    return 'software'
  if ID in not_software_IDs:
    return 'not_software'
  if ID in unclear_IDs:
    return 'unclear'
  return 'not_curated'
  
if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  
  parser.add_argument("--software-mentions-file", type=str, help="Input file", default = ROOT_DIR + 'comm_IDs.tsv.gz', required = False)
  parser.add_argument("--curated-terms", type=str, help="File containing curated terms", default = ROOT_DIR + 'curation_top10k_mentions_binary_labels.csv', required = False)
  parser.add_argument("--mention2ID-file", type=str, help="mention2ID mapping", default = ROOT_DIR + 'mention2ID.pkl', required = False)
  parser.add_argument("--output-curated-dataset", type=str, help="Output file for curated dataset", default = ROOT_DIR + 'comm_curated.tsv.gz', required = False)
  parser.add_argument("--output-augmented-dataset", type=str, help="Output file for augemented dataset", default = ROOT_DIR + 'comm_with_labels.tsv.gz', required = False)

  args, _ = parser.parse_known_args()
  raw_mentions_df = pd.read_csv(args.software_mentions_file, sep = '\\t', engine = 'python', compression = 'gzip')
  curated_terms = pd.read_csv(args.curated_terms)
  mention2ID = pickle.load(open(args.mention2ID_file, 'rb'))
  software_IDs = set(curated_terms[curated_terms['label'] == 'software']['ID'].values)
  not_software_IDs = set(curated_terms[curated_terms['label'] == 'not_software']['ID'].values)
  unclear_IDs = set(curated_terms[curated_terms['label'] == 'unclear']['ID'].values)
  
  # Exclude terms marked as not_software
  curated_mentions_df = raw_mentions_df.copy()
  curated_mentions_df = curated_mentions_df[~(curated_mentions_df['ID'].isin(not_software_IDs))]
  curated_mentions_df.to_csv(args.output_curated_dataset, sep = '\t', compression = 'gzip', index = False)

  # Adds a curation label to the raw dataset and saves to file
  raw_mentions_df['curation_label'] = raw_mentions_df['ID'].apply(lambda x: get_curation_label(x, software_IDs, not_software_IDs, unclear_IDs))
  
  # Print metrics
  dataset_software = len(raw_mentions_df[raw_mentions_df['curation_label'] == 'software'])
  dataset_not_software = len(raw_mentions_df[raw_mentions_df['curation_label'] == 'not_software'])
  dataset_unclear = len(raw_mentions_df[raw_mentions_df['curation_label'] == 'unclear'])
  dataset_not_curated = len(raw_mentions_df[raw_mentions_df['curation_label'] == 'not_curated'])
  num_total = len(raw_mentions_df)

  print('% software:', (dataset_software / num_total) * 100)
  print('% not-software:', (dataset_not_software / num_total) * 100)
  print('% unclear:', (dataset_unclear / num_total) * 100)
  print('% not-curated:', (dataset_not_curated / num_total) * 100)
  
  raw_mentions_df.to_csv(args.output_augmented_dataset, sep = '\t', compression = 'gzip', index = False)