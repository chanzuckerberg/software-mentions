#!/usr/bin/env python3
"""Assigns IDs to a given input file. Creates mention2ID from scratch if it doesn't exist, or updates it if necessary,
based on input file. 

Usage:
    python assign_IDs.py

Author:
    Ana-Maria Istrate
"""

import argparse
import pandas as pd
import pickle

ROOT_DIR_INTERMEDIATE_FILES = 'data/intermediate_files/'
ROOT_DIR_INPUT_FILES = 'data/input_files/'


def assign_new_IDs(mentions, mention2ID):
  """
  Assigns new IDs to a list of software mentions; Updates mention2ID
  
  :param mentions: list of software mentions to assign IDs to
  :param mention2ID: original mention2ID
  
  :return mention2ID_updated: updated mention2ID
  """
  mention2ID_updated = mention2ID.copy()
  max_ID = len(mention2ID)
  count = 0
  for m in mentions:
    if m not in mention2ID:
      mention2ID_updated[m] = 'SM' + str(max_ID)
      max_ID += 1
      count +=1
  print('Added', count, 'mentions')
  return mention2ID_updated

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Searching Bioconductor index...')
    parser.add_argument("--input-file", help="Input file", default = 'comm.tsv.gz', required = False)
    parser.add_argument("--output-file", help="Output file", default = 'comm_IDs.tsv.gz', required = False)
    parser.add_argument("--update_mention2ID", help="True if mention2ID already exists and you want to assign new IDs", default = False, required = False, action = 'store_true')
    parser.add_argument("--mention2ID-file", help="Location for mention2ID file.", default = ROOT_DIR_INTERMEDIATE_FILES + 'mention2ID.pkl', required = False)
    parser.add_argument("--mention2ID-updated-file", help="Location for updated mention2ID, if mention2ID already exists", default = ROOT_DIR_INTERMEDIATE_FILES + 'mention2ID_updated.pkl', required = False)

    args, _ = parser.parse_known_args()
    print(args)
    
    mentions_df = pd.read_csv(ROOT_DIR_INPUT_FILES + args.input_file, sep='\t', engine='python', compression = 'gzip')
    software_mentions = mentions_df['software'].unique()
    print('- Finished reading', args.input_file)

    if args.update_mention2ID:
        mention2ID = pickle.load(open(args.mention2ID_file, 'rb'))
        mention2ID = assign_new_IDs(software_mentions, mention2ID)
        map_filename = args.mention2ID_updated_file
    else:
        IDs = range(len(software_mentions))
        mention2ID = {x:'SM' + str(y) for x, y in zip(software_mentions, IDs)}
        map_filename = args.mention2ID_file

    with open(map_filename, 'wb') as f:
        pickle.dump(mention2ID, f)
        print('- Generated mappings for', len(mention2ID), 'software mentions') 

    mentions_df['ID'] = mentions_df['software'].apply(lambda x: mention2ID[x])
    mentions_df.to_csv(ROOT_DIR_INPUT_FILES + args.output_file, sep="\t", index = False, compression = 'gzip')
