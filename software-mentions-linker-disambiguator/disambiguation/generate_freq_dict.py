#!/usr/bin/env python3

"""Generates frequency dictionary based on an input file

Usage:
    python generate_freq_dict.py --input-file <input_file>

Author:
    Ana-Maria Istrate
"""

import pandas as pd
import pickle
import argparse

ROOT_DIR = "../data/"

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  
  parser.add_argument("--input-file", type=str, help="Location of software mentions input file", default = ROOT_DIR + 'input_files/comm_IDs.tsv', required = False)
  parser.add_argument('--output-file', type=str, help="Location of frequency dict output file", default = ROOT_DIR + 'intermediate_files/freq_dict.pkl')
  parser.add_argument('--descriptive-field', type=str, help="Field in input file corresponding to entity we compute frequencies for; e.g. 'software'", default = 'software')
  parser.add_argument('--freq-field', type=str, help="Field in input file corresponding to entity we compute the frequency according to; e.g. 'pmid', 'pmcid'", default = 'pmid')

  args, _ = parser.parse_known_args()
  
  mentions_df = pd.read_csv(args.input_file, sep='\t', compression = 'gzip', engine = 'python')
  mentions_grouped_df = mentions_df.groupby(args.descriptive_field).nunique().sort_values(by = args.freq_field, ascending = False).reset_index()
  
  mentions = mentions_grouped_df [args.descriptive_field].values
  freqs = mentions_grouped_df[args.freq_field].values
  freq_dict = {m : f for m, f in zip(mentions, freqs)}
  sorted_dict = dict(sorted(freq_dict.items(), key=lambda item: -item[1]))
  
  pickle.dump(sorted_dict, open(args.output_file, 'wb+'))