#!/usr/bin/env python3

"""Generates synonyms based on Scicrunch

Usage:
    python generate_synonyms_scicrunch.py

Author:
    Ana-Maria Istrate
"""


import pandas as pd
import pickle
import argparse
import ast
import time
import requests
from bs4 import BeautifulSoup

ROOT_DIR = "../data/"

def get_scicrunch_synonyms(scicrunch_df):
  """
  Retrieves software synonyms from an already computed Scicrunch df
  
  :param scicrunch_df: df containing mentions found in Scicrunch
  
  :return mapping from {Scicrunch mention : synonym}
  """
  software_names = scicrunch_df['software_mention'].values
  synonyms = scicrunch_df['scicrunch_synonyms'].values
  synonym_map = {}
  for software_name, synonyms in zip(software_names, synonyms):
    all_synonyms = ast.literal_eval(synonyms) + [software_name]
    synonym_map[software_name] = list(set(all_synonyms))
  return synonym_map


def get_more_scicrunch_synonyms(scicrunch_url):
  """
  Scrapes a Scicrunch page to retrieve more synonyms 
  
  :param scicrunch_url: URL of the Scicrunch page to query
  
  :return mapping from {Scicrunch mention : extra_synonym}
  """
  html = requests.get(url = scicrunch_url).text
  soup = BeautifulSoup(html, 'html.parser')
  synonyms = soup.find_all('h2', text = 'Synonym(s)')
  if len(synonyms) > 0:
    synonyms = synonyms[0].parent.find('p').text.split(', ')
  abbreviations = soup.find_all('h2', text = 'Abbreviation(s)')
  if len(abbreviations) > 0:
    abbreviations = abbreviations[0].parent.find('p').text.split(', ')
  return synonyms + abbreviations

def generate_extra_scicrunch_synonyms(scicrunch_df):
  """
  Generates extra Scicrunch synonyms by scraping Scicrunch page URLs
  
  :param scicrunch_df: df containing mentions found in Scicrunch
  
  :return mapping from {Scicrunch mention : synonym}
  """
  print('- Generating extra Scicrunch synonyms ...')
  t1 = time.time()
  scicrunch_df['extra_scicrunch_synonyms'] = scicrunch_df['package_url'].apply(lambda x: get_more_scicrunch_synonyms(x))
  software_mentions = scicrunch_df['mapped_to'].values
  extra_scicrunch_synonyms = scicrunch_df['extra_scicrunch_synonyms'].values
  synonym_dict = {x: y for x, y in zip(software_mentions, extra_scicrunch_synonyms)}
  t2 = time.time()
  print('It took', t2-t1, 'to get extra scicrunch synonyms')
  return synonym_dict

if __name__ == '__main__':
  parser = argparse.ArgumentParser()

  parser.add_argument('--scicrunch-file', type=str, default = ROOT_DIR + 'metadata_files/normalized/scicrunch_df.csv')
  parser.add_argument('--output_dir', type=str, default = ROOT_DIR + 'disambiguation_files/')

  args, _ = parser.parse_known_args()

  scicrunch_df = pd.read_csv(args.scicrunch_file)  
  scicrunch_synonyms = get_scicrunch_synonyms(scicrunch_df)
  pickle.dump(scicrunch_synonyms, open(args.output_dir + 'scicrunch_synonyms.pkl', 'wb+'))
  extra_scicrunch_synonyms = generate_extra_scicrunch_synonyms(scicrunch_df)
  
  pickle.dump(extra_scicrunch_synonyms, open(args.output_dir + 'extra_scicrunch_synonyms.pkl', 'wb+'))