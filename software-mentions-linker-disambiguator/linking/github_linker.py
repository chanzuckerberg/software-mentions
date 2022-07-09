#!/usr/bin/env python3

"""Links software mentions to Github: https://github.com/

Usage:
    python github_linker.py --input-file <input_file> --generate-new

Details:
    The linker queries Github for exact matches (case dependent) of mentions in the input file.
    Extracts: ['software_mention', 'best_github_match', 'description', 'github_url',
       'license', 'exact match']
    Saves raw file under metadata_files/raw/github_raw_df.csv
    Saves normalized file (to a common schema among all metadata files) under metadata_files/normalized/github_df.csv

Author:
    Ana-Maria Istrate
"""

from utils_linker import *
from utils_common import *
import time
from schema_normalizations import *
import requests
import os

def search_github_repos(software):
  """ 
  Query the Github API for a particular software.

  :param software: software mention to query

  :return JSON response
  """ 
  url = 'https://api.github.com/search/repositories'
  params = {
      'q' : software
  }
  user = os.getenv("GITHUB_USER")
  token = os.getenv("GITHUB_TOKEN")
  authorization = "token " + token
  headers = {
      "Accept": "application/vnd.github.v3+json",
      }
  response = requests.get(url = url, params = params, headers = headers, auth = (user, token))
  while(response.status_code != 200):
    time.sleep(1)
    response = requests.get(url = url, params = params, headers = headers, auth = (user, token))
  return response.json()

def parse_json_response(response, mention):
  """ 
  Parses the JSON response received by querying the Github API for a given mention.

  :param response: JSON response retrieved by querying the Github API
  :param mention: mention that was queried through the Github API

  :return parsed json result containing the following fields: ['name', 'description', 'url', 'license']
  """ 
  try:
    num_hits= response['total_count']
    best_name_match = ""
    if num_hits > 0:
      for hit in response['items']:
        best_name_match = hit['name']
        description = hit['description']
        url = hit['url']
        license = hit['license']
        if best_name_match == mention:
          return best_name_match, description, url, license
      top_result = response['items'][0]
      return top_result['name'], top_result['description'], top_result['url'], top_result['license']
    else:
        return "no_github_entry", "no_github_entry", "no_github_entry", "no_github_entry"
  except:
    return "no_github_entry", "no_github_entry", "no_github_entry", "no_github_entry"

def get_github_df(software_mentions, filename = None, save_new = True):
  """ 
  Retrieves links and metadata from Github for a list of software mentions. 

  :param software_mentions: list of software mentions to query
  :param filename: where the raw (un-normalized) file will be saved
  :param save_new: True if to create the file from scratch
  
  :return: linked (raw) dataframe
  """ 
  filename_exists = exists(filename)
  if filename_exists and not save_new:
    print('- Retrieving saved Github file:', filename)
    return pd.read_csv(filename)
  elif not filename_exists and not save_new:
    raise Exception('- Sorry, the file', filename, 'does not exist on file. Please use --generate-new t generate first.')
  else:
    print('- Generating Github dataframe ...')
    best_name_match_arr = []
    description_arr = []
    url_arr = []
    initial_queries = []
    licenses = []
    exact_matches = []
    num_processed = 0
    num_processed_total = 0
    num_total = len(software_mentions)
    header = True
    final_dfs = []
    mode = 'w'
    for software_mention in software_mentions:
      num_processed += 1
      num_processed_total += 1
      json_response = search_github_repos(software_mention)
      best_name_match, description, url, license = parse_json_response(json_response, software_mention)
      if best_name_match == software_mention:
        exact_match = 'true'
      else:
        exact_match = 'false'
      if best_name_match != 'no_github_entry':
        best_name_match_arr.append(best_name_match)
        description_arr.append(description)
        url_arr.append(url)
        licenses.append(license)
        initial_queries.append(software_mention)
        exact_matches.append(exact_match)

      if num_processed == CHUNK_SIZE:
        df = pd.DataFrame({'software_mention' : initial_queries, 'best_github_match' : best_name_match_arr, 'description' : description_arr, 'github_url' : url_arr, 'license' : licenses, 'exact_match' : exact_matches})
        if len(df) > 0:
          df.to_csv(filename, index = False, header = header, mode = mode)
          header = False
          print('Processed', num_processed_total, '/', num_total, 'saving', len(df), 'to', filename)
          final_dfs.append(df)
          mode = 'a'
        num_processed = 0
        best_name_match_arr = []
        description_arr = []
        url_arr = []
        initial_queries = []
        licenses = []
        exact_matches = []
    df = pd.DataFrame({'software_mention' : initial_queries, 'best_github_match' : best_name_match_arr, 'description' : description_arr, 'github_url' : url_arr, 'license' : licenses, 'exact match' : exact_matches})
    if len(df) > 0:
      df.to_csv(filename, index = False, header = header, mode = mode)
      print('Processed', num_processed_total, '/', num_total, 'saving', len(df), 'to', filename)
      final_dfs.append(df)
    return pd.concat(final_dfs)

# Usage: python github_linker.py --generate-new
if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Searching Github database ...')
  parser.add_argument("--input-file", help="Input file", default = 'pmc_oa_nov_22.tsv', required = False)
  parser.add_argument("--output-file", help="Output file", default = "github_df.csv", required = False)
  parser.add_argument("--raw-filename", help="Raw Output file", default = "github_raw_df.csv", required = False)
  parser.add_argument("--min-freq", help="Minimum Mention Frequency", type = int, default = FREQ_THRESHOLD, required = False)
  parser.add_argument("--top-k", help="Retrieve top_k mentions", type = int, default = -1, required = False)
  parser.add_argument("--generate-new", help="True if generating a new file from scratch", default = False, action = 'store_true', required = False)
  parser.add_argument("--ID-start", help="ID mention start", type = str, required = False)
  parser.add_argument("--ID-end", help="ID mention end", type = str, required = False)
  parser.add_argument("--IDs-seen-so-far", help="Filter out IDs seen so far", type = str, default = 'github_IDs_queried_total.npy', required = False)
  args = parser.parse_args()
  print(args)

  github_linker = DatabaseLinker(args.input_file, args.output_file, args.min_freq, args.top_k, 
                      args.raw_filename, args.generate_new, 'github_df', args.ID_start, args.ID_end, None)
  github_linker.get_metadata_df(get_github_df)
  github_linker.normalize_schema(normalize_github_df)
  github_linker.save_to_file()
