#!/usr/bin/env python3

"""Links software mentions to the SciCrunch database: https://scicrunch.org

Usage:
    python scicrunch_linker.py --input-file <input_file> --generate-new

Details:
    The linker queries the SciCrunch database for exact matches (case dependent) of mentions in the input file.
    Parses individual SciCrunch project pages to extract: ['software_name', 'scicrunch_synonyms', 'Resource Name',
       'Resource Name Link', 'Resource Type', 'Description', 'Keywords',
       'Resource ID', 'Resource ID Link', 'Proper Citation',
       'Parent Organization', 'Parent Organization Link', 'Related Condition',
       'Funding Agency', 'Relation', 'Reference', 'Website Status',
       'Alternate IDs', 'Alternate URLs', 'Old URLs', 'Reference Link']
    Saves raw file under metadata_files/raw/scicrunch_raw_df.csv
    Saves normalized file (to a common schema among all metadata files) under metadata_files/normalized/scicrunch_df.csv

Author:
    Ana-Maria Istrate
"""

from utils_linker import *
from utils_common import *
import xml.etree.ElementTree as ET
from schema_normalizations import *
import os

def scicrunch_endpoint(url_stem, query):
  """ 
  Query SciCrunch API endpoint for a particular query.

  :param url_stem: the SciCrunch URL endpoint to access
  :param query: query added to the url_stem

  :return response
  """ 
  url = url_stem + query
  access_token = os.getenv("SCICRUNCH_TOKEN") 
  params = {
    'limit' : 10,
    'searchSynonyms' : 'true',
    'searchAbbreviations' : 'true',
    'searchAcronyms' : 'true',
    'key' : access_token
  }
  response = requests.get(url = url, params = params)
  if response.status_code == 200:
    return response.content
  else:
    return None

def query_scicrunch_individual_mention(query):
  """ 
  Query the SciCrunch API endpoint for a particular query and parse the output to extract metadata.

  :param query: query (e.g. software_mention)

  :return list of matches, where each match is a dict containing metadata about a hit for the query using the SciCrunch API
  """ 
  t1 = time.time()
  endpoint = scicrunch_endpoint("https://scicrunch.org/api/1/dataservices/federation/data/nlx_144509-1?q=", query)
  t2 = time.time()
  if endpoint != None:
    try:
      root = ET.fromstring(endpoint)
      clauses = root.findall('query')[0].findall('clauses')[0].findall('clauses')[0].findall('expansion')[0].findall('expansion')
      synonyms_lower = [clause.text.lower() for clause in clauses] + [query.lower()]
      results = root.findall('result')[0].findall('results')[0].findall('row')
      matches = []
      for row in results:
        keys = {'software_name' : query, 'scicrunch_synonyms' : synonyms_lower}
        data = row.findall('data')
        found_datapt = False
        for data_pt in data:
          name = data_pt.find('name').text
          value = data_pt.find('value').text
          if name in ['Resource Name', 'Resource ID', 'Parent Organization', 'Reference'] and value and 'href' in value:
            link, link_name = parse_html_string(value)
            keys[name] = link_name
            keys[name + ' Link'] = link
            if link_name.lower() in synonyms_lower:
              found_datapt = True
          else:
            keys[name] = value
        if found_datapt:
          matches.append(keys)
      return matches
    except:
      return []
  else:
    return []

def get_scicrunch_df(queries, filename = None, save_new = True):
  """ 
  Retrieves links and metadata from the SciCrunch repository for a list of software mentions. 

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
    print('- Generating scicrunch dataframe ... ')
    all_matches = []
    num_processed = 0
    num_processed_total = 0
    num_total = len(queries)
    header = True
    mode = 'w'
    final_dfs = []
    for i, query in enumerate(queries):
      num_processed += 1
      num_processed_total += 1
      matches = query_scicrunch_individual_mention(query)
      all_matches.extend(matches)
      if num_processed == CHUNK_SIZE:
        df = pd.DataFrame(columns=all_matches[0].keys())
        for match in all_matches:
          df = df.append(match, ignore_index=True)
        if len(df) > 0:
          df.to_csv(filename, index = False, header = header, mode = mode)
          header = False
          print('Processed', num_processed_total, '/', num_total, 'saving', len(df), 'to', filename)
          final_dfs.append(df)
          mode = 'a'
        num_processed = 0
        all_matches = []
    if len(all_matches) > 0:
      df = pd.DataFrame(columns=all_matches[0].keys())
      for match in all_matches:
        df = df.append(match, ignore_index=True)
      df.to_csv(filename, index = False, header = header, mode = mode)
      print('Processed', num_processed_total, '/', num_total, 'saving', len(df), 'to', filename)
      final_dfs.append(df)
    return pd.concat(final_dfs)

# Usage: python scicrunch_linker.py --generate-new
if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Searching Scicrunch database ...')
  parser.add_argument("--input-file", help="Input file", default = 'pmc_oa_nov_22.tsv', required = False)
  parser.add_argument("--output-file", help="Output file", default = "scicrunch_df.csv", required = False)
  parser.add_argument("--raw-filename", help="Raw Output file", default = "scicrunch_raw_df.csv", required = False)
  parser.add_argument("--min-freq", help="Minimum Mention Frequency", type = int, default = FREQ_THRESHOLD, required = False)
  parser.add_argument("--top-k", help="Retrieve top_k mentions", type = int, default = -1, required = False)
  parser.add_argument("--generate-new", help="True if generating a new file from scratch", default = False, action = 'store_true', required = False)
  parser.add_argument("--ID-start", help="ID mention start", type = str, required = False)
  parser.add_argument("--ID-end", help="ID mention end", type = str, required = False)
  args = parser.parse_args()
  print(args)

  scicrunch_linker = DatabaseLinker(args.input_file, args.output_file, args.min_freq, args.top_k, 
                      args.raw_filename, args.generate_new, 'scicrunch_df', args.ID_start, args.ID_end)
  scicrunch_linker.get_metadata_df(get_scicrunch_df)
  print(scicrunch_linker.raw_df.columns)
  scicrunch_linker.normalize_schema(normalize_scicrunch_df)
  scicrunch_linker.save_to_file()
