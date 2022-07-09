"""Helper functions for linking metadata files

Author:
    Ana-Maria Istrate
"""

import pandas as pd
import time
import string
import re
import ast
import textdistance
import pickle
from utils_common import *


class DatabaseLinker:
  """ 
  Handles linking an input file to a metadata df
  """ 

  def __init__(self, input_file, output_file, min_freq, top_k, raw_filename, generate_new_file, df_type, ID_start, ID_end, IDs_seen_so_far = None):
    """
    :param input_file: input file (containing software mentions) to link
    :param output_file: output file for the normalized metadata df
    :param min_freq: only consider software mentions with this minimum frequency 
    :param top_k: only consider software mentions in top k (in terms of frequency)
    :param raw_filename: output file for the raw metadata df
    :param generate_new_file: if True, generate metadata files from scratch 
    :param df_type: type of metadata (eg cran_df, github_df, etc)
    :param ID_start: if True, only consider software mentions from this ID onward
    :param ID_end: if True, only consider software mentions up until this ID
    :param IDs_seen_so_far: added for sanity checking; list of IDs_seen_so_far, to be excluded when reading the file
    """  
    t0 = time.time()
    top_mentions_df, top_software_mentions, all_mentions_df, all_software_mentions = load_mentions('pmc-oa', file = ROOT_DIR_INPUT_FILES + input_file, 
    freq_threshold = min_freq, top_num_entities = top_k, ID_start = ID_start, ID_end = ID_end, IDs_seen_so_far = IDs_seen_so_far)
    t1 = time.time()
    print('Took', "{:.3f}".format(t1-t0), 's reading input_file')

    self.top_mentions_df = top_mentions_df
    self.top_software_mentions = top_software_mentions
    self.all_mentions_df = all_mentions_df
    self.raw_filename = raw_filename
    self.generate_new_file = generate_new_file
    self.df_type = df_type
    self.mention2ID = retrieve_ID_map()
    self.output_file = output_file

  def get_metadata_df(self, metadata_generation_fn):
    """
    Generates raw metadata df

    :param metadata_generation_fn: function to generate the raw metadata df
    """
    t0 = time.time()
    self.raw_df = metadata_generation_fn(self.top_software_mentions, ROOT_DIR_METADATA_RAW + self.raw_filename, self.generate_new_file)
    t1 = time.time()
    print('Took', "{:.3f}".format(t1-t0), 's generating', self.df_type , 'for', len(self.top_software_mentions), 'software mentions')

  def normalize_schema(self, schema_normalization_fn):
    """
    Generates normalized metadata df

    :param schema_normalization_fn: function to normalize the raw metadata df to a common schema
    """
    print('- Normalizing Schema')
    t0 = time.time()
    self.normalized_df = schema_normalization_fn(self.raw_df)
    t1 = time.time()
    print('Took', "{:.3f}".format(t1-t0), 's normalizing', self.df_type, 'schema for', len(self.top_software_mentions), 'software mentions')
    assign_IDs(self.normalized_df, self.mention2ID)

  def save_to_file(self):
    """
    Saves normalized metadata df to file
    """
    t0 = time.time()
    normalized_filename = ROOT_DIR_METADATA_NORMALIZED + self.output_file
    self.normalized_df.to_csv(normalized_filename, index = False)
    t1 = time.time()
    print('Took', "{:.3f}".format(t1-t0), 's for saving output file')
    print('- Saved', self.df_type, 'to', normalized_filename)
    print('- Here\'s how the df looks like:')
    print("=" * 30)
    print(self.normalized_df[:10])

def parse_html_string(string):
  """
  Parses an html string to retrieve the link and the name

  :param string: string to be parsed

  :return: link, name
  """
  link_tokens = string.split('href')[1].split('>')
  link = link_tokens[0][2:-1]
  if len(link_tokens) > 1:
    name = link_tokens[1][:-3]
  else:
    name = ""
  return link, name

def retrieve_ID_map():
  """
  Retrieves mention2IDmap from file. Assumes file is under ROOT_DIR_INTERMEDIATE_FILES + 'mention2ID.pkl'

  :return mention2ID: mapping from mention to ID
  """
  map_filename = ROOT_DIR_INTERMEDIATE_FILES + 'mention2ID.pkl'
  if exists(map_filename):
    mention2ID = pickle.load(open(map_filename, 'rb'))
  else:
    mention2ID = {}
    print('No ID map found. Please generate a new one by running "python generate_ID_map.py"')
  return mention2ID 

def assign_IDs(df, mention2ID, field = 'software_mention'):
  """
  Assigns IDs (in place) to a df, according to mention2ID

  :param df: df containing mentions to assign IDs to
  :param mention2ID: mapping from mention 2 ID
  :param field: field to assign an ID to in df
  """
  df['ID'] = df[field].apply(lambda x: mention2ID[x] if x == x else -1)