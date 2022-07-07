""" Helper functions common across files

Author:
    Ana-Maria Istrate
"""

import pandas as pd
import argparse
import requests
import re
import time
from bs4 import BeautifulSoup
from os.path import exists
from bioconductor_linker import get_bioconductor_df
from pypi_linker import get_pypi_df
from github_linker import get_github_df
from scicrunch_linker import get_scicrunch_df
from cran_linker import get_cran_df
import numpy as np

# Some repositories are queried in chunks; this is the chunk size
CHUNK_SIZE = 100

# Minimum frequency threshold for software mentions being read
FREQ_THRESHOLD = 1

# Root directory for all the data
ROOT_DIR = "data/"

# Root directory for raw metadata files 
ROOT_DIR_METADATA_RAW = 'data/metadata_files/raw/'

# Root directory for normalized metadata files 
ROOT_DIR_METADATA_NORMALIZED = 'data/metadata_files/normalized/'

# Root directory for input files 
ROOT_DIR_INPUT_FILES = 'data/input_files/'

# Root directory for output files 
ROOT_DIR_OUTPUT_FILES = 'data/output_files/'

# Root directory for intermediate files 
ROOT_DIR_INTERMEDIATE_FILES = 'data/intermediate_files/'

def load_mentions(mentions_type, file, freq_threshold, top_num_entities, ID_start = None, ID_end = None, IDs_seen_so_far = None, save_to_file = True):
  """ 
  Loads mentions file. Aggregates mentions by number of frequency

  :param mentions_type: if 'pmc-oa', assumes file is in the format of comm.tsv, or non_comm.tsv. 
  :param file: file to load software mentions from (e.g. comm.tsv, non_comm.tsv)
  :param freq_threshold: minimum frequency for mentions considered in top_mentions_df and top_software_mentions files 
  :param top_num_entities: top number of entities (in terms of frequency) to consider for the generated top_mentions_df and top_software_mentions files 
  :param ID_start: if True, only consider software mentions from this ID onward
  :param ID_end: if True, only consider software mentions up until this ID
  :param IDs_seen_so_far: added for sanity checking; list of IDs_seen_so_far, to be excluded when reading the file
  :param save_to_file: if True, saves top mentions (in terms of frequency) to file

  :return top_mentions_df: dataframe containing top software mentions (in terms of frequency) and their frequencies
          top_software_mentions: list of top software mentions (in terms of frequency)
          mentions_df: file containing extracted software mentions
          all_software_mentions: list of all software mentions extracted from the software mentions file
  """ 
  if mentions_type == 'pmc-oa':
    mentions_df = pd.read_csv(file, sep='\\t', engine='python', compression = 'gzip')
    print('- Opened the input file:', file, 'with', len(mentions_df), 'entries.')
    if IDs_seen_so_far:
      IDs_seen_so_far = list(np.load(open(ROOT_DIR + 'intermediate_files/' + IDs_seen_so_far, 'rb')))
      mentions_df = mentions_df[~(mentions_df['ID'].isin(IDs_seen_so_far))] 
    if ID_start and ID_end:
      ID_start_int = int(ID_start[2:])
      ID_end_int = int(ID_end[2:])
      ID_range = set(['SM' + str(x) for x in range(ID_start_int, ID_end_int)])
      mentions_df = mentions_df[(mentions_df['ID'].isin(ID_range))]
    mentions_grouped_df = mentions_df.groupby('software').nunique().sort_values(by = 'pmid', ascending = False).reset_index()
    software_qualifiers = ['software', 'pmcid']
  mentions_df_grouped = mentions_grouped_df[mentions_grouped_df[software_qualifiers[1]] >= freq_threshold]
  all_software_mentions = mentions_df[software_qualifiers[0]].unique()
  if top_num_entities == -1:
    top_num_entities = len(mentions_df)
  top_mentions_df = mentions_df_grouped[:top_num_entities]
  top_mentions_df = top_mentions_df.rename(columns = {'pmcid' : 'num_pmcids', 'pmid' : 'num_pmids', 'doi' : 'num_dois'})
  if save_to_file:
    top_mentions_df.to_csv(ROOT_DIR_INPUT_FILES + "top_mentions_df.csv")
  top_software_mentions = top_mentions_df[software_qualifiers[0]].unique()
  return top_mentions_df, top_software_mentions, mentions_df, all_software_mentions


def get_metadata_files(top_software_mentions, bioconductor_file, cran_file, github_file, pypi_file, scicrunch_file, save_new_file):
  """ 
    Loads normalized metadata files

    :param top_software_mentions: list of top software mentions (in terms of frequency)
    :param bioconductor_file: normalized bioconductor_file
    :param cran_file: normalized cran_file
    :param github_file: normalized github_file
    :param pypi_file: normalized pypi_file
    :param scicrunch_file: normalized scicrunch_file
    :param save_new_file: True if to generate metadata files from scratch

    :return list of metadata files
    """ 
  print(save_new_file)
  bioconductor_df = get_bioconductor_df(top_software_mentions, ROOT_DIR_METADATA_NORMALIZED + bioconductor_file, save_new_file)
  cran_df = get_cran_df(top_software_mentions, ROOT_DIR_METADATA_NORMALIZED + cran_file, save_new_file)
  github_df = get_github_df(top_software_mentions, ROOT_DIR_METADATA_NORMALIZED + github_file, save_new_file)
  pypi_df = get_pypi_df(top_software_mentions, ROOT_DIR_METADATA_NORMALIZED + pypi_file, save_new_file)
  scicrunch_df = get_scicrunch_df(top_software_mentions, ROOT_DIR_METADATA_NORMALIZED + scicrunch_file, save_new_file)
  return bioconductor_df, cran_df, github_df, pypi_df, scicrunch_df