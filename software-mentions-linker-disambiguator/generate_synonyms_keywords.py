#!/usr/bin/env python3

"""Generates synonyms for mentions in PyPI, CRAN and Bioconductor based on keywords

Usage:
    python generate_synonyms_keywords.py --input-file <input_file>

Author:
    Ana-Maria Istrate
"""

import pandas as pd
import pickle
import time
import re
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords

ROOT_DIR = "data/"

def generate_synonyms_keywords_extraction(packages, software_mentions, clue_words, pypi_cran_common, python = True):
  """
  Generates synonyms for a given list of packages using relevant keywords
  
  :param packages: packages to generate synonyms for
  :param software_mentions: the entire list of software packages to get synonyms from
  :param clue_words: keywords used to generate synonym pairs
  :param pypi_cran_common: list of mentions found in both PyPI and CRAN
  :param python: True if generating keywords for python packages
  
  :return synonyms: mapping from {package : software_mention}
  """
  # disregarding some common words from generating synonyms for, as these are likely to contain extra noise
  disregard_words = ['package', 'software', 'Library', 'studio', 'image', 'analysis', 'scripts', 'language'] + stopwords.words('english')
  synonyms = {}
  for i, mention in enumerate(software_mentions):
    if i % 10000 == 0:
      print('Processed', i, '/', len(software_mentions))
    t1 = time.time()
    if mention == mention:
      mention_tokens = mention.split()
      found_clue = False
      found_package_mention = False
      found_exact_match = False
      software = ""
      if mention in packages:
        found_exact_match = True
        software = mention
      else:
        for mention_token in mention_tokens:
          package = re.sub("[^A-z0-9 \n\-]", "", mention_token)
          if package in packages and package not in disregard_words:
            found_package_mention = True
            software = package
        
      for clue in clue_words:
        for mention_token in mention_tokens:
          if clue == mention_token:
            found_clue = True
          if python and software in pypi_cran_common and clue not in ['python', 'Python']:
            found_clue = False
          if not python and software in pypi_cran_common and clue in ['package', 'Package']:
            found_clue = False
      if found_exact_match or (found_package_mention and found_clue):
        if software in synonyms:
          synonyms[software].append(mention)
        else:
          synonyms[software] = [mention]
    t2 = time.time()
  return synonyms

def get_pypi_synonyms(pypi_df, software_mentions, pypi_cran_common):
  """
  Generates keywords-based synonyms for mentions found in the PyPI index
  
  :param pypi_df: df containing mentions found in the PyPI index
  :param software_mentions: full list of software mentions to generate synonyms from 
  :param pypi_cran_common: list of mentions found in both PyPI and CRAN
  """
  print('- Generating Pypi synonyms ... ')
  clue_words = ['python', 'Python', 'API']
  packages = pypi_df['mapped_to'].unique()
  return generate_synonyms_keywords_extraction(packages, software_mentions, clue_words, pypi_cran_common, python = True)


def get_bioconductor_synonyms(bioconductor_df, software_mentions, pypi_cran_common):
  """
  Generates keywords-based synonyms for mentions found in the Bioconductor index
  
  :param pypi_df: df containing mentions found in the Bioconductor index
  :param software_mentions: full list of software mentions to generate synonyms from 
  :param pypi_cran_common: list of mentions found in both PyPI and CRAN
  """
  print('- Generating Bioconductor synonyms ...')
  clue_words = ['R', 'r', 'package', 'Package', 'R-package', 'R-Package', 'r-package', 'bioconductor', 'Bioconductor']
  packages = bioconductor_df['mapped_to'].unique()
  return generate_synonyms_keywords_extraction(packages, software_mentions, clue_words, pypi_cran_common, python = False)


def get_cran_synonyms(cran_df, software_mentions, pypi_cran_common):
  """
  Generates keywords-based synonyms for mentions found in the CRAN index
  
  :param pypi_df: df containing mentions found in the CRAN index
  :param software_mentions: full list of software mentions to generate synonyms from 
  :param pypi_cran_common: list of mentions found in both PyPI and CRAN
  """  
  print('- Generating CRAN synonyms ...')
  clue_words = ['R', 'r', 'package', 'Package', 'R-package', 'R-Package', 'r-package']
  packages = cran_df['mapped_to'].unique()
  return generate_synonyms_keywords_extraction(packages, software_mentions, clue_words, pypi_cran_common, python = False)


if __name__ == '__main__':
  parser = argparse.ArgumentParser()

  parser.add_argument('--cran-file', type=str, default = ROOT_DIR + 'metadata_files/normalized/cran_df.csv')
  parser.add_argument('--pypi-file', type=str, default = ROOT_DIR + 'metadata_files/normalized/pypi_df.csv')
  parser.add_argument('--bioconductor-file', type=str, default = ROOT_DIR + 'metadata_files/normalized/bioconductor_df.csv')
  parser.add_argument('--mention2ID-file', type=str, default = ROOT_DIR + 'intermediate_files/mention2ID.pkl')
  parser.add_argument('--output_dir', type=str, default = ROOT_DIR)

  args, _ = parser.parse_known_args()

  cran_df = pd.read_csv(args.cran_file)
  pypi_df = pd.read_csv(args.pypi_file)
  bioconductor_df = pd.read_csv(args.bioconductor_file)
  mention2ID = pickle.load(open(args.mention2ID_file, 'rb'))
  all_software_mentions = list(mention2ID.keys())

  cran_mentions = cran_df['software_mention'].unique()
  bioconductor_mentions = bioconductor_df['software_mention'].unique()
  pypi_mentions = pypi_df['software_mention'].unique()
  pypi_cran_common = (set(cran_mentions).union(bioconductor_mentions)).intersection(pypi_mentions)

  pypi_synonyms = get_pypi_synonyms(pypi_df, all_software_mentions, pypi_cran_common)
  cran_synonyms = get_cran_synonyms(cran_df, all_software_mentions, pypi_cran_common)
  bioconductor_synonyms = get_cran_synonyms(bioconductor_df, all_software_mentions, pypi_cran_common)

  pickle.dump(pypi_synonyms, open(args.output_dir + 'pypi_synonyms.pkl', 'wb+'))
  pickle.dump(cran_synonyms, open(args.output_dir + 'cran_synonyms.pkl', 'wb+'))
  pickle.dump(bioconductor_synonyms, open(args.output_dir + 'bioconductor_synonyms.pkl', 'wb+'))