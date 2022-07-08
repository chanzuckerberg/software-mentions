#!/usr/bin/env python3

"""Combines all synonym files (keywords-generated, scicrunch-retrieved and string similarity generated).
Does some cleaning and post-processing.
Output of this file gets fed into the clustering algorithm.
Assumes the following files are already computed:
- 'pypi_synonyms.pkl'
- 'cran_synonyms.pkl'
- 'bioconductor_synonyms.pkl'
- 'scicrunch_synonyms.pkl'
- 'extra_scicrunch_synonyms.pkl'
- 'string_similarity_dict.pkl";
- 'mention2ID.pkl'

Usage:
    python combine_all_synonyms.py 

Details:
    Builds a similarity matrix based on synonym data.
    Finds connected components in similarity matrix.
    Uses DBSCAN to cluster a number of connected components into smaller clusters.
    Assigns the cluster name to the mention with the highest frequency according to a given freq_dict.

Author:
    Ana-Maria Istrate
"""


import pandas as pd
import pickle
import string
import time
import ast
import argparse

ROOT_DIR = "/dbfs/FileStore/meta/user/aistrate/software_entity_linking/"

def assign_confidences(df, synonym_map, synonym_source, conf = 1, use_confs = None):
  """
  Assigns confidences in a given df to synonyms in synonym_map coming from a synonym_source
  
  :param df: df to assing confidences to
  :param synonym_map: map from {software : (synonym, synonym_conf)}
  :param synonym_source: source of synonyms (e.g. pypi, CRAN, )
  :param conf: confidence to assign to synonyms
  :param use_confs: True if confidences are already computed (eg from string similarity)
  
  :return df_conf: df augmented with synonym confidences
  """
  df_conf = df.copy()
  if use_confs:
    df_conf['synonyms'] = df_conf['software_mention'].apply(lambda x: synonym_map[x][0] if ((x in synonym_map) and len(synonym_map[x][0]) > 1) else None)
    df_conf['synonyms_confs'] = df_conf['software_mention'].apply(lambda x: synonym_map[x][1] if ((x in synonym_map) and len(synonym_map[x][1]) > 1) else None)
  else:
    df_conf['synonyms'] = df_conf['software_mention'].apply(lambda x: synonym_map[x] if ((x in synonym_map) and len(synonym_map[x]) > 1) else None)
    df_conf['synonyms_confs'] = df_conf['synonyms'].apply(lambda x: [conf] * len(x) if x else None)
  df_conf['synonym_source'] = synonym_source
  df_conf = df_conf.dropna()
  return df_conf

def assert_equal_str_digits(x):
  """
  Asserts whether the 'software_mention' and 'synonym' fields in a given dataframe row are equal when stripped of digits and white space
  
  :param x: row in a dataframe; contains a 'software_mention' and a 'synonym' field
  
  :return True: if 'software_mention' and 'synonym' fields are equal given constraints
  """
  s1 = x['software_mention'].translate(str.maketrans('', '', string.digits)).replace(" ", "")
  s2 = x['synonym'].translate(str.maketrans('', '', string.digits)).replace(" ", "")
  return s1 == s2

def assert_equal_str_punctuation(x):
  """
  Asserts whether the 'software_mention' and 'synonym' fields in a given dataframe row are equal when stripped of punctuation and white space
  
  :param x: row in a dataframe; contains a 'software_mention' and a 'synonym' field
  
  :return True: if 'software_mention' and 'synonym' fields are equal given constraints
  """
  s1 = x['software_mention'].translate(str.maketrans('', '', string.punctuation)).replace(" ", "")
  s2 = x['synonym'].translate(str.maketrans('', '', string.punctuation)).replace(" ", "")
  return s1 == s2

def assert_equal_str_copyright(x):
  """
  Asserts whether the 'software_mention' and 'synonym' fields in a given dataframe row are equal when stripped of copyright chars and white space
  
  :param x: row in a dataframe; contains a 'software_mention' and a 'synonym' field
  
  :return True: if 'software_mention' and 'synonym' fields are equal given constraints
  """
  software_mention = x['software_mention']
  s11 = software_mention.translate(str.maketrans('', '', u'\N{COPYRIGHT SIGN}')).replace(" ", "")
  s12 = software_mention.translate(str.maketrans('', '', u'\N{TRADE MARK SIGN}')).replace(" ", "")
  s13 = software_mention.translate(str.maketrans('', '', u'\N{REGISTERED SIGN}')).replace(" ", "")
  
  synonym = x['synonym']
  s21 = synonym.translate(str.maketrans('', '', u'\N{COPYRIGHT SIGN}')).replace(" ", "")
  s22 = synonym.translate(str.maketrans('', '', u'\N{TRADE MARK SIGN}')).replace(" ", "")
  s23 = synonym.translate(str.maketrans('', '', u'\N{REGISTERED SIGN}')).replace(" ", "")
  return (s11 == s21) or (s12 == s22) or (s13 == s23)

def all_chars(s):
  """
  Returns True if a string contains all chars
  
  :param s: string to check
  """
  for ch in s:
    if not ch.isalpha():
      return False
  return True

def starts_with(x):
  """
  Returns True if the 'software_mention' field is a substring of the 'synonym' field in a give dataframe row
  
  :param s: dataframe row to check
  """
  s1 = x['software_mention']
  s2 = x['synonym']
  return s2.startswith(s1)


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  
  parser.add_argument("--pypi-synonyms-file", type=str, help="Location of pypi synonyms file", default = ROOT_DIR + 'pypi_synonyms.pkl', required = False)
  parser.add_argument("--cran-synonyms-file", help="Location of CRAN synonyms file", default = ROOT_DIR + 'cran_synonyms.pkl', required = False)
  parser.add_argument("--bioconductor-synonyms-file", help="Location of Bioconductor synonyms file", default = ROOT_DIR + 'bioconductor_synonyms.pkl', required = False)
  parser.add_argument("--scicrunch-synonyms-file", help="Location of Scicrunch synonyms file", default = ROOT_DIR + 'scicrunch_synonyms.pkl', required = False)
  parser.add_argument("--extra-scicrunch-synonyms-file", help="Location of extra Scicrunch synonyms file", default = ROOT_DIR + 'extra_scicrunch_synonyms.pkl', required = False)
  parser.add_argument("--string-sim-synonyms-file", help="Location of string similarity synonyms file", default = ROOT_DIR + 'string_similarity_dict.pkl', required = False)
  parser.add_argument("--conf_threshold", help="Minium confidence threshold for synonyms in the final file", default = 0.97, required = False)
  parser.add_argument('--mention2ID-file', type=str, default = ROOT_DIR + 'mention2ID.pkl')
  parser.add_argument('--output-file', type=str, default = ROOT_DIR + 'synonyms.csv')

  args, _ = parser.parse_known_args()

  pypi_synonyms = pickle.load(open(args.pypi_synonyms_file, 'rb'))
  cran_synonyms = pickle.load(open(args.cran_synonyms_file, 'rb'))
  bioconductor_synonyms = pickle.load(open(args.bioconductor_synonyms_file, 'rb'))
  scicrunch_synonyms = pickle.load(open(args.scicrunch_synonyms_file, 'rb'))
  extra_scicrunch_synonyms = pickle.load(open(args.extra_scicrunch_synonyms_file, 'rb'))
  string_similarity_dict = pickle.load(open(args.string_sim_synonyms_file, 'rb+'))

  mention2ID = pickle.load(open(args.mention2ID_file, 'rb'))
  all_software_mentions = list(mention2ID.keys())
  all_mentions_df = pd.DataFrame({'software_mention' : all_software_mentions})
  all_mentions_df['ID'] = all_mentions_df['software_mention'].apply(lambda x: mention2ID[x])
  all_mentions_df = all_mentions_df[['ID', 'software_mention']]

  pypi_synonyms_df = assign_confidences(all_mentions_df, pypi_synonyms, 'pypi', 0.99)
  cran_synonyms_df = assign_confidences(all_mentions_df, cran_synonyms, 'CRAN', 0.99)
  bioconductor_synonyms_df = assign_confidences(all_mentions_df, bioconductor_synonyms, 'Bioconductor', 0.99)
  scicrunch_synonyms_df = assign_confidences(all_mentions_df, scicrunch_synonyms, 'Scicrunch')
  scicrunch_extra_synonyms_df = assign_confidences(all_mentions_df, extra_scicrunch_synonyms, 'Scicrunch_page_query')
  string_sim_synonyms_df = assign_confidences(all_mentions_df, string_similarity_dict, 'string_similarity', 1, True)

  synonyms_df_raw = pd.concat([scicrunch_synonyms_df, scicrunch_extra_synonyms_df, pypi_synonyms_df, cran_synonyms_df, bioconductor_synonyms_df, string_sim_synonyms_df], axis = 0).sort_values(by = 'ID', ascending = True)
  synonyms_df_raw.to_csv(ROOT_DIR + "synonyms_raw.csv")

  # Clean up synonym files
  common_words_to_remove = ['script', 'Interface', 'R package', 'r package', 'interface', 'R packag', 'r packag', 'R packages', 'Bioconductor package', 'analysis', 'BioConductor', 'Bioconductor', 'Bioconductor package R', 'Bioconductor/R', 'R bioconductor', 'R/Bioconductor', 'package', 'Bioconductor R package', 'Bioconductor R-package', 'R package)', 'R Bioconductor package', 'bioconductor', 'Bioconductor', 'R/Bioconductor package', 'Library', 'Biothings API and Explorer', 'Python', 'python', 'python programming language', 'Python package', 'Python Package', 'packages', 'libraries']

  synonyms_df = synonyms_df_raw.set_index(['ID', 'software_mention', 'synonym_source']).apply(pd.Series.explode).reset_index()
  synonyms_df = synonyms_df.rename(columns = {'synonyms' : 'synonym', 'synonyms_confs' : 'synonym_conf'})
  synonyms_df = synonyms_df[['ID', 'software_mention', 'synonym', 'synonym_conf', 'synonym_source']]

  # Remove common words
  synonyms_df = synonyms_df[(~synonyms_df['software_mention'].isin(common_words_to_remove)) & (~synonyms_df['synonym'].isin(common_words_to_remove))]
  synonyms_df['synonym_ID'] = synonyms_df['synonym'].apply(lambda x: mention2ID[x] if x in mention2ID else -1)

  # Some cleanup of string similarity confidences; reduces the noise for clustering
  synonyms_df = synonyms_df.drop(synonyms_df[synonyms_df['software_mention'] == synonyms_df['synonym']].index)
  synonyms_df = synonyms_df[~((synonyms_df['synonym'].str.startswith('Bioconductor package')) & (synonyms_df['synonym_conf'] < 0.935))]
  synonyms_df = synonyms_df[~((synonyms_df['synonym'].str.startswith('R Bioconductor package')) & (synonyms_df['synonym_conf'] < 0.935))]
  synonyms_df = synonyms_df[~((synonyms_df['synonym'].str.startswith('R/BioConductor package')) & (synonyms_df['synonym_conf'] < 0.94))]
  synonyms_df = synonyms_df[~((synonyms_df['synonym'].str.startswith('R package')) & (synonyms_df['synonym_conf'] < 0.94))]
  synonyms_df = synonyms_df[~((synonyms_df['synonym'].str.startswith('R-package')) & (synonyms_df['synonym_conf'] < 0.94))]

  # Assign confidence of one for pairs of synonyms that are equal when stripped of digits 
  synonyms_df['syn_substring_digits'] = synonyms_df.apply(assert_equal_str_digits, axis = 1)
  synonyms_df.loc[synonyms_df['syn_substring_digits'], 'synonym_conf'] = 1

  # Assign confidence of one for pairs of synonyms that are equal when stripped of punctuation
  synonyms_df['syn_substring_punctuation'] = synonyms_df.apply(assert_equal_str_punctuation, axis = 1)
  synonyms_df.loc[synonyms_df['syn_substring_punctuation'], 'synonym_conf'] = 1

  # Assign confidence of one for pairs of synonyms that are equal when stripped of copyright chars
  synonyms_df['syn_substring_copyright'] = synonyms_df.apply(assert_equal_str_copyright, axis = 1)
  synonyms_df.loc[synonyms_df['syn_substring_copyright'], 'synonym_conf'] = 1

  # Assign confidence of one for pairs of synonyms that have more than one token and are equal (case insensitive)
  synonyms_df['lowercase_equal'] = synonyms_df['software_mention'].str.lower() == synonyms_df['synonym'].str.lower()
  synonyms_df['num_tokens'] = synonyms_df['software_mention'].apply(lambda x: len(x.split()))
  synonyms_df.loc[(synonyms_df['lowercase_equal']) & (synonyms_df['num_tokens'] > 1), 'synonym_conf'] = 1.0

  # Remove pairs of synonyms that are all chars and are not equal when stripped of digits, punctuation or copyright chars 
  synonyms_df['is_chars'] = synonyms_df['software_mention'].apply(all_chars)
  synonyms_df['is_chars_syn'] = synonyms_df['synonym'].apply(all_chars)
  synonyms_df = synonyms_df[~(synonyms_df['is_chars'] & synonyms_df['is_chars_syn'] & (~synonyms_df['syn_substring_digits'] & ~synonyms_df['syn_substring_punctuation'] & ~synonyms_df['syn_substring_punctuation']))]

  synonyms_df['is_substr'] = synonyms_df.apply(starts_with, axis = 1)
  
  # Only consider synonyms with a high confidence
  synonyms_df_high_conf = synonyms_df[(synonyms_df['synonym_conf'] >= args.conf_threshold)]
  
  # Remove synonyms that weren't in the original set to begin with (eg these are likely coming from Scicrunch)
  synonyms_df = synonyms_df_high_conf[synonyms_df_high_conf['synonym_ID'] != -1]
  
  # Save to file
  synonyms_df.to_csv(args.output_file)