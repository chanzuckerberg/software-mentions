#!/usr/bin/env python3

"""Clusters plain text software mentions using DBSCAN or HDBSCAN

Usage:
    python clustering.py --input-file <input_file>

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
from sklearn.cluster import DBSCAN
from scipy.sparse import csr_matrix
import numpy as np
from sklearn.decomposition import TruncatedSVD
import hdbscan
from scipy.sparse.csgraph import connected_components
import argparse
import time

ROOT_DIR = "data/"

def get_clusters(df):
  """
  Parses clusters and confidence scores from a given df containing pairs of synonyms and confidence scores

  :param df: dataframe containing pairs of synonyms and similarity confidence scores

  :return clusters: mapping from software mention to a dictionary containing {synonym : synonym_confidence}
  """
  clusters = {}
  for software_mention, synonyms, synonyms_confs in zip(df['software_mention'].values, df['synonym'].values, df['synonym_conf'].values):
    software_mention_mapping = {}
    for synonym, synonym_conf in zip(synonyms, synonyms_confs):
      if synonym in software_mention_mapping:
        if synonym_conf > software_mention_mapping[synonym]:
          software_mention_mapping[synonym] = synonym_conf
      else:
        software_mention_mapping[synonym] = synonym_conf
    clusters[software_mention] = software_mention_mapping
  return clusters

def get_sim_matrix(words, clusters):
  """
  Builds a similarity matrix for a given array of words, based on similarity scores found in initial_clusters 

  :param words: array of words to build similarity matrix for
  :param initial_clusters: mapping from software mention to a dictionary containing {synonym : synonym_confidence}

  :return m: similarity_matrix
  """
  words_unique = set(words)
  num_words = len(words)
  row = []
  col = []
  data = []

  word2idx = {word : idx for idx, word in enumerate(words)}
  keys = clusters.keys()
  seen = set()
  for i, w1 in enumerate(words):
    row.append(i)
    col.append(i)
    data.append(1.0)
    if w1 in clusters:
      synonyms = clusters[w1]
      
      for w2, w1w2_conf in synonyms.items():
        if w1 != w2 and w2 in words_unique and (w1, w2) not in seen:
          seen.add((w1, w2))
          w2_idx = word2idx[w2]
          row.extend([i])
          col.extend([w2_idx])
          d = w1w2_conf
          data.extend([d])
          
        if w1 != w2 and w2 in words_unique and (w2, w1) not in seen:
          seen.add((w2, w1))
          row.extend([w2_idx])
          col.extend([i])
          data.extend([d])
  m = csr_matrix((np.array(data), (np.array(row), np.array(col))))
  return m

def get_connected_components(n_components, labels, words):
  """
  Retrieves connected components and associated distance matrices for a given array of words 

  :param n_components: total number of connected components (cc)
  :param labels: array containing cc mappings; e.g. words[i] is part of the labels[i]'th cc
  :param words: array of words to get connected components for

  :return components: mapping from {n_component: words}
  :return matrices: mapping from {n_component: distance_matrix corresponding to words}
  """
  components = {}
  matrices = {}
  labels = np.array(labels)
  words = np.array(words)
  for n_component in range(n_components):
    indices = np.where(labels == n_component)[0]
    cc = words[indices]
    components[n_component] = cc
    matrix = get_sim_matrix(cc, clusters)
    matrices[n_component] =   1 - matrix.toarray()
  return components, matrices

def get_predictions_cc_n(matrices, components, eps, n, verbose, min_samples, freq_dict, metric = 'precomputed', threshold = 1.0, hdbscan_flag = True):
  """
  Clusters given distance matrices using DBSCAN or HDBSCAN

  :param matrices: distance matrices
  :param components: connected components
  :param eps: eps value for DBSCAN
  :param n: the index of the component to get predictions for
  :param min_samples: min_samples value for DBSCAN; also used as min_cluster_size for HDBSCAN
  :param freq_dict: mapping from mention to frequency; used to decide cluster name
  :param metric: metric used for DBSCAN/HDBSCAN (usually 'precomputed', since we already have a distance matrix)
  :param threshold: min probability to consider samples being part of the same cluster for HDBSCAN
  :param hdbscan_flag: True if to use hdbscan

  More information about choosing parameters for DBSCAN here: https://scikit-learn.org/stable/modules/generated/sklearn.cluster.DBSCAN.html

  :return pred_clusters: mapping from {cluster_name: cluster points}
  :return labels: cluster labels for words in the n'th component
  """
  t1 = time.time()
  sim_matrix = matrices[n]

  if hdbscan_flag:
    clusterer = hdbscan.HDBSCAN(min_cluster_size = min_samples,  min_samples = 1, metric = metric).fit(sim_matrix) 
    labels = clusterer.labels_
    probabilities = clusterer.probabilities_
  else:
    dbscan = DBSCAN(min_samples=min_samples, eps = eps, metric = metric)
    labels = dbscan.fit_predict(sim_matrix)
    probabilities = []
  pred_clusters = print_clusters(components[n], labels, probabilities, freq_dict, verbose, threshold)
  return pred_clusters, labels

def get_main_cluster(sorted_dict, samples):
  """
  Get main cluster for a list of samples. Chooses the sample with the highest frequency in sorted_dict

  :param sorted_dict: mapping from mention to frequency
  :param samples: samples to get the main cluster for

  :return best_mention: cluster name
  """	
  best_mention = ""
  best_freq = 0
  links = []
  for sample in samples:
    if sample not in sorted_dict:
      continue
    freq = sorted_dict[sample]
    if freq > best_freq:
      best_mention = sample
      best_freq = freq
  return best_mention

def print_clusters(words, labels, probabilities, sorted_dict, verbose = True, threshold = 1.0):
  """
  Maps words to predicted clusters 

  :param words: words to map
  :param labels: array containing word mappings; e.g. words[i] is part of cluster labels[i]
  :param probabilities: probability that each word belongs to each label
  :param sorted_dict: mapping from mention to frequency
  :param verbose: if True, print clusters
  :param threshold: min probability to consider a word part of a cluster

  :return clusters: 
  """ 
  clusters = {}
  for c in set(labels):
    c_words = np.array(words)[np.where(np.array(labels) == c)[0]]
    if c != -1: 
      if len(probabilities) > 0:
        c_probs = np.array(probabilities)[np.where(np.array(labels) == c)[0]]
        c_new_words = []
        for x, y in zip(c_words, c_probs):
          if y >= threshold:
            c_new_words.append(x)
        cluster_name = get_main_cluster(sorted_dict, c_new_words)
      else:
        cluster_name = get_main_cluster(sorted_dict, c_words)
      clusters[cluster_name] = c_words
      if verbose:
        print('Cluster', c, cluster_name)
        print('*' * 10)
        print(c_words)
        print('\n')
  return clusters

def find_cluster(x, clusters):
  """
  Returns the cluster for a given software mention x

  :param x: mention to get the cluster for
  :param clusters: predicted clusters

  :return k: cluster for mention x
  """
  for k, v in clusters.items():
    v_set = set(v)
    if x in v_set:
      return k
    
def get_predicted_clusters(matrices, components, arr, dbscan_eps, dbscan_min_samples, freq_dict):
  """
  Gets predicted clusters for an array of given distance matrices corresponding to components

  :param matrices: array of distance matrices
  :param components: connected components
  :param arr: components in this array will be clustered further using DBSCAN/HDBSCAN
  :param dbscan_eps: epsilon value to use for DBSCAN/HDBSCAN clustering
  :param dbscan_min_samples: min_samples value for DBSCAN; also used as min_cluster_size for HDBSCAN
  :param freq_dict: mapping from mention to frequency

  :return all_clusters: final predicted clusters, mapping from {cluster: cluster_entries}
  """
  metric = 'precomputed'
  num_components = len(components)
  for n in range(num_components):
    if n in arr:
      pred_c_n, labels = get_predictions_cc_n(matrices, components, dbscan_eps, n, False, dbscan_min_samples, freq_dict, metric, 0.3, hdbscan_flag = False)
      if len(set(labels)) > 1:
        silhouette_threshold = metrics.silhouette_score(matrices[n], labels, metric = metric)
        m[n] = (silhouette_threshold, pred_c_n)
        print("Component", n, "Silhouette Coefficient: %0.3f" % silhouette_threshold)
        if silhouette_threshold > threshold:
          for k, v in pred_c_n.items():
             all_clusters[k] = v
        else:
            cluster_name = get_main_cluster(freq_dict, components[n])
            all_clusters[cluster_name] = components[n]
    else:
      cluster_name = get_main_cluster(freq_dict, components[n])
      all_clusters[cluster_name] = components[n]
  return all_clusters
 
def get_mention2cluster(all_clusters):
  """
  Returns mention2cluster mapping

  :param all_clusters: predicted clusters, mapping from {cluster: cluster_entries}

  :return mention2cluster: mapping from {mention: cluster_name}
  """
  mention2cluster = {}
  for k, v in all_clusters.items():
    for v1 in v: 
      mention2cluster[v1] = k
  return mention2cluster

if __name__ == '__main__':
  parser = argparse.ArgumentParser()

  parser.add_argument('--synonyms-file', type=str, default = ROOT_DIR + 'synonyms.csv')
  parser.add_argument('--disambiguated-file', type=str, default = ROOT_DIR + 'synonyms_disambiguated.tsv')
  parser.add_argument('--freq_dict', type=str, default = ROOT_DIR + 'freq_dict.pkl')
  parser.add_argument('--input_file', type=str, default = ROOT_DIR + 'comm_IDs.tsv')
  parser.add_argument('--output_file', type=str, default = ROOT_DIR + 'comm_IDs_disambiguated.tsv')

  args, _ = parser.parse_known_args()

  synonyms_df = pd.read_csv(args.synonyms_file)
  freq_dict = pickle.load(open(args.freq_dict, 'rb'))
  # sample mentions for sanity checking
  sample_mentions = ['LIMMA', 'limma', 'Limma R package', 'Python', 'SciPy', 'scipy', 'stats', 'scikit-learn', 'sklearn', 'matplotlib', 'plotly', 'pandas', 'numpy', 'IMAGER', 'ImageJ', 'Image J', 'NIH ImageJ', 'ImageJ2', 'Fiji', 'NeuronJ', 'NeuronJ ImageJ', 'neuron', 'NEURON', 'BoneJ', 'SimPlot', 'Jupyter Notebook', 'Jupyter', 'iPython', 'iPython Notebook', 'ArcGIS', 'Stata', 'SAS', 'Affymetrix', 'Ringo', 'edgeR', 'Keras', 'CNN', 'AlexNet', 'ResNet', 'pytorch', 'DBSCAN', 'SPSS', 'SPSS®', 'BLAST', 'R Core Team', 'DAVID', 'GEPIA', 'GSEA', 'ggplot2', 'gplots', 'SVA', 'sva', 'SHELXTL', 'Stata', 'SAS', 'seqc', 'MAFFT', 'ClustalX', 'affy', 'glmer', 'Berkeley Madonna', 'Basic Local Alignment Search Tool (BLAST)', '(Basic Local Alignment Search Tool', 'MATLAB', 'DESeq2', 'codelink', 'beadarray', 'Lumi', 'car', 'Minimac', 'minimap2', 'Minim', 'statsmodels', 'seaborn', 'clusterProfiler', 'Chicago', 'ReactomeGSA', 'ReactomePA', 'Analysis', 'FASTX-Toolkit', 'FASTQC', 'GSVA', 'GOseq R package']

  dbscan_eps = 0.1
  dbscan_min_samples = 30

  synonyms_df_aggregated = synonyms_df.groupby(['ID', 'software_mention']).agg(list).reset_index()
  clusters = get_clusters(synonyms_df_aggregated)
  clusters_arr = list(clusters.keys())
  print('There are', len(clusters_arr), 'clusters')

  words = []
  for w in clusters:
    words.extend(set([w] + list(clusters[w].keys())))
  words = list(set(words))
  sim_matrix = get_sim_matrix(words, clusters)

  n_components, labels = connected_components(csgraph=sim_matrix, directed=False, return_labels=True)
  components, matrices = get_connected_components(n_components, labels, words)
  num_components = len(components)
  print('There are', num_components, 'connected components!')

  comp2len = {}
  for comp_id, arr in components.items():
    comp2len[(comp_id, get_main_cluster(freq_dict, arr))] = len(arr)

  comp2entries = {}
  for comp_id, arr in components.items():
    comp2entries[(comp_id, get_main_cluster(freq_dict, arr))] = arr

  sorted_comp2len = sorted(comp2len.items(), key = lambda x: -x[1])

  print('Here are the top 10 connected components:')
  print(sorted_comp2len[:10])

  all_clusters = {}
  threshold = 0.0
  m = {}
  # we are only using DBSCAN to cluster the first connected component
  arr = [sorted_comp2len[0][0][0]]
  predicted_clusters = get_predicted_clusters(matrices, components, arr, dbscan_eps, dbscan_min_samples, freq_dict)
  mention2cluster = get_mention2cluster(predicted_clusters)

  synonyms_df['mapped_to'] = synonyms_df['software_mention'].apply(lambda x: mention2cluster[x] if x in mention2cluster else x)
  synonyms_df.to_csv(args.disambiguated_file, index = False, sep = '\t')
  
  mentions_df = pd.read_csv(args.input_file, sep = '\\t', engine = 'python', compression = 'gzip')
  mentions_df['mapped_to'] = synonyms_df['software_mention'].apply(lambda x: mention2cluster[x] if x in mention2cluster else x)
  mentions_df.to_csv(args.output_file, index = False, sep = '\t')

  print('Here are some examples of disambiguated mentions: ')
  for x in sample_mentions:
    x_component = find_cluster(x, all_clusters)
    print('Mention:', x, 'Predicted cluster:', x_component)