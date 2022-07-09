#!/usr/bin/env python3

"""Generates a master metadata file by aggregating individual normalized metadata files

Usage:
    python generate_metadata_file.py --output-file <output_file>

Details:
    Retrieves normalized metadata files from a given root directory, does light processing, and outputs master metadata file to output directory. 

Author:
    Ana-Maria Istrate
"""

import os
import pandas as pd
import argparse
from utils_common import *

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Linking normalized metadata files.')
	parser.add_argument("--root_dir", help="Root directory where metadata files are located", default = ROOT_DIR_METADATA_NORMALIZED, required = False)
	parser.add_argument("--output-file", help="Location of output file", default = ROOT_DIR_OUTPUT_FILES + 'metadata.csv', required = False)
	args = parser.parse_args()

	root_dir = args.root_dir
	output_file = args.output_file

	metadata_dfs = []
	for filename in os.listdir(root_dir):
		f = os.path.join(root_dir, filename)
		if os.path.isfile(f):
			print('Concatenating file', f)
			df = pd.read_csv(f)
			metadata_dfs.append(df)

	metadata_df = pd.concat(metadata_dfs)
	metadata_df = metadata_df.drop_duplicates()
	metadata_df = metadata_df[((metadata_df['source'] == 'Github API') & (metadata_df['exact_match'] == True)) | (metadata_df['source'] != 'Github API')]
	metadata_df.to_csv(output_file, sep = '\t', index = False)

