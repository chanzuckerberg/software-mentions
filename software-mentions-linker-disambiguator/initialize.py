#!/usr/bin/env python3

"""Creates a couple of directories that are assumed to exist by the linking/disambiguation algorithms in the 'data' directory. 

Usage:
    python intialize.py

Author:
    Ana-Maria Istrate
"""

import os

ROOT_DIR_METADATA_RAW = 'metadata_files/raw/'
ROOT_DIR_METADATA_NORMALIZED = 'metadata_files/normalized/'
ROOT_DIR_INPUT_FILES = 'input_files/'
ROOT_DIR_OUTPUT_FILES = 'output_files/'
ROOT_DIR_INTERMEDIATE_FILES = 'intermediate_files/'
ROOT_DIR_DISAMBIGUATION = 'disambiguated_files/'
ROOT_DIR_CURATION = 'curation/'


def create_dir(parent_dir, filename_path):
	path = os.path.join(parent_dir, filename_path)
	os.makedirs(path)
	print("Directory '% s' created" % filename_path)

if __name__ == '__main__':
	parent_dir = "data"
	if not os.path.exists(parent_dir):
		os.mkdir(parent_dir)
	
	create_dir(parent_dir, ROOT_DIR_METADATA_RAW)
	create_dir(parent_dir, ROOT_DIR_METADATA_NORMALIZED)
	create_dir(parent_dir, ROOT_DIR_INPUT_FILES)
	create_dir(parent_dir, ROOT_DIR_OUTPUT_FILES)
	create_dir(parent_dir, ROOT_DIR_INTERMEDIATE_FILES)
	create_dir(parent_dir, ROOT_DIR_CURATION)
