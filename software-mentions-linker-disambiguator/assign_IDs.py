#!/usr/bin/env python3
"""Assigns IDs to a given input file. Assumes mention2ID dictionary exists under ROOT_DIR_INPUT_FILES + args.input_file

Usage:
    python assign_IDs.py

Author:
    Ana-Maria Istrate
"""

from utils_linker import *

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Searching Bioconductor index...')
	parser.add_argument("--input-file", help="Input file", default = 'comm_IDs.tsv', required = False)
	parser.add_argument("--output-file", help="Input file", default = 'comm_IDs.tsv', required = False)
	args = parser.parse_args()
	print(args)
	mentions_df = pd.read_csv(ROOT_DIR_INPUT_FILES + args.input_file, sep='\\t', engine='python', compression = 'gzip')
	software_mentions = mentions_df['software'].unique()
	print('- Finished reading', args.input_file)
	map_filename = ROOT_DIR_INTERMEDIATE_FILES + 'mention2ID.pkl'
	IDs = range(len(software_mentions))
	mention2ID = {x:'SM' + str(y) for x, y in zip(software_mentions, IDs)}
	ID2mention = {y:x for x, y in mention2ID.items()}
	with open(map_filename, 'wb') as f:
	    pickle.dump(mention2ID, f)
	print('- Generated mappings for', len(mention2ID), 'software mentions') 

	mentions_df['ID'] = mentions_df['software'].apply(lambda x: mention2ID[x])
	mentions_df.to_csv(ROOT_DIR_INPUT_FILES + args.output_file, sep="\t", index = False)
