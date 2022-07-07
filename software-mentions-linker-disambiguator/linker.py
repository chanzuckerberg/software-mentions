#!/usr/bin/env python3

"""Links a software mentions file to metadata files: PyPI, CRAN, BioConductor, SciCrunch, Github

Usage:
    python linker.py --input-file <input_file>

Details:
    The linker generates or retrieves metadata files, and then links software mentions in the given input 
    file to it. 

Author:
    Ana-Maria Istrate
"""

from utils_linker import *
from utils_common import *
import pickle

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Searching Bioconductor index...')
	parser.add_argument("--input-file", help="Input file", default = 'non_comm.tsv', required = False)
	parser.add_argument("--bioconductor-file", help="Bioconductor file", default = 'bioconductor_df.csv', required = False)
	parser.add_argument("--cran-file", help="CRAN file", default = "cran_df.csv", required = False)
	parser.add_argument("--pypi-file", help="Pypi file", default = "pypi_df.csv", required = False)
	parser.add_argument("--scicrunch-file", help="Scicrunch File", default = "scicrunch_df.csv", required = False)
	parser.add_argument("--github-file", help="Github File", default = "github_df.csv", required = False)
	parser.add_argument("--min-freq", help="Minimum Mention Frequency", type = int, default = FREQ_THRESHOLD, required = False)
	parser.add_argument("--top-k", help="Retrieve top_k mentions", type = int, default = -1, required = False)
	parser.add_argument("--master-top-mentions-file", help="Output file for disambiguated top mentions", default = "top_mentions_linked.csv", required = False)
	parser.add_argument("--master-output-file", help="Output file for disambiguated all mentions", default = "dataset_linked.csv", required = False)


	args = parser.parse_args()
	print(args)

	# Read input file
	top_mentions_df, top_software_mentions, all_mentions_df, all_software_mentions = load_mentions('pmc-oa', ROOT_DIR_INPUT_FILES + args.input_file, 
    freq_threshold = args.min_freq, top_num_entities = args.top_k)
	initial_dataset_cols = list(all_mentions_df.columns.values)
	print(len(all_mentions_df))
	save_new_file = False

	# Retrieve metadata files
	bioconductor_df = get_bioconductor_df(top_software_mentions, ROOT_DIR_METADATA_NORMALIZED + args.bioconductor_file, save_new_file)
	cran_df = get_cran_df(top_software_mentions, ROOT_DIR_METADATA_NORMALIZED + args.cran_file, save_new_file)
	github_df = get_github_df(top_software_mentions, ROOT_DIR_METADATA_NORMALIZED + args.github_file, save_new_file)
	pypi_df = get_pypi_df(top_software_mentions, ROOT_DIR_METADATA_NORMALIZED + args.pypi_file, save_new_file)
	scicrunch_df = get_scicrunch_df(top_software_mentions, ROOT_DIR_METADATA_NORMALIZED + args.scicrunch_file, save_new_file)

	update_df_with_metadata_bools(top_mentions_df, pypi_df, bioconductor_df, cran_df, scicrunch_df, github_df)

	# Retrieve mention2ID mapping
	mention2ID = retrieve_ID_map()
	assign_IDs(top_mentions_df, mention2ID, 'software')
	assign_IDs(all_mentions_df, mention2ID, 'software')

	# Generate metadata file
	metadata_df = pd.concat([pypi_df, bioconductor_df, cran_df, scicrunch_df, github_df]).groupby(['ID', 'software_mention']).agg(list).reset_index()
	top_mentions_df.drop(columns = ['license', 'location', 'pubdate', 'source', 'number', 'text', 'version'], inplace = True)
	top_mentions_df.rename(columns = {'pmcid' : 'num_pmcids', 'pmid' : 'num_pmids', 'doi' : 'num_dois', 'software' : 'software_mention'}, inplace = True)
	top_mentions_linked_df = pd.merge(top_mentions_df, metadata_df, on = ['ID', 'software_mention'])
	all_mentions_linked_df= pd.merge(top_mentions_linked_df, all_mentions_df, left_on = ['ID', 'software_mention'], right_on = ['ID', 'software'], how = 'right')

	# Save linked file
	software_mentions_linked_filename = ROOT_DIR_OUTPUT_FILES + args.master_top_mentions_file
	dataset_linked_filename = ROOT_DIR_OUTPUT_FILES + args.master_output_file
	top_mentions_linked_df.to_csv(software_mentions_linked_filename)
	all_mentions_linked_df = all_mentions_linked_df[COLS_DATASET_DF]
	print(len(fall_mentions_linked_df))
	all_mentions_linked_df.to_csv(dataset_linked_filename)
	print('- Saved linked dataset to file:', dataset_linked_filename)
	print('- This is how the dataset looks like: ')
	print(all_mentions_linked_df.head())
