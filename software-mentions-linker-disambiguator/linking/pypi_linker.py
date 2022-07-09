#!/usr/bin/env python3

"""Links software mentions to the PyPI repository: https://pypi.org/

Usage:
    python pypi_linker.py --input-file <input_file> --generate-new

Details:
    The linker queries the PyPI repository for exact matches (case dependent) of mentions in the input file.
    Parses individual PyPI project pages to extract: pypi package, pypi_url
    Saves raw file under metadata_files/raw/pypi_raw_df.csv
    Saves normalized file (to a common schema among all metadata files) under metadata_files/normalized/pypi_df.csv

Author:
    Ana-Maria Istrate
"""

from utils_linker import * 
from utils_common import *
from schema_normalizations import *

# Search pypi
def build_df(packages, software_mentions):
  """ 
  Filters list of PyPI packages and metadata for a given array of software mentions

  :param packages: software packages available in the PyPI repository
  :param software_mentions: software mentions we want to link

  :return: filtered df containing all the above metadata
  """ 
  pypi_df = pd.DataFrame({'pypi package' : packages})
  pypi_df['pypi_url'] = pypi_df['pypi package'].apply(lambda x: "https://pypi.org/project/" + x)
  pypi_df = pypi_df[pypi_df['pypi package'].isin(software_mentions)]
  return pypi_df

def get_pypi_df(software_mentions, filename = None, save_new = True):
  """ 
  Retrieves links and metadata from the PyPI repository for a list of software mentions. 

  :param software_mentions: list of software mentions to query
  :param filename: where the raw (un-normalized) file will be saved
  :param save_new: True if to create the file from scratch
  
  :return: linked (raw) dataframe
  """ 
  filename_exists = exists(filename)
  if filename_exists and not save_new:
    print('- Retrieving saved Pypi file:', filename)
    return pd.read_csv(filename)
  elif not filename_exists and not save_new:
    raise Exception('- Sorry, the file', filename, 'does not exist on file. Please use --generate-new t generate first.')
  else:
    print('- Generating Python dataframe ... ')
    pypi_url = "https://pypi.org/simple/"
    pypi_html = requests.get(url = pypi_url).text
    soup = BeautifulSoup(pypi_html, 'html.parser')
    packages = []
    num_processed = 0
    num_processed_total = 0
    total_entries = soup.find_all('a')
    num_total = len(total_entries)
    final_dfs = []
    header = True
    mode = 'w'
    for a in soup.find_all('a'):
      packages.append(a.text)
      num_processed += 1
      num_processed_total += 1
      if num_processed == CHUNK_SIZE:
        pypi_df = build_df(packages, software_mentions)
        if len(pypi_df) > 0:
          pypi_df.to_csv(filename, index = False, header = header, mode = mode)
          header = False
          mode = 'a'
          print('Processed', num_processed_total, '/', num_total, 'saving', len(pypi_df), 'to', filename)
          final_dfs.append(pypi_df)
        num_processed = 0
        packages = []
    pypi_df = build_df(packages, software_mentions)
    if len(pypi_df) > 0:
      pypi_df.to_csv(filename, index = False, header = header, mode = mode)
      print('Processed', num_processed_total, '/', num_total, 'saving', len(pypi_df), 'to', filename)
      final_dfs.append(pypi_df)
    return pd.concat(final_dfs)

# Usage: python pypi_linker.py --generate-new
if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Searching pypi database ...')
  parser.add_argument("--input-file", help="Input file", default = 'comm_IDs.tsv.gz', required = False)
  parser.add_argument("--output-file", help="Output file", default = "pypi_df.csv", required = False)
  parser.add_argument("--raw-filename", help="Raw Output file", default = "pypi_raw_df.csv", required = False)
  parser.add_argument("--min-freq", help="Minimum Mention Frequency", type = int, default = FREQ_THRESHOLD, required = False)
  parser.add_argument("--top-k", help="Retrieve top_k mentions", type = int, default = -1, required = False)
  parser.add_argument("--generate-new", help="True if generating a new file from scratch", default = False, action = 'store_true', required = False)
  parser.add_argument("--ID-start", help="ID mention start", type = str, required = False)
  parser.add_argument("--ID-end", help="ID mention end", type = str, required = False)
  args = parser.parse_args()
  print(args)

  pypi_linker = DatabaseLinker(args.input_file, args.output_file, args.min_freq, args.top_k, 
                      args.raw_filename, args.generate_new, 'pypi_df', args.ID_start, args.ID_end)
  pypi_linker.get_metadata_df(get_pypi_df)
  pypi_linker.normalize_schema(normalize_pypi_df)
  pypi_linker.save_to_file()
