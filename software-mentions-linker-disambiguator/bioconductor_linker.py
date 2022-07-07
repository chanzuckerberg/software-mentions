#!/usr/bin/env python3

"""Links software mentions to the BioConductor repository: https://www.bioconductor.org

Usage:
    python bioconductor_linker.py --input-file <input_file> --generate-new

Details:
    The linker queries the Bioconductor repository for exact matches (case dependent) of mentions in the input file.
    Parses individual Bioconductor project pages to extract: BioConductor Package, BioConductor Link, Maintainer, Title
    Saves raw file under metadata_files/raw/bioconductor_raw_df.csv
    Saves normalized file (to a common schema among all metadata files) under metadata_files/normalized/bioconductor_df.csv

Author:
    Ana-Maria Istrate
"""

from utils_linker import *
from utils_common import *
from schema_normalizations import *
from bs4 import BeautifulSoup

# Search BioConductor
def build_df(packages, software_mentions, bioconductor_links, maintainers, titles):
  """ 
  Filters list of BioConductor packages and metadata for a given array of software mentions

  :param packages: software packages available in the BioConductor repository
  :param software_mentions: software mentions we want to link
  :param bioconductor_links: links of software_mentions in the BioConductor repository
  :param maintainers: maintainers of software packages in the BioConductor repository
  :param titles: titles of software packages in the BioConductor repository

  :return: filtered df containing all the above metadata
  """ 

  df = pd.DataFrame({'Bioconductor Package' : packages, 'BioConductor Link' : bioconductor_links, 'Maintainer' : maintainers, 'Title' : titles})
  df = df[df['Bioconductor Package'].isin(software_mentions)]
  return df

def get_bioconductor_df(software_mentions, filename = None, save_new = True):
  """ 
  Retrieves links and metadata from the BioConductor repository for a list of software mentions. 

  :param software_mentions: list of software mentions to query
  :param filename: where the raw (un-normalized) file will be saved
  :param save_new: True if to create the file from scratch

  :return: linked (raw) dataframe
  """ 

  filename_exists = exists(filename)
  if filename_exists and not save_new:
    print('- Retrieving saved Bioconductor file:', filename)
    return pd.read_csv(filename)
  elif not filename_exists and not save_new:
    raise Exception('- Sorry, the file', filename, 'does not exist on file. Please use --generate-new t generate first.')
  else:
    header = True
    print('- Generating Bioconductor dataframe ... ')
    bioconductor_url = "https://www.bioconductor.org/packages/release/bioc/"
    bioconductor_html = requests.get(url = bioconductor_url).text
    soup = BeautifulSoup(bioconductor_html, 'html.parser')
    packages = []
    maintainers = []
    titles = []
    bioconductor_links = []
    num_processed = 0
    num_processed_total = 0
    total_entries = soup.find_all('tr')[1:]
    num_total = len(total_entries)
    final_dfs = []
    mode = 'w'
    for tr in total_entries:
      num_processed += 1
      num_processed_total += 1
      tds = tr.find_all('td')
      hrefs = str(tds[0].find_all('a')[0])
      package_link, package_name = parse_html_string(hrefs)
      bioconductor_link = 'https://www.bioconductor.org/packages/release/bioc/' + package_link
      maintainer = tds[1].text
      title = tds[2].text
      packages.append(package_name.lower())
      bioconductor_links.append(bioconductor_link)
      maintainers.append(maintainer)
      titles.append(title)
      if num_processed == CHUNK_SIZE:
        df  = build_df(packages, software_mentions, bioconductor_links, maintainers, titles)
        if len(df) > 0:
          df.to_csv(filename, index = False, header = header, mode = mode)
          header = False
          print('Processed', num_processed_total, '/', num_total, 'saving', len(df), 'to', filename)
          final_dfs.append(df)
          mode = 'a'
        num_processed = 0
        packages = []
        maintainers = []
        titles = []
        bioconductor_links = []
    df = build_df(packages, software_mentions, bioconductor_links, maintainers, titles)
    if len(df) > 0:
      df.to_csv(filename, index = False, header = header, mode = mode)
      final_dfs.append(df)
      print('Processed', num_processed_total, '/', num_total, 'saving', len(df), 'to', filename)
    return pd.concat(final_dfs)


# Usage: python bioconductor_linker.py --generate-new
if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Searching Bioconductor index...')
  parser.add_argument("--input-file", help="Input file", default = 'pmc_oa_nov_22.tsv', required = False)
  parser.add_argument("--output-file", help="Output file", default = "bioconductor_df.csv", required = False)
  parser.add_argument("--raw-filename", help="Raw Output file", default = "bioconductor_raw_df.csv", required = False)
  parser.add_argument("--min-freq", help="Minimum Mention Frequency", type = int, default = FREQ_THRESHOLD, required = False)
  parser.add_argument("--top-k", help="Retrieve top_k mentions", type = int, default = -1, required = False)
  parser.add_argument("--generate-new", help="True if generating a new file from scratch", default = False, action = 'store_true', required = False)
  parser.add_argument("--ID-start", help="ID mention start", type = str, required = False)
  parser.add_argument("--ID-end", help="ID mention end", type = str, required = False) 
  args = parser.parse_args()
  print(args)

  bioconductor_linker = DatabaseLinker(args.input_file, args.output_file, args.min_freq, args.top_k, 
                      args.raw_filename, args.generate_new, 'bioconductor_df', args.ID_start, args.ID_end)
  bioconductor_linker.get_metadata_df(get_bioconductor_df)
  bioconductor_linker.normalize_schema(normalize_bioconductor_df)
  bioconductor_linker.save_to_file()
