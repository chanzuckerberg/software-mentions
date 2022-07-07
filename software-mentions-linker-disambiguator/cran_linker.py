#!/usr/bin/env python3

"""Links software mentions to the CRAN repository: https://cran.r-project.org/

Usage:
    python cran_linker.py --input-file <input_file> --generate-new

Details:
    The linker queries the CRAN repository for exact matches (case dependent) of mentions in the input file.
    Parses individual CRAN project pages to extract: CRAN Package, CRAN Link and Ttile 
    Saves raw file under metadata_files/raw/cran_raw_df.csv
    Saves normalized file (to a common schema among all metadata files) under metadata_files/normalized/cran_df.csv

Author:
    Ana-Maria Istrate
"""

from utils_linker import *
from utils_common import *
from schema_normalizations import *

def build_df(packages, software_mentions, cran_links, titles):
  """ 
  Filters list of CRAN packages and metadata for a given array of software mentions

  :param packages: software packages available in the CRAN repository
  :param software_mentions: software mentions we want to link
  :param cran_links: links of software_mentions to the CRAN repository
  :param titles: titles of software packages in the CRAN repository

  :return: filtered df containing all the above metadata
  """ 
  df = pd.DataFrame({'CRAN Package' : packages, 'CRAN Link' : cran_links, 'Title' : titles})
  df = df[df['CRAN Package'].isin(software_mentions)]
  return df

def get_cran_df(software_mentions, filename = None, save_new = True):
  """ 
  Retrieves links and metadata from the CRAN repository for a list of software mentions. 

  :param software_mentions: list of software mentions to query
  :param filename: where the raw (un-normalized) file will be saved
  :param save_new: True if to create the file from scratch
  
  :return: linked (raw) dataframe
  """ 
  filename_exists = exists(filename)
  if filename_exists and not save_new:
    print('- Retrieving saved CRAN file:', filename)
    return pd.read_csv(filename)
  elif not filename_exists and not save_new:
    raise Exception('- Sorry, the file', filename, 'does not exist on file. Please use --generate-new t generate first.')
  else:
    print('- Generating CRAN dataframe ... ')
    # Parses the CRAN Index
    cran_url = "https://cran.r-project.org/web/packages/available_packages_by_name.html"
    cran_html = requests.get(url = cran_url).text
    soup = BeautifulSoup(cran_html, 'html.parser')
    packages = []
    titles = []
    cran_links = []
    num_processed = 0
    num_processed_total = 0
    total_entries = soup.find_all('tr')[1:]
    num_total = len(total_entries)
    print(num_total)
    final_dfs = []
    header = True
    mode = 'w'
    for tr in total_entries:
      num_processed += 1
      num_processed_total += 1
      tds = tr.find_all('td')
      if len(tds) > 1:
        hrefs = str(tds[0].find_all('a')[0])
        package_link, package_name = parse_html_string(hrefs)
        cran_link = 'https://cran.r-project.org/' + package_link[6:]
        title = tds[1].text
        packages.append(package_name)
        cran_links.append(cran_link)
        titles.append(title)
        if num_processed == CHUNK_SIZE:
          df = build_df(packages, software_mentions, cran_links, titles)
          if len(df) > 0:
            df.to_csv(filename, index = False, header = header, mode = mode)
            mode = 'a'
            header = False
            print('Processed', num_processed_total, '/', num_total, 'saving', len(df), 'to', filename)
            final_dfs.append(df)
          num_processed = 0
          packages = []
          titles = []
          cran_links = []
    df = build_df(packages, software_mentions, cran_links, titles)
    if len(df) > 0:
      final_dfs.append(df)
      df.to_csv(filename, index = False, header = header, mode = mode)
      print('Processed', num_processed_total, '/', num_total, 'saving', len(df), 'to', filename)
    return pd.concat(final_dfs)

# Usage: python cran_linker.py --generate-new
if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Searching CRAN database ...')
  parser.add_argument("--input-file", help="Input file", default = 'pmc_oa_nov_22.tsv', required = False)
  parser.add_argument("--output-file", help="Output file", default = "cran_df.csv", required = False)
  parser.add_argument("--raw-filename", help="Raw Output file", default = "cran_raw_df.csv", required = False)
  parser.add_argument("--min-freq", help="Minimum Mention Frequency", type = int, default = FREQ_THRESHOLD, required = False)
  parser.add_argument("--top-k", help="Retrieve top_k mentions", type = int, default = -1, required = False)
  parser.add_argument("--generate-new", help="True if generating a new file from scratch", default = False, action = 'store_true', required = False)
  parser.add_argument("--ID-start", help="ID mention start", type = str, required = False)
  parser.add_argument("--ID-end", help="ID mention end", type = str, required = False)
  args = parser.parse_args()
  print(args)

  cran_linker = DatabaseLinker(args.input_file, args.output_file, args.min_freq, args.top_k, 
                      args.raw_filename, args.generate_new, 'cran_df', args.ID_start, args.ID_end)
  cran_linker.get_metadata_df(get_cran_df)
  cran_linker.normalize_schema(normalize_cran_df)
  cran_linker.save_to_file()
