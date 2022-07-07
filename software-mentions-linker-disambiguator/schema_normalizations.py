#!/usr/bin/env python3

""" Helper functions for schema normalization for raw metadata files (CRAN, PyPI, Bioconductor, SciCrunch, Github)

Author:
    Ana-Maria Istrate
"""

from utils_linker import *
from utils_common import *
import json

# Normalized fields that all metadata files are mapped to
FIELDS = ['ID', 'software_mention', 'mapped_to', 'source', 'platform', 'package_url', 'description', 'homepage_url', 'other_urls', 'license', 'github_repo', 'github_repo_license',  'exact_match', 'RRID', 'reference']

def parse_license(x):
  """ 
  Parses Github License from the JSON response retrieved from Github API

  :param x: software mention to query

  :return JSON response
  """ 
  try:
    return json.loads(x.replace("\'", "\""))['spdx_id']
  except:
    return None
    
def parse_child_value(parent, attrs):
  children = parent.find_all(attrs = attrs)
  if len(children) > 0:
    return [child.parent['href'] for child in children]
  else:
    return [None]

# Pypi schema normalization
def normalize_pypi_df(pypi_df):
  """ 
  Normalizes raw pypi df

  :param pypi_df: raw pypi dataframe

  :return normalized pypi dataframe (normalized to FIELDS)
  """ 
  new_pypi_df = pd.DataFrame(columns = FIELDS)
  new_pypi_df['software_mention'] = pypi_df['pypi package'].values
  new_pypi_df['mapped_to'] = pypi_df['pypi package'].values
  new_pypi_df['platform'] = 'Pypi'
  new_pypi_df['source'] = 'Pypi Index'
  new_pypi_df['package_url'] = pypi_df['pypi_url']
  new_pypi_df['exact_match'] = True
  descriptions, github_repos, licenses, languages, homepages = get_more_info_pypi(pypi_df['pypi_url'].values)
  new_pypi_df['description'] = descriptions
  new_pypi_df['github_repo'] = github_repos
  new_pypi_df['license'] = licenses
  new_pypi_df['language'] = languages
  new_pypi_df['homepage_url'] = homepages
  new_pypi_df['language'] = 'Python'
  return new_pypi_df

def get_more_info_pypi(pypi_packages_urls):
  """ 
  Scrapes webpages for linked pypi_packages to retrieve additional metadata fields

  :param pypi_packages_urls: PyPI packages URLs to be scraped

  :return lists of: descriptions, github_repos, licenses, languages, homepages for the pypi packages URLs
  """ 
  descriptions = []
  github_repos = []
  licenses = []
  languages = []
  homepages = []
  for url in pypi_packages_urls:
    html = requests.get(url = url).text
    soup = BeautifulSoup(html, 'html.parser')
    description  = soup.find_all(attrs = {"name" : "description"})[0]['content']
    project_links = soup.find_all(text = "Project links")
    if len(project_links) > 0:
      project_links = project_links[0].parent.parent
      homepage = parse_child_value(project_links, {"class" :"fas fa-home"})   
      github_repo = parse_child_value(project_links, {"class" :"fab fa-github"})
      if github_repo == [None]:
        github_repo = [parse_github_from_homepage(homepage)]
    else:
      homepage = [None]
      github_repo = [None]
    classifiers = soup.find_all(text = "Classifiers")
    
    if len(classifiers) > 0:
      classifiers = classifiers[0].parent.parent
      license = classifiers.find_all(text = 'License')
      if len(license) > 0:
        license = license[0].parent.parent.find('a').text.strip()
      else:
        license = None
      language = classifiers.find_all(text = 'Programming Language')
      if len(language) > 0:
        language = [x.text.strip() for x in language[0].parent.parent.find_all('a')]
      else:
        language = None
    else:
      license = None
      language = None
    descriptions.append(description)
    github_repos.append(github_repo)
    homepages.append(homepage)
    licenses.append(license)
    languages.append(language)
  return descriptions, github_repos, licenses, languages, homepages


# Bioconductor schema normalization
def normalize_bioconductor_df(bioconductor_df):
  """ 
  Normalizes raw Bioconductor df

  :param bioconductor_df: raw bioconductor dataframe

  :return normalized bioconductor dataframe (normalized to FIELDS)
  """ 
  new_bioconductor_df = pd.DataFrame(columns = FIELDS)
  new_bioconductor_df['software_mention'] = bioconductor_df['Bioconductor Package'].values
  new_bioconductor_df['mapped_to'] = bioconductor_df['Bioconductor Package'].values
  new_bioconductor_df['platform'] = 'Bioconductor'
  new_bioconductor_df['source'] = 'Bioconductor Index'
  new_bioconductor_df['package_url'] = bioconductor_df['BioConductor Link'].values
  new_bioconductor_df['description'] = bioconductor_df['Title'].values
  new_bioconductor_df['exact_match'] = True
  new_bioconductor_df['language'] = 'R'
  descriptions, paper_urls, homepages, github_urls, licenses = get_more_info_bioconductor(bioconductor_df['BioConductor Link'].values)
  new_bioconductor_df['reference'] = paper_urls
  new_bioconductor_df['homepage_url'] = homepages
  new_bioconductor_df['github_repo'] = github_urls
  new_bioconductor_df['license'] = licenses
  return new_bioconductor_df


def get_more_info_bioconductor(packages_urls):
  """ 
  Scrapes webpages for linked BioConductor packages to retrieve additional metadata fields

  :param packages_urls: BioConductor packages URLs to be scraped

  :return lists of: descriptions, paper_urls, homepage_urls, github_urls, licenses for the BioConductor packages URLs
  """ 
  descriptions = []
  paper_urls = []
  homepage_urls = []
  github_urls = []
  licenses = []
  for url in packages_urls:
    html = requests.get(url = url).text
    soup = BeautifulSoup(html, 'html.parser')
    try:
      description = soup.find_all(attrs = {"property" : "og:description"})[0]['content']
      DOI = soup.find_all('div', attrs = {"class" : "do_not_rebase"})[0].find_all('a')[0]['href']
      homepage = soup.find_all('td', text = "URL")[0].parent.find_all('a')[0]['href']
      license = soup.find_all('td', text = "License")[0].parent.find_all('td')[1].text
      github = parse_github_from_homepage([homepage])
    except:
      description = None
      DOI = None
      homepage = None
      license = None
      github = None
    descriptions.append(description)
    paper_urls.append(DOI)
    github_urls.append(github)
    homepage_urls.append([homepage])
    licenses.append(license)
  return descriptions, paper_urls, homepage_urls, github_urls, licenses

# CRAN schema normalization
def normalize_cran_df(cran_df):
  """ 
  Normalizes raw CRAN df

  :param cran_df: raw CRAN dataframe

  :return normalized CRAN dataframe (normalized to FIELDS)
  """ 
  new_cran_df = pd.DataFrame(columns = FIELDS)
  new_cran_df['software_mention'] = cran_df['CRAN Package'].values
  new_cran_df['mapped_to'] = cran_df['CRAN Package'].values
  new_cran_df['platform'] = 'CRAN'
  new_cran_df['source'] = 'CRAN Index'
  new_cran_df['package_url'] = cran_df['CRAN Link'].values
  new_cran_df['description'] = cran_df['Title'].values
  new_cran_df['exact_match'] = True
  new_cran_df['language'] = 'R'
  descriptions, homepage_urls, github_urls, licenses, citations = get_more_info_cran(cran_df['CRAN Link'].values)
  new_cran_df['homepage_url'] = homepage_urls
  new_cran_df['github_repo'] = github_urls
  new_cran_df['license'] = licenses
  new_cran_df['reference'] = citations
  return new_cran_df

def get_more_info_cran(packages_urls):
  """ 
  Scrapes webpages for linked CRAN packages to retrieve additional metadata fields

  :param packages_urls: CRAN packages URLs to be scraped

  :return lists of: descriptions, homepage_urls, github_urls, licenses, citations for the CRAN packages URLs
  """ 
  descriptions = []
  paper_urls = []
  homepage_urls = []
  github_urls = []
  licenses = []
  citations = []
  for url in packages_urls:
    html = requests.get(url = url).text
    soup = BeautifulSoup(html, 'html.parser')
    description = soup.find_all('h2')[0].parent.find_all('p')[0].text
    table = soup.find_all('table')[0]
    try:
      license = table.find_all('td', text = 'License:')[0].parent.find_all('a')[0].text
      homepages = [x.text for x in table.find_all('td', text = 'URL:')[0].parent.find_all('a')]
    except:
      homepages = [None]
    citation = get_citation_info(url[:-10] + "citation.html")
    github = parse_github_from_homepage(homepages)
    licenses.append(license)
    homepage_urls.append(homepages)
    descriptions.append(description)
    github_urls.append(github)
    citations.append(citation)
  return descriptions, homepage_urls, github_urls, licenses, citations

# Scicrunch schema normalization
def get_citation_info(url):
  """ 
  Retrieves citation info for a CRAN package from a CRAN url

  :param url: url to extract citation from

  :return citation
  """ 
  html = requests.get(url = url).text
  soup = BeautifulSoup(html, 'html.parser')
  title = soup.find_all('title')[0].text
  if title == 'Object not found!':
    return None
  else:
    return soup.find_all('pre')[0]

def normalize_scicrunch_df(scicrunch_df):
  """ 
  Normalizes raw SciCrunch df

  :param scicrunch_df: raw SciCrunch dataframe

  :return normalized SciCrunch dataframe (normalized to FIELDS)
  """ 
  new_scicrunch_df = pd.DataFrame(columns = FIELDS + ['scicrunch_synonyms'])
  new_scicrunch_df['software_mention'] = scicrunch_df['software_name'].values
  new_scicrunch_df['mapped_to'] = scicrunch_df['Resource Name'].values
  
  homepages = [x.split() for x in scicrunch_df['Resource Name Link'].values]
  new_scicrunch_df['homepage_url'] = homepages
  new_scicrunch_df['resource_type'] = scicrunch_df['Resource Type']
  new_scicrunch_df['description'] = scicrunch_df['Description'].values
  new_scicrunch_df['keywords'] = scicrunch_df['Keywords'].values
  new_scicrunch_df['RRID'] = scicrunch_df['Resource ID'].values
  new_scicrunch_df['alternate_RRIDs'] = scicrunch_df['Alternate IDs'].values
  new_scicrunch_df['parent_org'] = scicrunch_df['Parent Organization'].values
  new_scicrunch_df['alternate_RRIDs'] = scicrunch_df['Alternate IDs'].values
  new_scicrunch_df['related_condition'] = scicrunch_df['Related Condition'].values
  new_scicrunch_df['funding_agency'] = scicrunch_df['Funding Agency'].values
  new_scicrunch_df['relation'] = scicrunch_df['Relation'].values
  other_urls = get_scicrunch_other_urls(scicrunch_df['Alternate URLs'].values, scicrunch_df['Old URLs'].values)
  new_scicrunch_df['other_urls'] = other_urls
  new_scicrunch_df['parent_org_link'] = scicrunch_df['Parent Organization Link'].values
  new_scicrunch_df['scicrunch_synonyms'] = scicrunch_df['scicrunch_synonyms']
  references = []
  if 'Reference Link' in scicrunch_df.columns:
    papers = scicrunch_df['Reference Link'].values
  else:
    papers = [None] * len(scicrunch_df)
  proper_citations = scicrunch_df['Proper Citation'].values
  for x, y in zip(papers, proper_citations):
    reference = []
    if x:
      reference.append(x)
    if y:
      reference.append(y)
    references.append(reference)
  
  new_scicrunch_df['reference'] = references
  new_scicrunch_df['source'] = 'SciCrunch API'
  new_scicrunch_df['package_url'] = scicrunch_df['Resource ID Link'].values
  new_scicrunch_df['exact_match'] = (scicrunch_df['software_name'] == scicrunch_df['Resource Name']).values

  homepage_githubs = [parse_github_from_homepage(urls) for urls in homepages]
  other_urls_githubs = [parse_github_from_homepage(urls) for urls in other_urls]
  github_repos_all = []
  for x, y, in zip(homepage_githubs, other_urls_githubs):
    github_repos = []
    if x:
      github_repos.append(x)
    if y:
      github_repos.append(y)
    if not x and not y:
      github_repos = [None]
    github_repos_all.append(github_repos)
  new_scicrunch_df['github_repo'] = github_repos_all
  return new_scicrunch_df

def get_scicrunch_other_urls(alternate_urls, old_urls):
  """ 
  Combines alternate and old URLs retrieved from SciCrunch 

  :param alternate_urls: list of alternate urls retrieved from SciCrunch
  :param old_urls: list of old urls retrieved from SciCrunch

  :return combined list
  """ 
  all_urls = []
  for alternate_urls, old_urls in zip (alternate_urls, old_urls):
    if alternate_urls and alternate_urls == alternate_urls:
      alternate_urls = alternate_urls.split()
    else:
      alternate_urls = []
    if old_urls and old_urls == old_urls:
      old_urls = old_urls.split()
    else:
      old_urls = []
    all_urls.append(alternate_urls + old_urls)
  return all_urls


# Github schema normalization
def normalize_github_df(github_df):
  """ 
  Normalizes raw Github df

  :param github_df: raw Github dataframe

  :return normalized Github dataframe (normalized to FIELDS)
  """ 
  github_df['parsed_license'] = github_df['license'].apply(parse_license)
  github_df.drop(github_df[github_df['best_github_match'] == 'no_github_entry'].index, inplace = True)
  github_df['github_url'] = github_df['github_url'].apply(lambda x: "https://github.com" + str(x)[28:])

  new_github_df = pd.DataFrame(columns = FIELDS)
  new_github_df['software_mention'] = github_df['software_mention'].values
  new_github_df['package_url'] = github_df['github_url'].values
  new_github_df['mapped_to'] = github_df['best_github_match'].values
  new_github_df['description'] = github_df['description'].values
  new_github_df['github_repo'] = github_df['github_url'].values
  new_github_df['github_repo_license'] = github_df['parsed_license'].values
  new_github_df['exact_match'] = github_df['exact_match'].values
  new_github_df['source'] = 'Github API'
  return new_github_df

def parse_github_from_homepage(homepages):
  """ 
  Checks if list of homepages contains Github page and if so, returns it.

  :param homepages: list of homepages
  
  :return homepage, if a Github page, or None
  """ 
  github = None
  for homepage in homepages:
    if homepage and "//github.com" in homepage:
      github = homepage
  return github