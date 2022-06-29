#!/usr/bin/env python3
'''Extracts mentions from papers

Usage:
    ./software-mentions-extractor [options] DIR DIR DIR...

Output:
    A list of files with then names corresponding to the directories 
    on the command line, with / changed to _ and a prefix f_.  Each file
    is the result of extraction of all files in the current directory.
    Each file is a tab separated list with the following fields:
     - Paper license (comm or non_comm);
     - Paper location in the directory;
     - Paper pmcid or empty;
     - Paper pmid or empty;
     - Paper doi or empty;
     - Paper publication date or empty;
     - Mention source: paper_title, paper_abstract, fig_caption, 
       tab_caption, section name;
     - Mention source number (paragraph number for body text, caption
       number for captions) or empty;
     - Source text (sentence).
     - Software name;
     - Software version or empty.


Author:
    Boris Veytsman

'''

import pubmed_parser as pp
import argparse
import sys
import numpy as np
import torch
from transformers import BertForTokenClassification, BertTokenizerFast
import re
from os.path import exists
import glob
import os

tag_values = ['I-version', 'O', 'I-software', 'B-version', 'B-software', 'PAD']


argparser = argparse.ArgumentParser(
    description=
    'Extract software mentions from papers in NXML format.'
)

argparser.add_argument('directories', metavar='DIR', 
                    nargs='+', help='list ' +
                    'of directories with NXML files, ' +
                    'one per line')

argparser.add_argument('-d', '--debug', action='store_true',
                       help='Debug mode on')
                      

argparser.add_argument("-l", "--license", default="",
                       help="License, either comm or non comm. " +
                       "If empty (the default), determine from the " +
                       "directory names")

argparser.add_argument("-m", "--model",
                       default=
                       "../../software-mention-extraction/models/scibert_software_sent",
                       help="The location of the trained model, " +
                       "by default %(default)s")

argparser.add_argument("-o", "--outputdir", default="test/output",
                       help="Output directory, by default " +
                       "%(default)s.")



def process_directory (directory, args):
    '''Process all files in one directory from the arguments.

    Arguments:
    directory -- directory to process
    args -- list of command line arguments

    Returns:
    None.

    We open output file and write down the results
    '''
    directory = directory.rstrip()
    output_file_base = "f_" + re.sub("/", "_", directory)
    if (exists(args.outputdir + "/" + output_file_base + ".tsv") or
        exists(args.outputdir + "/" + output_file_base + ".tsv.tmp")):
        return
    output = open(args.outputdir + "/" + output_file_base + ".tsv.tmp",
                  "w")
    headers = ('license', 'location', 'pmcid', 'pmid', 'doi',
               'pubdate', 'source', 'number', 'text', 'software',
               'version')
    print('\t'.join(headers), file=output)
    
    license = args.license
    if (len(license)==0):
        if(re.search("/non_comm/", directory, flags=re.IGNORECASE)):
            license='non_comm'
        else:
            license='comm'
    for file in glob.glob(directory + "/*.nxml"):
        process_file(file, license, output)
    
    output.close()
    os.rename(args.outputdir + "/" + output_file_base + ".tsv.tmp",
              args.outputdir + "/" + output_file_base + ".tsv")

def process_file (file, license, output):
    '''Process one file and dump the results (if any) to output handle.

    Arguments:
    file -- file location (XML)
    license -- the current license
    output -- a handle

    '''

    if args.debug:
        print("Processing  " + file, file=sys.stderr)
    
    try:
        tree = pp.utils.read_xml(file)
    except:
        return
    
    try:
        metadata = pp.parse_pubmed_xml(tree=tree)
    except:
        return
    
    pmcid = metadata.get('pmc', '')
    pmid = metadata.get('pmid', '')
    doi = metadata.get('doi', '')
    pubdate = metadata.get('publication_year', '')
    title = metadata.get('full_title', '')
    abstract = metadata.get('abstract', '')

    pmcid = re.sub("[ \t\n\r]+", " ", pmcid)
    pmid = re.sub("[ \t\n\r]+", " ", pmid)
    doi = re.sub("[ \t\n\r]+", " ", doi)
    pubdate = re.sub("[ \t\n\r]+", " ", pubdate)
    title = re.sub("[ \t\n\r]+", " ", title)
    abstract = re.sub("[ \t\n\r]+", " ", abstract)

    id = [license, file, pmcid, pmid, doi, pubdate]

    process_object(id, 'paper_title', 0, title, output)
    process_object(id, 'paper_abstract', 0, abstract, output)

    try:
        figs = pp.parse_pubmed_caption(tree=tree)
    except:
        figs = None
    if figs != None:
        for i, fig in enumerate(figs):
            process_object(id, 'fig_caption', i,
                           fig.get('fig_caption', ''), output)
    try:
        tables = pp.parse_pubmed_table(tree=tree,
                                       return_xml=False)
    except:
        tables = None
    if tables != None:
        for i, table in enumerate(tables):
            process_object(id, 'tab_caption', i,
                           table.get('caption', ''),
                           output)

    try:
        paras = pp.parse_pubmed_paragraph(tree=tree,
                                          all_paragraph=True)
    except:
        paras = None
    if paras != None:
        for i, para in enumerate(paras):
            section = para.get('section', '')
            section = re.sub("[ \t\n\r]+", " ", section)
            text = para.get('text', '')
            process_object(id, section, i,
                           text, output)
                           
    
def process_object(id, source, number, text, output):
    '''Extract software mentions from paragraph, title, abstract or caption.

    Arguments:
    id -- a list of ids for the paper
    source -- a string with the source (abstract, paragraph...)
    number -- number of the source in the sequence of sources
    text -- the text of the source
    output -- handle to dump the result
    '''

    text = re.sub("[ \t\n\r]+", " ", text)
    sentences = text.split(". ")
    for sentence in sentences:
        sentence = sentence[:512]
        ner_result = get_soft_ver_labels(sentence)        
        for soft, version in collapse(ner_result):            
            print('\t'.join(id +
                            [source, str(number), sentence,
                             soft, version]),
                  file=output)

def get_soft_ver_labels(sentence):
    '''Convert a sentence into a list of tuples (token, tag).
    
    Arguments:
    sentence -- a sentence to tag

    Returns:
    A list of tokens and tags
    '''
    tokenized_sentence = tokenizer.encode(sentence)
    input_ids = torch.tensor([tokenized_sentence])#.cuda()
    with torch.no_grad():
        output = model(input_ids)
    label_indices = np.argmax(output[0].to('cpu').numpy(), axis=2)
    tokens = tokenizer.convert_ids_to_tokens(input_ids.to('cpu').numpy()[0])
    new_tokens, new_labels = [], []
    for token, label_idx in zip(tokens, label_indices[0]):
        if token.startswith("##"):
            new_tokens[-1] = new_tokens[-1] + token[2:]
        else:
            new_labels.append(tag_values[label_idx])
            new_tokens.append(token)
    return list(zip(new_tokens[1:-1],
                    new_labels[1:-1]))
    
def collapse (ner_result):
    '''Convert tagged sequence of tokens into list [(software, version),...]

    Arguments:
    ner_result -- a list of tuples (token, tag)

    Returns:
    a list of tuples (soft, ver)
    '''
    collapsed_list = []
    current_soft = ""
    current_version = ""
    for token, tag in ner_result:
        if tag == "O":
            if current_soft != "":
                collapsed_list.append((current_soft, current_version))
            current_soft=""
            current_version=""
        if tag == "B-software" or tag == "I-software":
            if current_version != "":
                collapsed_list.append((current_soft, current_version))
                current_soft = str(token)
                curent_version=""
            else:
                if current_soft == "":
                    current_soft = str(token)
                else:
                    current_soft = current_soft + " " + str(token)
        if tag == "I-version" or tag == "B-version":
            if current_version == "":
                current_version = str(token)
            else:
                current_version = current_version + " " + str(token)                
    if current_soft != "":
        collapsed_list.append((current_soft, current_version))
    return collapsed_list



if __name__ == "__main__":
    args = argparser.parse_args()
    trained_model = args.model
    tokenizer = BertTokenizerFast.from_pretrained(trained_model, do_lower_case=False)
    model = BertForTokenClassification.from_pretrained(trained_model)
    for directory in args.directories:
        process_directory(directory, args)



