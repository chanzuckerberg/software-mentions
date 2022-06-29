#!/usr/bin/env python3
'''Detokenize output of software mentions extractor

Usage:
    ./detokinizer.py [options] < input > output

Details:
    The detokenizer accepts a tab-separated stream of lines.
    One column in the stream is considered TEXT, and there is
    any number of tokenized columns.  For each tokenized columns
    we substitute the result by deleting spurious spaces and adding
    symbols that may be omitted by the extractor.

Author:
    Boris Veytsman

'''

import argparse
import sys
import re

argparser = argparse.ArgumentParser(
    description=
    'Detokenize output of software mentions extractor.  Reads stdin and outputs to stdout'
)

argparser.add_argument('-d', '--debug', action='store_true',
                       help='Debug mode on')

argparser.add_argument('-t', '--text_column', type=int,
                       default=9,
                       help='Number of the column with the text, by default %(default)s.')

argparser.add_argument('-r', '--results_column', type=int, nargs='+',
                       default=[10, 11],
                       help='Number(s) of the column(s) with the results, by default %(default)s.')

def process_line(line, args):
    '''Process a line from stdin and output the result to stdout

    Arguments:
    line -- line to read
    args -- command line arguments
    '''
    line = line.rstrip('\n')
    columns = line.split('\t')
    text = columns[args.text_column-1]
    for i in args.results_column:
        columns[i-1] = process_column(columns[i-1], text, args)
    print ("\t".join(columns))

def process_column(tokens, text, args):
    '''Process column and return detokenized string.

    Arguments:
    tokens -- string of tokens
    text -- text to seek the strings
    args -- command line arguments
    
    Output:  
    detokenized string
    '''
    if len(tokens)==0:
        return(tokens)
    pattern = re.sub("\\.", "\\.", tokens.strip())
    pattern = re.sub("\\^", "\\^", pattern)
    pattern = re.sub("\\$", "\\$", pattern)
    pattern = re.sub("\\*", "\\*", pattern)
    pattern = re.sub("\\+", "\\+", pattern)
    pattern = re.sub("\\?", "\\?", pattern)
    pattern = re.sub("\\(", "\\(", pattern)
    pattern = re.sub("\\)", "\\)", pattern)
    pattern = re.sub("\\[", "\\[", pattern)
    pattern = re.sub("\\]", "\\]", pattern)
    pattern = re.sub("\\{", "\\{", pattern)
    pattern = re.sub("\\}", "\\}", pattern)
    pattern = re.sub(" ", " *", pattern)
    pattern = "\\w*" + pattern + "\\w*"
    if args.debug:
        print(pattern, file = sys.stderr)
    match = re.search(pattern, text)
    if match:
        return(match.group())
    else:
        return(tokens)

    
if __name__ == "__main__":
    args = argparser.parse_args()
    for line in sys.stdin.readlines():
        process_line(line, args)
