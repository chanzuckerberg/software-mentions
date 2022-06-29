# Scripts for software extraction #

## software-mentions-extractor.py ##

Extracts mentions from papers

Usage:
    ./software-mentions-extractor [options] DIR DIR DIR...

Output:

A list of files with then names corresponding to the directories 
on the command line, with / changed to \_ and a prefix f\_.  Each file
is the result of extraction of all files in the current directory.
Each file is a tab separated list with the following fields:

- Paper license (comm or non\_comm);
- Paper location in the directory;
- Paper pmcid or empty;
- Paper pmid or empty;
- Paper doi or empty;
- Paper publication date or empty;
- Mention source: paper\_title, paper\_abstract, fig\_caption, 
  tab\_caption, section name;
- Mention source number (paragraph number for body text, caption
  number for captions) or empty;
- Source text (sentence).
- Software name (space separated tokens);
- Software version or empty (space separated tokens).


Options:

*  -h, --help:            show this help message and exit
*  -d, --debug:           Debug mode on
*  -l LICENSE, --license LICENSE:
                        License, either comm or non comm. If empty (the default), determine from the directory names
*  -m MODEL, --model MODEL: The location of the trained model, by default ../../software-mention-extraction/models/scibert_software_sent
*  -o OUTPUTDIR, --outputdir OUTPUTDIR:
                        Output directory, by default test/output.

**Note:** The extractor requires a modified version of pubmed\_parser available at [https://github.com/borisveytsman/pubmed_parser](https://github.com/borisveytsman/pubmed_parser)


## detokenizer.py ##

Detokenize output of software mentions extractor

Usage:
    ./detokinizer.py [options] < input > output

Details:
    The detokenizer accepts a tab-separated stream of lines.
    One column in the stream is considered TEXT, and there is
    any number of tokenized columns.  For each tokenized columns
    we substitute the result by deleting spurious spaces and adding
    symbols that may be omitted by the extractor.

Options:

*  -h, --help            show this help message and exit
*  -d, --debug           Debug mode on
*  -t TEXT_COLUMN, --text_column TEXT_COLUMN
                        Number of the column with the text, by default 9.
*  -r RESULTS_COLUMN [RESULTS_COLUMN ...], --results_column RESULTS_COLUMN [RESULTS_COLUMN ...]
                        Number(s) of the column(s) with the results, by default [10, 11].
