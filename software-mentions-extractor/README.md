# Scripts for software extraction #

## Introduction

The scripts in this directory extract software mentions from NXML files downloaded from PMC OA files, [https://www.ncbi.nlm.nih.gov/pmc/tools/openftlist/](https://www.ncbi.nlm.nih.gov/pmc/tools/openftlist/). We assume that you downloaded the files, and extracted the contents. Then you have file system with two main directories, comm and non\_comm for the commercial use subset and the non commercial use only subset.  Each directory has subdurectories, one per journal.  The subdirectories contain NXML files, one per paper.

You use software-mentions-extractor.py to extract the mentions. If needed, the result can be detokenized using detokenyzer.py auxillary script.

The extractor is designed to work with GNU parallel command [https://doi.org/10.5281/zenodo.6682930](https://doi.org/10.5281/zenodo.6682930).  For example, suppose your working directory contains comm and non\_comm directories, then you can extract all mentions from the commercial subset using

   ls -d comm/* | parallel -d software-mentions-extractor -o output


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
- Paper location in the directory structure (see above);
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
