# A large dataset of software mentions in the biomedical literature

## About
We describe the CZ Software Mentions dataset, a new dataset of software mentions in biomedical papers. Plain-text software mentions are extracted with a trained SciBERT model from several sources: the NIH PubMed Central collection and from papers provided by various publishers to the Chan Zuckerberg Initiative. The dataset provides sources, context and metadata, and, for a number of mentions, the disambiguated software entities and links. We extract 1.12 million unique string software mentions from 2.4 million papers in the NIH PMC-OA Commercial subset, 481k unique mentions from the NIH PMC-OA Non-Commercial subset (both gathered in October 2021) and 934k unique mentions from 4 million papers in the Publishers’ collection. There is variation in how software is mentioned in papers and extracted by the NER algorithm. We propose a clustering-based disambiguation algorithm to map plain-text software mentions into distinct software entities and apply it on the NIH PubMed Central Commercial collection. Through this methodology, we disambiguate 1.12 million unique strings extracted by the NER model into ~97,000 unique software entities, covering 78% of all links. We link 185,000 of the mentions to a repository, covering about 55% of all software-paper links.  We make all data and code publicly available as a new resource to help assess the impact of software (in particular scientific open source projects) on science.

This code repository accompanies our [data release](https://doi.org/10.5061/dryad.6wwpzgn2c). It is released under the MIT License.

## Authors

Ana-Maria Istrate,
Donghui Li,
Dario Taraborelli,
Michaela Torkar,
Boris Veytsman,
Ivana Williams.

[Chan Zuckerberg Initiative](https://chanzuckerberg.com) 


## Directories

* [sample_notebooks](sample_notebooks) - sample notebooks for the usage of the dataset
* [software-mentions-extractor](software-mentions-extractor) - the code for extracting mentions from OA PMC
* [software-mentions-linker-disambiguator](software-mentions-linker-disambiguator) - the code for linking and disambiguation of software mentions

## Links

* Dataset: [https://doi.org/10.5061/dryad.6wwpzgn2c](https://doi.org/10.5061/dryad.6wwpzgn2c) (CC0)
* Preprint: [https://arxiv.org/abs/2209.00693](https://arxiv.org/abs/2209.00693) (CC BY)
* SciBERT model: [https://github.com/chanzuckerberg/software-mention-extraction](https://github.com/chanzuckerberg/software-mention-extraction)


## Code of Conduct

This project adheres to the Contributor Covenant [code of conduct](https://github.com/chanzuckerberg/.github/blob/master/CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to [opensource@chanzuckerberg.com](mailto:opensource@chanzuckerberg.com).

## Reporting Security Issues

If you believe you have found a security issue, please responsibly disclose by contacting us at [security@chanzuckerberg.com](mailto:security@chanzuckerberg.com).
