
import dateutil
import inspect
import copy
from pymedx import PubMed, article
import pymedx
import pandas as pd
import pybibx
from hamilton.io import utils
from hamilton import driver
from hamilton.function_modifiers import datasaver, dataloader, cache, config
import textwrap
from pathlib import Path
from hamilton import lifecycle
import pickle
from datetime import datetime
from enum import Enum
from motor_learning_network.constants import RAW_DATA_PATH, FIGURES_PATH, PROCESSED_DATA_PATH
from motor_learning_network.get_pubmed_dataset import LoadingFrom

SAVED_DB_PATH = Path(RAW_DATA_PATH, 'articles.pkl')

# @dataloader
# def references(path_to_references: Path) -> dict[str,list[str]]:
#     with open(path_to_references, "rb") as f:
#         references = pickle.load(f)
#     metadata = utils.get_file_metadata(path_to_references)
#     return metadata\

# @config.when(loading_from=LoadingFrom.LOCAL)
@datasaver()
def bibtex_articles_with_references(articles: list[article.PubMedArticle], loaded_references: dict, saving_directory: Path) -> dict:
    """Convert pymedx articles to BibTeX format with references in Scopus format"""
    bibtex_entries = []
    # append each entry
    for i, art in enumerate(articles):
        pubmed_id = art.pubmed_id or ""
        title = art.title or ""
        authors = art.authors or []
        journal = art.journal if hasattr(art, 'journal') else ""
        pub_date = art.publication_date or pd.NaT
        if not isinstance(pub_date, datetime):
            pub_date = dateutil.parser.parse(pub_date)
        abstract = art.abstract or ""
        doi = art.doi.lower() if art.doi else ""
        keywords = art.keywords if hasattr(art, 'keywords') else []
        author_str = ", ".join([f"{author['lastname']}, {author['firstname'][0]}" for author in authors]) if authors else ""
        
        # Get references for this article based on DOI
        references_str = ""
        if doi in loaded_references:
            references_list = loaded_references[doi]
            references_str = "; ".join(references_list)

        # Build BibTeX entry in Scopus format
        bibtex_parts = [f"@ARTICLE{{PMID{pubmed_id},"]
        bibtex_parts.append(f"author={{{author_str}}},")
        bibtex_parts.append(f"title={{{title}}},")
        bibtex_parts.append(f"journal={{{journal}}},")
        if pub_date and hasattr(pub_date, 'year'):
            bibtex_parts.append(f"year={{{pub_date.year}}},")
        bibtex_parts.append(f"doi={{{doi}}},")
        if abstract:
            bibtex_parts.append(f"abstract={{{abstract}}},")
        if keywords:
            bibtex_parts.append(f"keywords={{{'; '.join(keywords)}}},")
        if references_str:
            bibtex_parts.append(f"references={{{references_str}}},")
        bibtex_parts.append("}")
        
        bibtex = "\n".join(bibtex_parts)
        bibtex_entries.append(bibtex)

    #saving
    bibtex_content = "\n\n".join(bibtex_entries)

    path = saving_directory / 'pubmed_results.bib'
    with open(path, 'w') as f:
        f.write(bibtex_content)
    metadata = utils.get_file_metadata(path)
    return metadata

if __name__ == "__main__":

    outputs = ["bibtex_articles_with_references"]
    
    inputs = dict(
            pickled_file_path=PROCESSED_DATA_PATH/'updated_references.pickle',
            saving_directory = PROCESSED_DATA_PATH/"bibtex_with_references",
            )
    
    #import oreder matters!
    import motor_learning_network.get_pubmed_dataset
    import motor_learning_network.get_references
    import __main__
    dr = (
        driver.Builder()
        .with_modules(motor_learning_network.get_pubmed_dataset,
                      motor_learning_network.get_references,
                      __main__)
        .with_config(
            dict(
                loading_from=LoadingFrom.LOCAL
            )
        )
        .build()
        )

    dr.validate_execution(outputs, inputs=inputs)

    # dr.execute(outputs,
    #             inputs=inputs,
    # )
    
    dr.visualize_execution(outputs,
                        inputs=inputs,
                        output_file_path=FIGURES_PATH/f"{__file__}.png"
                        )

    dr.display_all_functions(FIGURES_PATH/f"{__file__}_all_functions.png",keep_dot=True)
