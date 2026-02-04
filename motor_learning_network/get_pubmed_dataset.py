
import dateutil
import inspect
import copy
from pymedx import PubMed, article
import pymedx
import pandas as pd
import pybibx
from hamilton.io import utils
from hamilton import driver
from hamilton.function_modifiers import datasaver, cache, config
import textwrap
from pathlib import Path
from hamilton import lifecycle
import pickle
from datetime import datetime
from enum import Enum
from constants import DATA_PATH

class LoadingFrom(Enum):
    LOCAL = 0
    ONLINE = 1

def _check_if_db_exists():
    path_exists = Path(DATA_PATH, 'articles.pkl').is_file()
    return path_exists

@config.when(loading_from=LoadingFrom.LOCAL)
def articles__local() -> list[article.PubMedArticle]:
    path = DATA_PATH / 'articles.pkl'
    with open(path, 'rb') as f:
        articles = pickle.load(f)
    return articles

@cache(format='pickle')
@config.when(loading_from=LoadingFrom.ONLINE)
def articles__online(query: str) -> list[article.PubMedArticle]:
    pubmed = PubMed(tool="MysTool", email="my@esmail.address")
    results = pubmed.query(query, max_results=17000) 
    results = list(results)
    def clear_xml_from_article(article):
        if hasattr(article, 'xml'):
            article.xml = None
        return article
    results = [clear_xml_from_article(copy.deepcopy(article)) for article in results]
    return results
    
@datasaver()
def medline_articles(articles: list[article.PubMedArticle]) -> dict:
    """Convert pymedx articles to MEDLINE format"""
    def _wrap_medline_field(text: str, width: int = 80) -> str:
        """
        Wrap text for MEDLINE format fields.
        First line starts with field, subsequent lines indented with spaces.
        """
        lines = textwrap.wrap(text, width=width)
        if not lines:
            return ""
        return ("\n      ").join(lines)


    def _create_author_abbr(firstname: str, lastname: str) -> str:
        """Create author abbreviation from first and last name"""
        abbr = lastname if lastname else ""
        if firstname:
            abbr += " " + firstname[0]
        return abbr.strip()
    medline_records = []
    
    for art in articles:
        pmid = art.pubmed_id or ""
        title = art.title or ""
        authors = art.authors or []
        journal = art.journal if hasattr(art, 'journal') else ""
        pub_date = art.publication_date or pd.NaT
        abstract = art.abstract or ""
        doi = art.doi.lower() if art.doi else ""
        keywords = art.keywords if hasattr(art, 'keywords') else []
        
        # Extract year from publication date
        year = ""
        if pub_date:
            if hasattr(pub_date, 'year'):
                year = str(pub_date.year)
            elif isinstance(pub_date, str):
                year = pub_date[:4]
        
        # Build MEDLINE record
        record = []
        
        # Basic identifiers and metadata
        record.append(f"PMID- {pmid}")
        record.append("OWN - NLM")
        record.append("STAT- PubMed")
        if year:
            record.append(f"DA  - {year}")
        
        # Journal information
        if journal:
            record.append(f"TA  - {journal}")
            record.append(f"JT  - {journal}")
        
        # Publication details
        if year:
            record.append(f"DP  - {year}")
        
        # Title
        if title:
            # Wrap title at 80 characters for MEDLINE format
            wrapped_title = _wrap_medline_field(title, 80)
            record.append(f"TI  - {wrapped_title}")
        
        # Authors - format as MEDLINE
        if authors:
            for author in authors:
                if isinstance(author, dict):
                    firstname = author.get('firstname', '')
                    lastname = author.get('lastname', '')
                    full_name = f"{lastname}, {firstname}".strip().rstrip(', ')
                    author_abbr = _create_author_abbr(firstname, lastname)
                elif isinstance(author, str):
                    full_name = author
                    author_abbr = author.split()[-1] if author else ""
                else:
                    continue
                
                record.append(f"FAU - {full_name}")
                record.append(f"AU  - {author_abbr}")
        
        # Abstract
        if abstract:
            wrapped_abstract = _wrap_medline_field(abstract, 80)
            record.append(f"AB  - {wrapped_abstract}")
        
        # DOI and other identifiers
        if doi:
            record.append(f"LID - {doi} [doi]")
        
        # Keywords/MeSH terms
        if keywords:
            for keyword in keywords:
                record.append(f"OT  - {keyword}")
        
        # Language
        record.append("LA  - eng")
        
        # Publication type
        record.append("PT  - Journal Article")
        
        # Add blank line between records
        record.append("")
        
        medline_records.append("\n".join(record))
    
    # Join all records with blank line separator
    medline_content = "\n".join(medline_records)
    
    # Save to file
    path = DATA_PATH / "medline_articles.txt"
    with open(path, 'w') as f:
        f.write(medline_content)
    metadata = utils.get_file_metadata(path)
    return metadata

@datasaver()
def bibtex_articles(articles: list[article.PubMedArticle]) -> dict:
    """Convert pymedx articles to BibTeX format"""
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
        author_str = " and ".join([f"{author['firstname']} {author['lastname']}" for author in authors]) if authors else ""

        bibtex = \
        f"""@ARTICLE{{PMID{pubmed_id},
            title = {{{title}}},
            author = {{{author_str}}},
            journal = {{{journal}}},
            date = {{{pub_date.strftime("%Y-%m-%d") if pub_date else None}}},
            year = {{{pub_date.year if pub_date and hasattr(pub_date, 'year') else None}}},
            abstract = {{{abstract}}},
            keywords = {{{'; '.join(keywords)}}},
            doi = {{{doi}}},
            PMID = {{{pubmed_id}}}
        }}"""
        bibtex_entries.append(inspect.cleandoc(bibtex))

    #saving
    # bibtex_content = textwrap.dedent("\n\n".join(bibtex_entries))
    bibtex_content = "\n".join(bibtex_entries)

    path = DATA_PATH / 'pubmed_results.bib'
    with open(path, 'w') as f:
        f.write(bibtex_content)
    metadata = utils.get_file_metadata(path)
    return metadata

@datasaver()
def pickled_articles(articles: list) -> dict:
    path = DATA_PATH / 'articles.pkl'
    with open(path, 'wb') as f:
        pickle.dump(articles, f)
    metadata = utils.get_file_metadata(path)
    return metadata

if __name__ == "__main__":
    query = '("Motor Learning" OR "Skill Acquisition" OR "Motor Adaptation" OR "Motor Sequence Learning" OR "Sport Practice" OR "Motor Skill Learning" OR "Sensorimotor Learning" OR "Motor Memory" OR "Motor Training") AND ("1900/01/01"[Date - Publication] : "2025/12/31"[Date - Publication])'
    if _check_if_db_exists():
        loading_from = LoadingFrom.LOCAL
    else:
        loading_from = LoadingFrom.ONLINE

    import __main__
    dr = (
        driver.Builder()
        .with_modules(__main__)
        .with_cache()
        .with_config(dict(
            loading_from=loading_from,
        ))
        # .with_adapters(debug_hook)
        .build()
        )

    outputs = ["pickled_articles","bibtex_articles", "medline_articles"]
    inputs = dict(query=query)

    dr.execute(outputs,
                inputs=inputs,
    )
    
    # dr.visualize_execution(outputs,
    #                     inputs=inputs,
    #                     output_file_path='node_tree_get_pubmed.png')
