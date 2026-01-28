import inspect
import copy
from pymedx import PubMed, article
import pymedx
import pandas as pd
import pybibx
from hamilton.io import utils
from hamilton import driver
from hamilton.function_modifiers import datasaver
import textwrap
from pathlib import Path
from hamilton import lifecycle

DATA_PATH = Path("data","raw")

def articles(query: str) -> list[article.PubMedArticle]:
    pubmed = PubMed(tool="MysTool", email="my@esmail.address")
    results = pubmed.query(query, max_results=17000) 
    return list(results)
    
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
        journal = art.journal or ""
        pub_date = article.publication_date or pd.NaT
        abstract = art.abstract or ""
        doi = art.doi.lower() or ""
        keywords = art.keywords or []
        
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
    for i, article in enumerate(articles):
        pubmed_id = article.pubmed_id or ""
        title = article.title or ""
        authors = article.authors or []
        journal = article.journal or ""
        pub_date = article.publication_date or pd.NaT
        abstract = article.abstract or ""
        doi = article.doi.lower() or ""
        keywords = article.keywords or []
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
    def clear_xml_from_article(article):
        article.xml = None
        return article
    import pickle

    # Saving
    articles = [clear_xml_from_article(copy.deepcopy(article)) for article in articles]
    path = DATA_PATH / 'articles.pkl'
    with open(path, 'wb') as f:
        pickle.dump(articles, f)
    metadata = utils.get_file_metadata(path)
    return metadata


if __name__ == "__main__":
    debug_hook = lifecycle.PDBDebugger(
        node_filter=lambda n, tags: n == 'bibtex', 
        after=True
    )
    query = '("Motor Learning" OR "Skill Acquisition" OR "Motor Adaptation" OR "Motor Sequence Learning" OR "Sport Practice" OR "Motor Skill Learning" OR "Sensorimotor Learning" OR "Motor Memory" OR "Motor Training") AND ("1900/01/01"[Date - Publication] : "2025/12/31"[Date - Publication])'
    import __main__
    dr = (
        driver.Builder()
        .with_modules(__main__)
        # .with_adapters(debug_hook)
        .build()
        )

    outputs = ["bibtex_articles", "medline_articles", "pickled_articles"]
    inputs = dict(query=query)

    dr.execute(outputs,
                inputs=inputs,
    )
    import pickle

    # Loading
    with open(DATA_PATH/'articles.pkl', 'rb') as f:
        loaded_list = pickle.load(f)
    # dr.visualize_execution(outputs,
    #                     inputs=inputs,
    #                     output_file_path='node_tree_get_pubmed.png')
