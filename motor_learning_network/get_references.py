#%%
from hamilton import driver
from hamilton.function_modifiers import dataloader, config, cache, extract_columns
from hamilton.io import utils
from habanero import Crossref
import time
import os 
import pickle
from pathlib import Path
import pandas as pd
import logging
from motor_learning_network.constants  import DATA_PATH, FIGURES_PATH, EMAIL

logging.getLogger("hamilton").setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)

@extract_columns('doi')
def database(database_path: Path) -> pd.DataFrame:
    return pd.read_parquet(database_path)

def cleaned_dois(doi: pd.Series) -> pd.Series:
    """Clean dois. Modify according to your cleaning needs"""
    cleaned_dois = doi[doi['doi'] != "UNKNOWN"]
    cleaned_dois = cleaned_dois.dropna()
    return cleaned_dois

@config.when(references_on_disk=True)
@dataloader()
def loaded_references(pickled_file_path: Path) -> tuple[dict, dict]:
    with open(pickled_file_path, 'rb') as f:
        loaded_references = pickle.load(f)
        return loaded_references, utils.get_file_metadata(pickled_file_path)

@config.when(references_on_disk=True)
def dois_to_query__with_loaded_references(cleaned_dois: pd.Series, loaded_references: pd.Series) -> pd.Series:
    return cleaned_dois[~cleaned_dois.isin(loaded_references)]

@config.when(references_on_disk=False)
def dois_to_query__all_dois(cleaned_dois: pd.Series) -> pd.Series:
    return cleaned_dois
    
@cache(format='pickle')
def references(dois_to_query: pd.Series) -> dict:
    all_references = {}
    error_dois = []
    cr = Crossref(mailto=EMAIL)
    for i in range(0, len(dois_to_query), 100):
        dois = dois_to_query[i:i+100]
        logging.log(f'Retrieving DOIs from {i} to {i+100}...')
        try:
            res = cr.works(ids=dois.tolist(), warn=True)
        except Exception as e:
            logging.warning(f"An error occurred: {e}. Retrying in 5 seconds...")
            time.sleep(5)
            try:
                res = cr.works(ids=dois.tolist(),)
            except Exception as e2:
                logging.warning(f'Skipping chunk starting at {i} after retry: {e2}')
                continue
        for refs in res:
            current_doi = refs['message']['DOI']
            list_of_refs = refs['message'].get('reference', [])
            if list_of_refs:
                dois = [ref['DOI'] for ref in list_of_refs if 'DOI' in ref] #sometimes the reference does not have a DOI, so we ignore those
            else:
                dois = []
            all_references[current_doi] = dois
        time.sleep(0.1)

if __name__ == "_main__":

    inputs = {}
    pickled_file_path  = Path(DATA_PATH,'references.pickle')
    if Path(DATA_PATH,'references.pickle').is_file():
        references_on_disk = True
        inputs['pickled_file_path'] = pickled_file_path
    else:
        references_on_disk = False
    
    import __main__
    dr = (
        driver.Builder()
        .with_modules(__main__)
        # .with_config(
        #     dict(
        #         references_on_disk=references_on_disk
        #     )
        # )
        .with_cache()
        .build()
        )
    inputs = dict(database_path=DATA_PATH/'database.csv')
    outputs = ["references"]
    dr.validate_execution(outputs, inputs=inputs)
    dr.display_all_functions(FIGURES_PATH/"get_references.png",keep_dot=True)
    
# %%
