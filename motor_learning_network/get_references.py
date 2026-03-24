import sys
from hamilton_sdk import adapters
from hamilton import driver
from hamilton.function_modifiers import dataloader, datasaver, config, cache, extract_columns, unpack_fields
from hamilton.io import utils
from habanero import Crossref
import time
import pickle
from pathlib import Path
import pandas as pd
import logging
from motor_learning_network.constants import PROCESSED_DATA_PATH, FIGURES_PATH, EMAIL, OPENCITATIONS_ACCESS_TOKEN, DEFAULT_UI_PROJECT_ID, DEFAULT_UI_USERNAME, TEAM_NAME
import hamilton.log_setup
import json
import requests

###################
##   Constants   ##
###################

CURRENT_FILE_NAME = Path(__file__).stem
UI_CONFIG = adapters.HamiltonTracker(
    project_id=DEFAULT_UI_PROJECT_ID,
    username=DEFAULT_UI_USERNAME,
    dag_name=CURRENT_FILE_NAME,
    tags={"environment": "DEV", "team": TEAM_NAME, "version": "0.1"},
)
CURRENT_FILE_NAME = Path(__file__).stem
hamilton.log_setup.setup_logging(logging.INFO)

logger = logging.getLogger(__name__)

#####################
##  Aux Functions  ##
#####################

##################
##     Main     ##
##################
def _main() -> int:
    """
    Main function, called only when this file is executed directly.
    It will build the DAG to get the citations for a database with a column that contain the DOIs
    Inputs:
        - database_path: path of the parquet file to be loaded
        - saving_path: directory where the resulting 
    """
    ########################
    ## Inputs and Outputs ##
    ########################
    inputs = dict(
            database_path=PROCESSED_DATA_PATH/'medline_database.parquet',
            saving_path = PROCESSED_DATA_PATH
                  )
    outputs = ["save_references", "save_error_references"]

    references_pickled_file_path  = Path(PROCESSED_DATA_PATH,'references.pickle')
    if references_pickled_file_path.is_file():
        references_on_disk = True
        inputs['references_pickled_file_path'] = references_pickled_file_path
        
    else:
        references_on_disk = False
    logger.info(f"References on disk: {references_on_disk}")
    import __main__
    dr = (
        driver.Builder()
        .with_modules(__main__)
        .with_config(
            dict(
                references_on_disk=references_on_disk
            )
        )
        .with_cache()
        .build()
        )
    
    #######################
    ##   Sanity checks   ##
    #######################
    dr.validate_execution(outputs, inputs=inputs)
    dr.display_all_functions(FIGURES_PATH/f"{CURRENT_FILE_NAME}_all_functions.png",keep_dot=True)
    dr.visualize_execution(outputs, inputs=inputs,output_file_path=FIGURES_PATH/f"{CURRENT_FILE_NAME}.png",keep_dot=False)

    ###################
    ##   Execution   ##
    ###################
    dr.execute(outputs, inputs=inputs)


#########################
##    DAG Definition   ##
#########################
@extract_columns('doi')
def database(database_path: Path) -> pd.DataFrame:
    return pd.read_parquet(database_path)

def cleaned_dois(doi: pd.Series) -> pd.Series:
    """Clean dois. Modify according to your cleaning needs"""
    if isinstance(doi, pd.DataFrame): #this shouldn´t be her, but apparently there's a bug with extract fields
        doi = doi['doi']
    cleaned_dois = doi.dropna()
    cleaned_dois = cleaned_dois[cleaned_dois != "UNKNOWN"]
    cleaned_dois = cleaned_dois.str.lower()
    return cleaned_dois

@config.when(references_on_disk=True)
@dataloader()
def loaded_references(references_pickled_file_path: Path) -> tuple[dict, dict]:
    logger.info(f"Loading references from {references_pickled_file_path}...")
    with open(references_pickled_file_path, 'rb') as f:
        loaded_references = pickle.load(f)
        return loaded_references, utils.get_file_metadata(references_pickled_file_path)

@config.when(references_on_disk=True)
def dois_to_query__with_loaded_references(cleaned_dois: pd.Series, loaded_references: dict) -> pd.Series:
    loaded_references_lower = [k.lower() for k in loaded_references.keys()]
    query_dois = cleaned_dois[~cleaned_dois.isin(loaded_references_lower)] #only query DOIs that are not already in the loaded references
    logger.info(f"Querying {len(query_dois)} out of {len(cleaned_dois)} dois due to the rest already being in the database!")
    return query_dois

@config.when(references_on_disk=False)
def dois_to_query__all_dois(cleaned_dois: pd.Series) -> pd.Series:
    return cleaned_dois
    
def fetch_references_with_opencitations(dois_to_query: pd.Series) -> tuple[dict[str,list[str]],list[str]]:
    fetched_references = {}
    error_references = []
    API_CALL = "https://api.opencitations.net/index/v2/references/doi:10.1186/1756-8722-6-59"
    HTTP_HEADERS = {"authorization": OPENCITATIONS_ACCESS_TOKEN}

    results = requests.get(API_CALL, headers=HTTP_HEADERS)
    json.loads(results.content)

@cache(format='pickle')
@unpack_fields('fetched_references', "error_references")
def fetch_references_with_crossref(dois_to_query: pd.Series) -> tuple[dict[str,list[str]],list[str]]:
    fetched_references = {}
    error_references = []
    cr = Crossref(mailto=EMAIL)
    for i in range(0, len(dois_to_query), 100):
        dois = dois_to_query[i:i+100]
        logger.info(f'Retrieving DOIs from {i} to {i+100}...')
        try:
            res = cr.works(ids=dois.tolist(), warn=True)
        except Exception as e:
            logger.warning(f"An error occurred: {e}. Retrying in 5 seconds...")
            time.sleep(5)
            try:
                res = cr.works(ids=dois.tolist(),)
            except Exception as e2:
                logger.warning(f'Skipping chunk starting at {i} after retry: {e2}')
                continue
        found_dois = set([])
        for refs in res:
            if not refs:
                continue
            current_doi = refs['message']['DOI']
            found_dois.add(current_doi)
            list_of_refs = refs['message'].get('reference', [])
            if list_of_refs:
                list_of_refs = [ref['DOI'] for ref in list_of_refs if 'DOI' in ref] #sometimes the reference does not have a DOI, so we ignore those
            fetched_references[current_doi] = list_of_refs
        error_references.extend([doi for doi in dois if doi not in found_dois])
        time.sleep(0.1)
    return fetched_references, error_references

@datasaver()
def save_error_references(error_references: list[str], saving_path: Path) -> dict:
    filename = "error_references.txt"
    with open(saving_path / filename, "w") as f:
        for doi in error_references:
            f.write(f"{doi}\n")
    metadata = utils.get_file_metadata(saving_path / filename)
    return metadata
    

@config.when(references_on_disk=True)
@datasaver()
def save_references__with_loaded_references(fetched_references: dict[str,list[str]], loaded_references: dict[str,list[str]], saving_path: Path) -> dict:
    all_references = fetched_references | loaded_references #merging both dictionaries into one
    filename = "references.pickle"
    with open(saving_path / filename,"wb") as f:
        pickle.dump(all_references, f, pickle.HIGHEST_PROTOCOL)
    metadata = utils.get_file_metadata(saving_path / filename)
    return metadata

@config.when(references_on_disk=False)
@datasaver()
def save_references__fetched(fetched_references: dict[str,list[str]], saving_path: Path) -> dict:
    filename = "references.pickle"
    with open(saving_path / filename,"wb") as f:
        pickle.dump(fetched_references,  f, pickle.HIGHEST_PROTOCOL)
    metadata = utils.get_file_metadata(saving_path / filename)
    return metadata

if __name__ == "__main__":
    sys.exit(_main())
