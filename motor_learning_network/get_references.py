from enum import Enum
from tqdm import tqdm
import time
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
class ReferencesSource(Enum):
    OPENCITATIONS = 0
    CROSSREF = 1

REFERENCES_SOURCE = ReferencesSource.OPENCITATIONS

CURRENT_FILE_NAME = Path(__file__).stem
hamilton.log_setup.setup_logging(logging.INFO)

logger = logging.getLogger(__name__)

EXECUTE = True
if EXECUTE:
    logger.info("Executing the DAG!")

CURRENT_FILE_NAME = Path(__file__).stem
UI_CONFIG = adapters.HamiltonTracker(
    project_id=DEFAULT_UI_PROJECT_ID,
    username=DEFAULT_UI_USERNAME,
    dag_name=CURRENT_FILE_NAME,
    tags={"environment": "DEV", "team": TEAM_NAME, "version": "0.1"},
)
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
        - saving_path: directory where the resulting references will be loaded
        - references_file_path: (if exist) path of the references pickle file
        when references_file_path exist, the references will be loaded 
    """
    ########################
    ## Inputs and Outputs ##
    ########################
    references_file_path  = PROCESSED_DATA_PATH/'references_opencitations.parquet'
    error_references_file_path  = PROCESSED_DATA_PATH/"error_references_opencitations.parquet"
    inputs = dict(
            database_path=PROCESSED_DATA_PATH/'clean_unified_database.parquet',
            references_file_path = references_file_path,
            error_references_file_path = error_references_file_path,
                  )
    outputs = ["save_references_from_open_citations", "save_error_references_from_open_citations"]
    # outputs = ["save_references", "save_error_references"]

    #the DAG wil change according to if we have already have results saved on disk
    if references_file_path.is_file():
        references_on_disk = True
    else:
        references_on_disk = False

    logger.info(f"References on disk: {references_on_disk}")
    import __main__
    dr = (
        driver.Builder()
        .with_modules(__main__)
        .with_config(
            dict(
                references_on_disk=references_on_disk,
                references_source=REFERENCES_SOURCE
            )
        )
        # .with_cache()
        .with_adapters(UI_CONFIG)
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
    if EXECUTE:
        dr.execute(outputs, inputs=inputs)
    return 0

#########################
##    DAG Definition   ##
#########################

def doi(database: pd.DataFrame) -> pd.Series:
    return database['doi']

def database(database_path: Path) -> pd.DataFrame:
    db = pd.read_parquet(database_path)
    # Convert all list columns back to Python lists
    list_columns = ['keywords', 'authors']  # specify your list columns
    for col in list_columns:
        db[col] = db[col].apply(tuple)
    return db

def cleaned_dois(doi: pd.Series) -> pd.Series:
    """Clean dois. Modify according to your cleaning needs"""
    if isinstance(doi, pd.DataFrame): #this shouldn't be here, but apparently there's a bug with extract fields
        doi = doi['doi']
    cleaned_dois = doi.dropna()
    cleaned_dois = cleaned_dois[cleaned_dois != "UNKNOWN"]
    cleaned_dois = cleaned_dois.str.lower()
    return cleaned_dois

@config.when(references_on_disk=True)
@dataloader()
def loaded_references_df(references_file_path: Path) -> tuple[pd.DataFrame, dict]:
    logger.info(f"Loading references from {references_file_path}...")
    loaded_references = pd.read_parquet(references_file_path)
    loaded_references = loaded_references
    return loaded_references, utils.get_file_metadata(references_file_path)

@config.when(references_on_disk=True)
def loaded_references(loaded_references_df: pd.DataFrame) -> pd.Series:
    loaded_references = loaded_references_df['citing_doi'].str.lower()
    return loaded_references

@config.when(references_on_disk=True)
@dataloader()
def loaded_error_references(error_references_file_path: Path) -> tuple[pd.Series, dict]:
    logger.info(f"Loading references from {error_references_file_path}...")
    if error_references_file_path.suffix == ".parquet":
        loaded_error_references = pd.read_parquet(error_references_file_path)
        loaded_error_references = loaded_error_references['error_doi']
    else:
        raise ValueError("Accepted file formats are pickle and parquet.")
    return loaded_error_references, utils.get_file_metadata(error_references_file_path)
    
@config.when(references_on_disk=True)
def dois_to_query__with_loaded_references(cleaned_dois: pd.Series, loaded_references: pd.Series, loaded_error_references: pd.Series) -> pd.Series:
    query_dois = cleaned_dois[~cleaned_dois.isin(loaded_references)] #only query DOIs that are not already in the loaded references
    if (set(loaded_error_references) != set(query_dois)):
        logger.warning("The set of dois to be queried are not identical to error dois!")
    logger.info(f"Querying {len(query_dois)} out of {len(cleaned_dois)} dois due to the rest already being in the database!")
    return query_dois

@config.when(references_on_disk=False)
def dois_to_query__all_dois(cleaned_dois: pd.Series) -> pd.Series:
    return cleaned_dois

# @cache(format='pickle')
# def fetch_references_with_opencitations(dois_to_query: pd.Series) -> tuple[dict[str,list[str]],list[str]]:
#     fetched_references = {}
#     error_references = []
#     api_call = "https://api.opencitations.net/index/v2/references/doi:{doi}"
#     HTTP_HEADERS = {"authorization": OPENCITATIONS_ACCESS_TOKEN}
#     for doi in dois_to_query:
#         result = requests.get(api_call.format(doi=doi), headers=HTTP_HEADERS)
#         if result.status_code == 200:
#             result_df = pd.DataFrame.from_dict(json.loads(result.content))
#             result_df['doi'] = result_df['cited'].str.extract(r'doi:([\S]+)')
#             fetched_references[doi] = result_df
#         else:
#             error_references.append("doi")
#         time.sleep(0.5)
#     return fetched_references, error_references
@config.when(references_source=ReferencesSource.OPENCITATIONS)
@unpack_fields('fetched_references_df', "error_references_df")
def fetch_references_with_opencitations(dois_to_query: pd.Series) -> tuple[pd.DataFrame,pd.DataFrame]:
    """
    Fetch citation data from OpenCitations API using DOIs.
    
    Args:
        dois_to_query: Series of DOIs to query
    
    Returns:
        tuple of (fetched_references_df, error_references_df)
        - fetched_references_df: columns [citing_omid, citing_doi, cited_dois, cited_omids]
        - error_references_df: columns [error_omid, error_doi]
    """
    fetched_references = []
    error_references = []
    api_call = "https://api.opencitations.net/index/v2/references/doi:{doi}"
    HTTP_HEADERS = {"authorization": OPENCITATIONS_ACCESS_TOKEN}
    
    for doi in tqdm(dois_to_query):
        try:
            result = requests.get(api_call.format(doi=doi), headers=HTTP_HEADERS)
            if result.status_code == 200:
                try:
                    result_df = pd.DataFrame.from_dict(json.loads(result.content))
                    if not result_df.empty:
                        # Extract OMID from the 'citing' field (the paper we queried)
                        citing_omid = result_df['citing'].iloc[0] if 'citing' in result_df.columns else None
                        if citing_omid:
                            citing_omid = pd.Series([citing_omid]).str.extract(r'omid:([\S]+)', expand=False).iloc[0]
                        
                        # Extract cited DOIs and OMIDs from the 'cited' field
                        result_df['cited_doi'] = result_df['cited'].str.extract(r'doi:([\S]+)', expand=False)
                        result_df['cited_omid'] = result_df['cited'].str.extract(r'omid:([\S]+)', expand=False)
                        
                        # Aggregate into lists
                        cited_dois = result_df['cited_doi'].dropna().tolist()
                        cited_omids = result_df['cited_omid'].dropna().tolist()
                        
                        fetched_references.append({
                            'citing_omid': citing_omid,  # OMID extracted from API response
                            'citing_doi': doi,            # DOI we used to query
                            #these ones are tuples because we need them hashable
                            'cited_dois': tuple(cited_dois),     # DOIs it cites
                            'cited_omids': tuple(cited_omids)    # OMIDs it cites
                        })
                    else:
                        # Empty response - no citations found
                        fetched_references.append({
                            'citing_omid': None,
                            'citing_doi': doi,
                            'cited_dois': (),
                            'cited_omids': ()
                        })
                except Exception as e:
                    error_message = f"Error processing of {doi}: {e}"
                    logger.warning(error_message)
                    error_references.append({
                        'error_omid': None,
                        'error_doi': doi,
                        'error_message': error_message
                    })
            else:
                error_references.append({
                    'error_omid': None,
                    'error_doi': doi,
                    'error_message': f'got status code {result.status_code}  during fetch '
                })
        except Exception as e:
            error_message = f"Error during retrieval of {doi}: {e}"
            logger.warning(error_message)
            error_references.append({
                'error_omid': None,
                'error_doi': doi,
                'error_message': error_message
            })
            
        time.sleep(0.5)
    
    # Convert to DataFrames
    fetched_references_df = pd.DataFrame(fetched_references)
    error_references_df = pd.DataFrame(error_references)
    return fetched_references_df, error_references_df

@datasaver()
@config.when(references_source=ReferencesSource.OPENCITATIONS)
def save_error_references_from_open_citations(error_references_file_path: Path, error_references_df: pd.DataFrame) -> dict:
    error_references_df.to_parquet(error_references_file_path)
    metadata = utils.get_file_metadata(error_references_file_path)
    return metadata

@datasaver()
@config.when(references_source=ReferencesSource.OPENCITATIONS)
def save_references_from_open_citations(references_file_path: Path, fetched_references_df: pd.DataFrame, loaded_references_df: pd.DataFrame = pd.DataFrame()) -> dict:
    if not loaded_references_df.empty:
        fetched_references_df = pd.concat([loaded_references_df, fetched_references_df])
    fetched_references_df.to_parquet(references_file_path)
    metadata = utils.get_file_metadata(references_file_path)
    return metadata

@cache(format='pickle')
@unpack_fields('fetched_references', "error_references")
@config.when(references_source=ReferencesSource.CROSSREF)
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

@config.when(references_source=ReferencesSource.CROSSREF)
@datasaver()
def save_error_references_from_crossref(error_references: list[str], saving_path: Path) -> dict:
    filename = "error_references.txt"
    with open(saving_path / filename, "w") as f:
        for doi in error_references:
            f.write(f"{doi}\n")
    metadata = utils.get_file_metadata(saving_path / filename)
    return metadata
    

@config.when(references_on_disk=True, references_source=ReferencesSource.CROSSREF)
@datasaver()
def save_references_from_crossref__with_loaded_references(fetched_references: dict[str,list[str]], loaded_references: dict[str,list[str]], saving_path: Path) -> dict:
    all_references = fetched_references | loaded_references #merging both dictionaries into one
    filename = "references.pickle"
    with open(saving_path / filename,"wb") as f:
        pickle.dump(all_references, f, pickle.HIGHEST_PROTOCOL)
    metadata = utils.get_file_metadata(saving_path / filename)
    return metadata

@config.when(references_on_disk=False, references_source=ReferencesSource.CROSSREF)
@datasaver()
def save_references_from_crossref__fetched(fetched_references: dict[str,list[str]], saving_path: Path) -> dict:
    filename = "references.pickle"
    with open(saving_path / filename,"wb") as f:
        pickle.dump(fetched_references,  f, pickle.HIGHEST_PROTOCOL)
    metadata = utils.get_file_metadata(saving_path / filename)
    return metadata

if __name__ == "__main__":
    sys.exit(_main())
