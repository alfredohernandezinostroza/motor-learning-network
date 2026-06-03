import time
import json
import sys
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from hamilton_sdk import adapters
from hamilton import driver
from pathlib import Path
import logging
from motor_learning_network.constants import PROCESSED_DATA_PATH, FIGURES_PATH, OPENALEX_API_KEY, DEFAULT_UI_PROJECT_ID, DEFAULT_UI_USERNAME, TEAM_NAME
import hamilton.log_setup
import pandas as pd
from hamilton.io import utils
from hamilton.function_modifiers import dataloader, datasaver, unpack_fields
import requests

###################
##   Constants   ##
###################
CURRENT_FILE_NAME = Path(__file__).stem
hamilton.log_setup.setup_logging(logging.INFO)

logger = logging.getLogger(__name__)

EXECUTE = True
if EXECUTE:
    logger.info("Executing the DAG!")

CURRENT_FILE_NAME = Path(__file__).stem

##################
##     Main     ##
##################
def _main() -> int:
    ########################
    ##  UI configuration  ##
    ########################

    UI_CONFIG = adapters.HamiltonTracker(
        project_id=DEFAULT_UI_PROJECT_ID,
        username=DEFAULT_UI_USERNAME,
        dag_name=CURRENT_FILE_NAME,
        tags={"environment": "DEV", "team": TEAM_NAME, "version": "0.1"},
    )

    ########################
    ## Inputs and Outputs ##
    ########################
    inputs = dict(
        #input paths
        references_path = PROCESSED_DATA_PATH/"references_opencitations.parquet",
        # references_path = PROCESSED_DATA_PATH/"updated_references.parquet",
        #outputs paths
        updated_loaded_citations_path = PROCESSED_DATA_PATH/"updated_references.parquet"
    )
    outputs = ["save_missing_citations_openalex_ids",
               "save_missing_citations_openalex_ids_errors",
               "save_updated_loaded_citations"]
    import __main__
    dr = (
        driver.Builder()
        .with_modules(__main__)
        # .with_config()
        # .with_cache()
        #  .with_adapters(UI_CONFIG)
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

#####################
##  Aux Functions  ##
#####################

def _failed_fetch(doi: str, message: str, failed_fetches: dict):
    logger.warning(f'failed on doi {doi}: {message}')
    failed_fetches[doi] = message

#########################
##    DAG Definition   ##
#########################

@dataloader()
def loaded_citations(references_path: Path) -> tuple[pd.DataFrame, dict]:
    citations = pd.read_parquet(references_path)
    metadata = utils.get_file_metadata(references_path)
    return citations, metadata


@unpack_fields("missing_citations_openalex_ids", "failed_fetches")
def get_missing_citations_openalex_ids(loaded_citations: pd.DataFrame) -> tuple[dict, dict]:
    missing_citations = dict()
    failed_fetches = dict()
    dois_with_missing_citations = loaded_citations.loc[loaded_citations["cited_dois"].str.len() == 0,"citing_doi"]
    for doi in tqdm(dois_with_missing_citations):
        with logging_redirect_tqdm():
            time.sleep(0.02)
            try:
                url = f"https://api.openalex.org/works/https://doi.org/{doi}"
                result = requests.get(url, params={"api_key": OPENALEX_API_KEY})
            except Exception as e:
                _failed_fetch(doi, 'Error with request', failed_fetches)
                raise e
            if result.status_code == 200:
                try:
                    fetched_data: dict = json.loads(result.content)
                    citations = fetched_data['referenced_works']
                    if not citations:
                        _failed_fetch(doi, 'referenced_works is empty', failed_fetches)
                        continue
                    citations = [openalex_id.removeprefix('https://openalex.org/') for openalex_id in citations]
                    missing_citations[doi] = citations
                except Exception as e:
                    _failed_fetch(doi, 'Error during fetched data processing', failed_fetches)
                    raise e
            else:
                _failed_fetch(doi, 'status code is not 200', failed_fetches)
    return missing_citations, failed_fetches

@datasaver()
def save_missing_citations_openalex_ids(missing_citations_openalex_ids: dict) -> dict:
    df = pd.DataFrame(list(missing_citations_openalex_ids.items()), columns=["doi", "openalex_ids"])
    df.to_csv(PROCESSED_DATA_PATH/"missing_openalex_ids.csv")
    metadata = utils.get_file_metadata(PROCESSED_DATA_PATH/"missing_openalex_ids.csv")
    return metadata

@datasaver()
def save_missing_citations_openalex_ids_errors(failed_fetches: dict) -> dict:
    df = pd.DataFrame.from_dict(failed_fetches, orient="index").reset_index()
    df.columns = ["citing_doi", "error"]
    df.to_csv(PROCESSED_DATA_PATH/"missing_openalex_ids_errors.csv")
    metadata = utils.get_file_metadata(PROCESSED_DATA_PATH/"missing_openalex_ids_errors.csv")
    return metadata


def missing_citations_dois(missing_citations_openalex_ids: dict) -> dict:
    missing_citations = missing_citations_openalex_ids.copy()
    for doi, citations in tqdm(missing_citations_openalex_ids.items()):
        with logging_redirect_tqdm():
            time.sleep(0.03)
            citations_string = "|".join(citations)
            url = f'https://api.openalex.org/works?filter=openalex:{citations_string}&select=id,doi'
            answer = requests.get(url, params={"api_key": OPENALEX_API_KEY})
            if answer.status_code == 200:
                fetched_data: dict = json.loads(answer.content)
                dois = fetched_data['results']
                dois = [result['doi'] for result in dois]
                dois = [doi.removeprefix("https://doi.org/") for doi in dois if doi]
                missing_citations[doi] = dois
    return missing_citations

def updated_loaded_citations(loaded_citations: pd.DataFrame, missing_citations_dois: dict) -> pd.DataFrame:
    loaded_citations["cited_dois"] = loaded_citations["citing_doi"].map(missing_citations_dois).fillna(loaded_citations["cited_dois"])
    return loaded_citations

@datasaver()
def save_updated_loaded_citations(updated_loaded_citations: pd.DataFrame, updated_loaded_citations_path: Path) -> dict:
    updated_loaded_citations.to_parquet(updated_loaded_citations_path)
    metadata = utils.get_file_metadata(updated_loaded_citations_path)
    return metadata

if __name__ == "__main__":
    sys.exit(_main())