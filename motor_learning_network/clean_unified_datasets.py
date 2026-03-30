from pathlib import Path
import pandas as pd
import sys
from hamilton_sdk import adapters
from hamilton import driver
from hamilton.io import utils
from hamilton.function_modifiers import dataloader, datasaver
from pathlib import Path
import logging
from motor_learning_network.constants import PROCESSED_DATA_PATH, FIGURES_PATH, EMAIL, OPENCITATIONS_ACCESS_TOKEN, DEFAULT_UI_PROJECT_ID, DEFAULT_UI_USERNAME, TEAM_NAME
import hamilton.log_setup

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


    ########################
    ## Inputs and Outputs ##
    ########################
    inputs = dict(
        merged_dataframes_path=PROCESSED_DATA_PATH/"unified_database.parquet",
        clean_merged_dataframes_path=PROCESSED_DATA_PATH/"clean_unified_database.parquet"
    )
    outputs = ["save_clean_merged_dataframes"]
    import __main__
    from hamilton.io.materialization import from_, to
    dr = (
        driver.Builder()
        .with_modules(__main__)
        # .with_config()
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

@dataloader()
def unified_database(merged_dataframes_path: Path) -> tuple[pd.DataFrame, dict]:
    db = pd.read_parquet(merged_dataframes_path)
    list_columns = ['authors', 'keywords']
    for col in list_columns:
        db[col] = db[col].apply(tuple)
    return db, utils.get_file_metadata(merged_dataframes_path)

def clean_merged_dataframes(unified_database: pd.DataFrame) -> pd.DataFrame:
    #group entries by doi and fills nulls in groups
    other_columns = unified_database.columns.to_list()
    other_columns.remove('doi')
    filledgroups = unified_database.groupby('doi')[other_columns].apply(lambda x: x.ffill().bfill())
    filledgroups.reset_index(inplace=True)
    filledgroups.set_index("level_1", inplace=True)
    unified_database.loc[:,other_columns] = filledgroups.loc[:,other_columns]
    subset_for_deleting_duplicates = unified_database.columns.to_list()
    unified_database.drop_duplicates(subset=subset_for_deleting_duplicates, inplace=True)
    unified_database.reset_index(drop=True, inplace=True)
    # We might still have duplicated entries with slightly different values, like having the same titles but in caps, etc.
    # First, let's remove every entry that does not have a DOI
    unified_database = unified_database[unified_database["doi"].notnull()]
    # We still have duplicated DOIs:
    unified_database[unified_database.duplicated(subset=['doi'], keep=False)].sort_values(by='doi')
    unified_database[unified_database.duplicated(subset=['doi'], keep='first')].sort_values(by='doi')
    unified_database = unified_database.drop_duplicates(subset=['doi'], keep='first')
    return unified_database

@datasaver()
def save_clean_merged_dataframes(clean_merged_dataframes: pd.DataFrame, clean_merged_dataframes_path: Path) -> dict:
    clean_merged_dataframes.to_parquet(clean_merged_dataframes_path)
    metadata = utils.get_file_metadata(clean_merged_dataframes_path)
    return metadata

if __name__ == "__main__":
    sys.exit(_main())