import duckdb
import sys
from hamilton_sdk import adapters
from hamilton import driver
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
    inputs = dict()
    outputs = []
    import __main__
    dr = (
        driver.Builder()
        .with_modules(__main__)
        # .with_config()
        # .with_cache()
        # .with_adapters(UI_CONFIG)
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

if __name__ == "__main__":
    sys.exit(_main)