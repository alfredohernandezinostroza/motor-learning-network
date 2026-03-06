from hamilton.function_modifiers import datasaver, parameterize, source, value, group
from hamilton.io import utils
from hamilton_sdk import adapters
from pathlib import Path
from hamilton import driver
from motor_learning_network.constants import RAW_DATA_PATH, PROCESSED_DATA_PATH, FIGURES_PATH, DEFAULT_UI_USERNAME, DEFAULT_UI_PROJECT_ID, TEAM_NAME
import pandas as pd
import hamilton.log_setup
import logging

WOS_RAW_PATH = RAW_DATA_PATH / "wos_until_mid_2025"
CURRENT_FILE_NAME = Path(__file__).stem

UI_CONFIG = adapters.HamiltonTracker(
   project_id=DEFAULT_UI_PROJECT_ID,
   username=DEFAULT_UI_USERNAME,
   dag_name=CURRENT_FILE_NAME,
   tags={"environment": "DEV", "team": TEAM_NAME, "version": "0.1"},
)

hamilton.log_setup.setup_logging(logging.INFO)
logger = logging.getLogger(__name__)

# generate a list of the 45 expected filenames; parameterize will
# create one node per entry in this list.  the name passed to
# `parameterize` must match the argument name in the function
# signature (``filename`` below).  we can also use a simple list
# build a simple list of the 45 expected filenames
FILENAMES_LIST = [f"savedrecs({i}).txt" for i in range(45)]

#cheatsheet:
#{output_node_1:
#  {parameter_to_replace_1: value(literal_value),
#  parameter_to_replace_2: source("upstream_node",
# output_node_2:
#  {parameter_to_replace_1: value(another_literal_value),
#  parameter_to_replace_2: source("another_upstream_node")
#)}
FILE_NAMES = {
    f"dataframe_{i}": {"filename": value(fname)} for i, fname in enumerate(FILENAMES_LIST)
}

@parameterize(**FILE_NAMES)
def load_dataframe(wos_data_path: Path, filename: str) -> pd.DataFrame:
# def loaded_dataframe(wos_data_path: Path, filename: str) -> tuple[pd.DataFrame, dict]:
    """Load a single Web‑of‑Science file.

    ``parameterize`` will create 45 copies of this node with the
    ``filename`` argument filled in from ``FILE_NAMES``.  The nodes
    will be named ``loaded_dataframe_0`` ... ``loaded_dataframe_44`` by
    default.
    """
    try:
        return pd.read_csv(wos_data_path / filename, sep="\t", quoting=3, on_bad_lines='warn') #quoting = 3 => quotes are normal characters
    except Exception as exception: 
        logger.warning("error reading %s: %s", filename, exception)
        return pd.DataFrame()
        
    
#Parameterize try
@parameterize(
        merged_database={
            "loaded_dataframes": group(*[source(f"dataframe_{i}") for i in range(45)])
            }
)
def merge_database(loaded_dataframes: list[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate all of the parameterized dataframes.

    The name of the argument (``loaded_dataframe``) must match the base
    name of the nodes produced by ``@parameterize``; Hamilton will
    collect every value with that prefix into a list for us.
    """
    valid_dfs = [df for df in loaded_dataframes if not df.empty]
    df = pd.concat(valid_dfs, ignore_index=True, axis=0)
    print(f"Length of final db: {len(df)}")
    logger.info(f"Length of final db: {len(df)}")
    return df


@datasaver()
def saved_merged_database(merged_database: pd.DataFrame) -> dict:
    filename = "merged_WOS_database_until_mid_2025.parquet"
    merged_database.astype(str).to_parquet(PROCESSED_DATA_PATH / filename)
    metadata = utils.get_file_metadata(PROCESSED_DATA_PATH / filename)
    return metadata

if __name__ == "__main__":
    print(FIGURES_PATH / f"{CURRENT_FILE_NAME}.png")
    outputs = ["saved_merged_database"]
    inputs = dict(wos_data_path=WOS_RAW_PATH)
    import __main__
    dr = (
        driver.Builder()
        .with_modules(__main__)
        .with_adapters(UI_CONFIG)
        .build()
    )
    dr.validate_execution(outputs, inputs=inputs)
    dr.display_all_functions(FIGURES_PATH/f"{CURRENT_FILE_NAME}_all_functions.png",keep_dot=True,deduplicate_inputs=True)
    dr.visualize_execution(outputs, inputs=inputs,output_file_path=FIGURES_PATH/f"{CURRENT_FILE_NAME}_diagram.png",keep_dot=True,deduplicate_inputs=True)
    dr.execute(outputs, inputs=inputs)