from hamilton.function_modifiers import (
    dataloader,
    datasaver,
    parameterize,
    parameterize_sources,
    source,
    value,
    group,
)
from hamilton.io import utils
from hamilton_sdk import adapters
from pathlib import Path
from hamilton import driver
from motor_learning_network.constants import (
    RAW_DATA_PATH,
    PROCESSED_DATA_PATH,
    FIGURES_PATH,
    DEFAULT_UI_USERNAME,
    DEFAULT_UI_PROJECT_ID,
    TEAM_NAME,
)
import pandas as pd
import hamilton.log_setup
import logging

WOS_RAW_PATH_CORE = RAW_DATA_PATH / "wos_core_collection"
WOS_RAW_PATH_BIOSIS = RAW_DATA_PATH / "wos_biosis_ci"
WOS_RAW_PATH_KCI = RAW_DATA_PATH / "wos_kci"
CURRENT_FILE_NAME = Path(__file__).stem

UI_CONFIG = adapters.HamiltonTracker(
    project_id=DEFAULT_UI_PROJECT_ID,
    username=DEFAULT_UI_USERNAME,
    dag_name=CURRENT_FILE_NAME,
    tags={"environment": "DEV", "team": TEAM_NAME, "version": "0.1"},
)

hamilton.log_setup.setup_logging(logging.INFO)
logger = logging.getLogger(__name__)

@parameterize(
        scopus_1895_2014= dict(filename=value("scopus_1895_2014.csv")),
        scopus_2015_2025= dict(filename=value("scopus_2015_2025.csv")),
)
def load_dataframe(filename: str, scopus_data_path: Path = RAW_DATA_PATH) -> pd.DataFrame:
    try:
        return pd.read_csv(
            scopus_data_path / filename, sep=",", on_bad_lines="warn"
        )
    except Exception as exception:
        logger.warning("error reading %s: %s", filename, exception)
        return pd.DataFrame()
    
@parameterize(
    merge_databases={
        "loaded_dataframes": group(
            *[source("scopus_1895_2014"), source("scopus_2015_2025")]
        )
    },
)
def merge_databases(loaded_dataframes: list[pd.DataFrame]) -> pd.DataFrame:
    df = pd.concat(loaded_dataframes, ignore_index=True, axis=0)
    return df

@datasaver()
def save_scopus_database(merge_databases: pd.DataFrame) -> dict:
    filename = "scopus_database.parquet"
    merge_databases.astype(str).to_parquet(PROCESSED_DATA_PATH / filename)
    #for now everything is a string, I will parse what is necessary later
    metadata = utils.get_file_metadata(PROCESSED_DATA_PATH / filename)
    return metadata


if __name__ == "__main__":
    outputs = ["save_scopus_database"]
    inputs = {}
    output_files = []
    import __main__
    from hamilton.io.materialization import to, from_
    dr = (
        driver.Builder()
        .with_modules(__main__)
        .with_adapters(UI_CONFIG)
        .build()
        )
    dr.validate_execution(outputs, inputs=inputs)
    dr.display_all_functions(
        FIGURES_PATH / f"{CURRENT_FILE_NAME}_all_functions.png",
        keep_dot=True,
        deduplicate_inputs=True,
    )
    dr.visualize_execution(
        outputs,
        inputs=inputs,
        output_file_path=FIGURES_PATH / f"{CURRENT_FILE_NAME}_diagram.png",
        keep_dot=True,
        deduplicate_inputs=True,
    )
    dr.execute(outputs, inputs=inputs)
