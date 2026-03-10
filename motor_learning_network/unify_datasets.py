import sys
import pandera.pandas as pa
import hamilton.driver as driver
from hamilton_sdk import adapters
from hamilton.function_modifiers import check_output
import pandas as pd
from motor_learning_network.constants import (
    DEFAULT_UI_PROJECT_ID,
    DEFAULT_UI_USERNAME,
    TEAM_NAME,
    FIGURES_PATH,
    PROCESSED_DATA_PATH,
)
from pathlib import Path
from hamilton.io.materialization import from_, to

# Constants
CURRENT_FILE_NAME = Path(__file__).stem
UI_CONFIG = adapters.HamiltonTracker(
    project_id=DEFAULT_UI_PROJECT_ID,
    username=DEFAULT_UI_USERNAME,
    dag_name=CURRENT_FILE_NAME,
    tags={"environment": "DEV", "team": TEAM_NAME, "version": "0.1"},
)


def main() -> int:
    """
    Main function, called only when this file is executed directly.
    Inputs:
        - Schema for the unified database: it defines the recquired columns
        - Scopus dataframe
        - Web of Science (WOS) dataframe
        - PubMed dataframe
        - EBSCO's Academic Search Ultimate dataframe
    """
    unified_database_schema = pa.DataFrameSchema(
        dict(
            title=pa.Column(str),
            author=pa.Column(str),
            abstract=pa.Column(str),
            keywords=pa.Column(str),
            journal=pa.Column(str),
            source_database=pa.Column(str),
            doi=pa.Column(str),
            date=pa.Column(pa.Date),
        ),
        strict=True,  # validation will fail if there are columns not defined in the schema
        coerce=False,  # validation will NOT parse the columns to fit the schema
    )
    inputs = dict(unified_database_schema=unified_database_schema)
    input_files = [
        from_.parquet(
            target="scopus_df", path=PROCESSED_DATA_PATH / "merged_WOS_database.parquet"
        ),
        from_.parquet(target="wos_df", path=PROCESSED_DATA_PATH / "merged_EBSCO_database.parquet"),
        from_.parquet(
            target="pubmed_df", path=PROCESSED_DATA_PATH / "merged_Scopus_database.parquet"
        ),
        from_.parquet(target="ebsco_df", path=PROCESSED_DATA_PATH / "merged_WOS_database.parquet"),
    ]
    outputs = ["merged_dataframes__parquet"]
    output_files = [
        to.parquet(
            id="merged_dataframes__parquet",
            dependencies=["merged_dataframes"],
            path=PROCESSED_DATA_PATH / "unified_database.parquet",
        ),
    ]
    import __main__

    dr = (
        driver.Builder()
        .with_modules(__main__)
        .with_adapters(UI_CONFIG)
        .with_materializers(*input_files, *output_files)
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
        output_file_path=FIGURES_PATH / f"{CURRENT_FILE_NAME}_execution_diagram.png",
        keep_dot=True,
        deduplicate_inputs=True,
    )
    # dr.execute(outputs, inputs=inputs)
    return 0


def scopus_df_clean(
    scopus_df: pd.DataFrame, unified_database_schema: pa.DataFrameSchema
) -> pd.DataFrame:
    scopus_df = scopus_df[unified_database_schema.columns]
    unified_database_schema.validate(scopus_df)
    return scopus_df


def wos_df_clean(
    wos_df: pd.DataFrame, unified_database_schema: pa.DataFrameSchema
) -> pd.DataFrame:
    wos_df = wos_df_clean[unified_database_schema.columns]
    unified_database_schema.validate(wos_df)
    return wos_df


def pubmed_df_clean(
    pubmed_df: pd.DataFrame, unified_database_schema: pa.DataFrameSchema
) -> pd.DataFrame:
    pubmed_df = pubmed_df_clean[unified_database_schema.columns]
    unified_database_schema.validate(pubmed_df)
    return pubmed_df


def ebsco_df_clean(
    ebsco_df: pd.DataFrame, unified_database_schema: pa.DataFrameSchema
) -> pd.DataFrame:
    ebsco_df = ebsco_df_clean[unified_database_schema.columns]
    unified_database_schema.validate(ebsco_df)
    return ebsco_df


def merged_dataframes(
    scopus_df_clean: pd.DataFrame,
    wos_df_clean: pd.DataFrame,
    pubmed_df_clean: pd.DataFrame,
    ebsco_df_clean: pd.DataFrame,
) -> pd.DataFrame:
    merged_dataframes = pd.concat(scopus_df_clean, wos_df_clean, pubmed_df_clean, ebsco_df_clean)
    return merged_dataframes


if __name__ == "__main__":
    sys.exit(main())
