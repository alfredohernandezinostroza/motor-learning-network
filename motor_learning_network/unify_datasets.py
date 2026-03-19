import sys
import pandera.pandas as pa
import hamilton.driver as driver
from hamilton_sdk import adapters
from hamilton.function_modifiers import check_output
import pandas as pd
from motor_learning_network.constants import (
    RAW_DATA_PATH,
    DEFAULT_UI_PROJECT_ID,
    DEFAULT_UI_USERNAME,
    TEAM_NAME,
    FIGURES_PATH,
    PROCESSED_DATA_PATH,
)
from pathlib import Path
from hamilton.io.materialization import from_, to

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


def _main() -> int:
    """
    Main function, called only when this file is executed directly.
    Inputs:
        - Schema for the unified database: it defines the recquired columns
        - Scopus dataframe
        - Web of Science (WOS) dataframe
        - PubMed dataframe
        - EBSCO's Academic Search Ultimate dataframe
    """
    ########################
    ## Inputs and Outputs ##
    ########################
    unified_database_schema = pa.DataFrameSchema(
        dict(
            title=pa.Column(str),
            authors=pa.Column(str),
            abstract=pa.Column(str),
            keywords=pa.Column(str),
            journal=pa.Column(str),
            source_database=pa.Column(str),
            doi=pa.Column(
                str, pa.Check.str_matches(r"^[^A-Z]*$")
            ),  # dois have to be normalized to lowercase
            pubmed_id=pa.Column(pa.Int, nullable=True),
            year=pa.Column(pa.Int, nullable=True),
        ),
        strict=True,  # validation will fail if columns not defined in the schema exist in the DF
        coerce=False,  # validation will NOT parse the columns to fit the schema
    )
    inputs = dict(unified_database_schema=unified_database_schema)
    input_files = [
        from_.parquet(target="scopus_df", path=PROCESSED_DATA_PATH / "scopus_database.parquet"),
        from_.parquet(target="wos_df", path=PROCESSED_DATA_PATH / "wos_database.parquet"),
        from_.parquet(target="pubmed_df", path=PROCESSED_DATA_PATH / "medline_database.parquet"),
        from_.csv(target="ebsco_df", path=RAW_DATA_PATH / "ebsco_ASU_all_until_2025.csv"),
    ]
    outputs = ["merged_dataframes__parquet"]
    output_files = [
        to.parquet(
            id="merged_dataframes__parquet",
            dependencies=["merged_dataframes"],
            path=PROCESSED_DATA_PATH / "unified_database.parquet",
        ),
    ]
    #######################
    ##   Sanity checks   ##
    #######################
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
    ###################
    ##   Execution   ##
    ###################
    dr.execute(outputs, inputs=inputs)
    return 0


def scopus_df_clean(
    scopus_df: pd.DataFrame, unified_database_schema: pa.DataFrameSchema
) -> pd.DataFrame:
    renaming_columns_map = {
        "Title": "title",
        "Authors": "authors",
        "Abstract": "abstract",
        "Author Keywords": "keywords",
        "Source title": "journal",
        "Source": "source_database",
        "DOI": "doi",
        "PubMed ID": "pubmed_id",
        "Year": "year"
    }
    scopus_df = scopus_df.rename(columns=renaming_columns_map)
    scopus_df = scopus_df[list(unified_database_schema.columns.keys())]
    scopus_df["doi"] = scopus_df["doi"].str.lower()
    scopus_df = scopus_df.replace("nan","")
    scopus_df["pubmed_id"] = pd.to_numeric(scopus_df["pubmed_id"]).astype("Int64")
    scopus_df["year"] = pd.to_numeric(scopus_df["year"]).astype("Int64")
    unified_database_schema.validate(scopus_df)
    return scopus_df


def wos_df_clean(
    wos_df: pd.DataFrame, unified_database_schema: pa.DataFrameSchema
) -> pd.DataFrame:
    renaming_columns_map = {
        "TI": "title",
        "AU": "authors",
        "AB": "abstract",
        "DE": "keywords",
        "SO": "journal",
        # "": "source_database", #no source_database column here, it will be added later
        "DI": "doi",
        "PM": "pubmed_id",
        "PY": "year"
    }
    wos_df = wos_df.rename(columns=renaming_columns_map)
    wos_df["source_database"] = "Web of Science"
    wos_df = wos_df[list(unified_database_schema.columns.keys())]
    wos_df["doi"] = wos_df["doi"].str.lower()
    wos_df = wos_df.replace("nan","")
    wos_df["pubmed_id"] = pd.to_numeric(wos_df["pubmed_id"]).astype("Int64")
    # wos_df["year"] = wos_df["year"].str[-4:] # the date format of WoS is of the type: NOV 2 2019
    wos_df["year"] = pd.to_numeric(wos_df["year"]).astype("Int64")
    unified_database_schema.validate(wos_df)
    return wos_df


def pubmed_df_clean(
    pubmed_df: pd.DataFrame, unified_database_schema: pa.DataFrameSchema
) -> pd.DataFrame:
    renaming_columns_map = { #most have the same names, I left them (commented out) just for reference
        # "title": "title",
        "full_author": "authors",
        # "abstract": "abstract",
        "author_keywords": "keywords",
        # "journal": "journal",
        "source": "source_database",
        # "doi": "doi",
        # "pubmed_id": "pubmed_id",
        # "year": "year"
    }
    pubmed_df = pubmed_df.rename(columns=renaming_columns_map)
    pubmed_df = pubmed_df[list(unified_database_schema.columns.keys())]
    unified_database_schema.validate(pubmed_df)
    return pubmed_df


def ebsco_df_clean(
    ebsco_df: pd.DataFrame, unified_database_schema: pa.DataFrameSchema
) -> pd.DataFrame:
    ebsco_df = ebsco_df[unified_database_schema.columns]
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
    sys.exit(_main())
