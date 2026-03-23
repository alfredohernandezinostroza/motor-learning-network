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
from typing import List

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

#####################
##  Aux Functions  ##
#####################

##################
##     Main     ##
##################

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
            authors=pa.Column(List[str]),
            abstract=pa.Column(str),
            keywords=pa.Column(List[str]),
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
        to.parquet(
            id="scopus_author_ids__parquet",
            dependencies=["scopus_author_ids"],
            path=PROCESSED_DATA_PATH / "scopus_author_ids.parquet",
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


#########################
##    DAG Definition   ##
#########################
def scopus_author_ids(scopus_df: pd.DataFrame) -> pd.DataFrame:
    """Saves author ids to a dataframe. Some of them will be missing because there rows without the same number of
    ids vs number of rows (this is a problem from Scopus, the missing authors can be added manually, but it would take some time"""
    scopus_author_ids = scopus_df["Author(s) ID"].str.split(
        "; "
    )  # extract the IDs, they could be useful
    author_names_with_ids = pd.concat([scopus_df["authors"], scopus_author_ids], axis=1)
    author_names_with_ids.columns = ["authors", "author_ids"]
    author_names_with_ids = author_names_with_ids[
        author_names_with_ids["authors"].apply(len)
        == author_names_with_ids["author_ids"].apply(len)
    ]  # Filter out rows where list lengths do not match
    author_names_with_ids = author_names_with_ids.explode(
        ["authors", "author_ids"]
    )  # pair each author with corresponding ID
    author_names_with_ids["author_ids"] = pd.to_numeric(
        author_names_with_ids["author_ids"]
    ).astype("Int64")
    return author_names_with_ids


def scopus_df_clean(
    scopus_df: pd.DataFrame, unified_database_schema: pa.DataFrameSchema
) -> pd.DataFrame:
    renaming_columns_map = {
        "Title": "title",
        "Author full names": "authors",
        "Abstract": "abstract",
        "Author Keywords": "keywords",
        "Source title": "journal",
        "Source": "source_database",
        "DOI": "doi",
        "PubMed ID": "pubmed_id",
        "Year": "year",
    }
    scopus_df = scopus_df.rename(columns=renaming_columns_map)
    scopus_df = scopus_df[list(unified_database_schema.columns.keys())]  #keep only the columns defined in the schema
    scopus_df["doi"] = scopus_df["doi"].str.lower()
    scopus_df = scopus_df.replace("nan", "")
    scopus_df["keywords"] = scopus_df["keywords"].str.split('; ')
    scopus_df["pubmed_id"] = pd.to_numeric(scopus_df["pubmed_id"]).astype("Int64")
    scopus_df["year"] = pd.to_numeric(scopus_df["year"]).astype("Int64")
    scopus_df["authors"] = scopus_df["authors"].str.replace(pat=" \(.*?\)", repl="", regex=True)
    scopus_df["authors"] = scopus_df["authors"].str.split("; ")
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
        "PY": "year",
    }
    wos_df = wos_df.rename(columns=renaming_columns_map)
    wos_df["source_database"] = "Web of Science"
    wos_df = wos_df[list(unified_database_schema.columns.keys())]  #keep only the columns defined in the schema
    wos_df["doi"] = wos_df["doi"].str.lower()
    wos_df = wos_df.replace("nan", "")
    wos_df["pubmed_id"] = pd.to_numeric(wos_df["pubmed_id"]).astype("Int64")
    wos_df["year"] = pd.to_numeric(wos_df["year"]).astype("Int64")
    wos_df["keywords"] = wos_df["keywords"].str.replace(pat="<(.*?)>", repl="", regex=True)#some keywords had weird HTML tags on them
    wos_df["keywords"] = wos_df["keywords"].str.split('; ')
    wos_df["authors"] = wos_df["authors"].str.split('; ')
    unified_database_schema.validate(wos_df)
    return wos_df


def pubmed_df_clean(
    pubmed_df: pd.DataFrame, unified_database_schema: pa.DataFrameSchema
) -> pd.DataFrame:
    def _get_authors_from_series(papers: pd.Series) -> str:
        papers_with_parsed_authors = []
        for authors in papers:
            authors_list = []
            for author in authors:
                if author["firstname"] is None:
                    author["firstname"] = ""
                if author["lastname"] is None:
                    author["lastname"] = ""
                new_author = (
                    f"{author['firstname']} {author['lastname']}"
                )
                if new_author == " ":
                    continue
                authors_list.append(new_author)
            papers_with_parsed_authors.append(authors_list)
        return pd.Series(papers_with_parsed_authors)
    renaming_columns_map = {  # most have the same names, I left them (commented out) just for reference
        # "title": "title",
        # "authors": "authors",
        # "abstract": "abstract",
        # "keywords": "keywords",
        # "journal": "journal",
        # "": "source_database", #this one is not presenmt, it will be added manually later
        # "doi": "doi",
        # "pubmed_id": "pubmed_id",
        "publication_date": "year"
    }
    pubmed_df = pubmed_df.rename(columns=renaming_columns_map)
    pubmed_df["source_database"] = "PubMed"
    pubmed_df = pubmed_df[list(unified_database_schema.columns.keys())]  #keep only the columns defined in the schema
    pubmed_df["authors"] = _get_authors_from_series(pubmed_df["authors"])
    pubmed_df["pubmed_id"] = pd.to_numeric(pubmed_df["pubmed_id"]).astype("Int64")
    for idx in pubmed_df.index[pubmed_df["keywords"].isna()]:
        pubmed_df.at[idx, "keywords"] = []
    pubmed_df["keywords"]=pubmed_df["keywords"].map(lambda s: s.tolist() if not isinstance(s, list) else s)
    pubmed_df.loc[pubmed_df["journal"].isna(),"journal"] = ""
    pubmed_df.loc[pubmed_df["doi"].isna(),"doi"] = ""
    pubmed_df["doi"] = pubmed_df["doi"].str.lower()
    pubmed_df["year"] = pubmed_df["year"].str[:4]
    pubmed_df["year"] = pd.to_numeric(pubmed_df["year"]).astype("Int64")
    unified_database_schema.validate(pubmed_df)
    return pubmed_df


def ebsco_df_clean(
    ebsco_df: pd.DataFrame, unified_database_schema: pa.DataFrameSchema
) -> pd.DataFrame:
    
    renaming_columns_map = {
        # "title": "title",
        "contributors": "authors",
        # "abstract": "abstract",
        "subjects": "keywords",
        "source": "journal",
        "longDBName": "source_database",
        # "doi": "doi",
        # "": "pubmed_id", it does not have it, we will have to add an empty column
        "publicationDate": "year",
    }
    ebsco_df = ebsco_df.rename(columns=renaming_columns_map)
    ebsco_df[ "pubmed_id"] = pd.NA
    ebsco_df[ "pubmed_id"] = ebsco_df[ "pubmed_id"].astype("Int64")
    ebsco_df = ebsco_df[list(unified_database_schema.columns.keys())]  #keep only the columns defined in the schema
    ebsco_df["doi"] = ebsco_df["doi"].str.lower()
    ebsco_df["doi"] = ebsco_df["doi"].fillna('')
    ebsco_df = ebsco_df.replace("nan", "")
    ebsco_df["abstract"] = ebsco_df["abstract"].fillna('')
    ebsco_df["keywords"] = ebsco_df["keywords"].str.split('; ')
    for idx in ebsco_df.index[ebsco_df["keywords"].isna()]:
        ebsco_df.at[idx, "keywords"] = []
    ebsco_df["authors"] = ebsco_df["authors"].str.split('; ') #fix this
    ebsco_df["year"] = ebsco_df["year"].apply(str) #first convert year to str
    ebsco_df["year"] = ebsco_df["year"].str[:4] #keep first four digits, which is the year
    ebsco_df["year"] = pd.to_numeric(ebsco_df["year"]).astype("Int64") #convert back t int
    for idx in ebsco_df.index[ebsco_df["authors"].isna()]:
        ebsco_df.at[idx, "authors"] = []
    unified_database_schema.validate(ebsco_df)
    return ebsco_df


def merged_dataframes(
    scopus_df_clean: pd.DataFrame,
    wos_df_clean: pd.DataFrame,
    pubmed_df_clean: pd.DataFrame,
    ebsco_df_clean: pd.DataFrame,
) -> pd.DataFrame:
    merged_dataframes = pd.concat([scopus_df_clean, wos_df_clean, pubmed_df_clean, ebsco_df_clean])
    return merged_dataframes


if __name__ == "__main__":
    sys.exit(_main())
