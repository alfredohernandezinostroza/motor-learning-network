import leidenalg
import numpy as np
import pandas as pd
import cdlib
import pickle
import igraph as ig
import sys
from hamilton.function_modifiers import dataloader, datasaver, value, source, group, parameterize
from hamilton.io import utils
from hamilton_sdk import adapters
from hamilton import driver
from pathlib import Path
import logging
from motor_learning_network.constants import PROCESSED_DATA_PATH, GRAPH_LEVEL_DATA_PATH, FIGURES_PATH, EMAIL, OPENCITATIONS_ACCESS_TOKEN, DEFAULT_UI_PROJECT_ID, DEFAULT_UI_USERNAME, TEAM_NAME
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
resolutions = [round(0.1 + i * 0.1, 2) for i in range(1, 9)] #[0.01, ..., 0.1]
logger.info(resolutions)
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
        resolutions=resolutions,
        seed=0,
        n_iterations=10,
        # input paths
        citation_network_path=GRAPH_LEVEL_DATA_PATH/"citation_network.graphml",
        clean_unified_database_path=PROCESSED_DATA_PATH / "clean_unified_database.parquet",
        #output paths
        clean_unified_database_with_communities_path=GRAPH_LEVEL_DATA_PATH / "clean_unified_database_with_communities_high_res.parquet",
        new_citation_network_path=GRAPH_LEVEL_DATA_PATH / "citation_network_full_high_res.graphml",
    )
    outputs = ["save_citation_network_as_graphml", "save_database_with_communities"]
    # outputs = [f"leiden_with_resolution_{resolution}" for resolution in resolutions]
    import __main__
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
def citation_network(citation_network_path: Path) -> tuple[ig.Graph, dict]:
    citation_network = ig.Graph.Read(citation_network_path)
    metadata = utils.get_file_metadata(citation_network_path)
    return citation_network, metadata

@parameterize(**{f"leiden_with_resolution_{resolution}": {"resolution": value(resolution)} for resolution in resolutions})
def leiden_cpm_communities(citation_network: ig.Graph, resolution: list[float], n_iterations: int, seed: int) -> cdlib.NodeClustering:
    """Detect communities in the citation network using the Leiden algorithm and the constant potts model.
    Args:
        citation_network: Directed igraph citation network.
        resolution: Resolution parameter for the Leiden algorithm.
            Higher values yield more, smaller communities; lower values
            yield fewer, larger communities.

    Returns:
        A cdlib NodeClustering object with the detected communities.
    """
    logger.info(
        f"Running Leiden with resolution={resolution} on directed graph "
        f"({citation_network.vcount()} vertices, {citation_network.ecount()} edges)."
    )

    partition = leidenalg.find_partition(
        citation_network,
        leidenalg.CPMVertexPartition, #constant potts model
        resolution_parameter=resolution,
        initial_membership=None,
        weights=None,
        node_sizes=None,
        seed=seed,
        n_iterations=n_iterations
    )
    coms = [citation_network.vs[x]["name"] for x in partition]
    communities = cdlib.NodeClustering(
        coms,
        citation_network,
        "CPM",
        method_parameters={
            "initial_membership": None,
            "weights": None,
            "node_sizes": None,
            "resolution_parameter": resolution,
            "n_iterations": n_iterations,
        },
    )
    logger.info(f"Leiden detected {len(communities.communities)} communities.")
    return communities


@dataloader()
def clean_unified_database(clean_unified_database_path: Path) -> tuple[pd.DataFrame, dict]:
    db = pd.read_parquet(clean_unified_database_path)
    list_columns = ['authors', 'keywords']
    for col in list_columns:
        db[col] = db[col].apply(tuple)
    return db, utils.get_file_metadata(clean_unified_database_path)

@parameterize(communities_per_resolution_df = {"communities": group(*[source(f"leiden_with_resolution_{resolution}") for resolution in resolutions])})
def communities_per_resolution_df(communities: list[cdlib.NodeClustering]) -> pd.DataFrame:
    doi_to_community_series = [pd.Series(map.to_node_community_map()) for map in communities]
    doi_to_community_df = pd.concat(doi_to_community_series, axis=1)
    doi_to_community_df.columns = [f"cpm_communities_at_res={map.method_parameters['resolution_parameter']}" for map in communities]
    doi_to_community_df = doi_to_community_df.map(lambda x: x[0])
    return doi_to_community_df

def clean_unified_database_with_communities(clean_unified_database: pd.DataFrame, communities_per_resolution_df: pd.DataFrame) -> pd.DataFrame:
    communities_per_resolution_df['doi'] = communities_per_resolution_df.index
    return pd.merge(clean_unified_database, communities_per_resolution_df, on='doi')

def citation_network_with_attributes_and_communities(citation_network: ig.Graph, clean_unified_database_with_communities: pd.DataFrame) -> ig.Graph:
    clean_unified_database_with_communities = clean_unified_database_with_communities.set_index("doi")
    columns = clean_unified_database_with_communities.columns.to_list()
    for col in columns:
        citation_network.vs[col] = clean_unified_database_with_communities.loc[citation_network.vs["name"], col].tolist()
    citation_network.vs["keywords"] = ["|".join(keywords) if keywords else "" for keywords in citation_network.vs["keywords"]]
    citation_network.vs["authors"] = ["|".join(authors) if authors else "" for authors in citation_network.vs["authors"]]
    return citation_network


@datasaver()
def save_citation_network_as_graphml(citation_network_with_attributes_and_communities: ig.Graph, new_citation_network_path: Path) -> dict:
    """Persist the igraph citation network as a pickle file."""
    citation_network_with_attributes_and_communities.write(new_citation_network_path)
    metadata = utils.get_file_metadata(new_citation_network_path)
    return metadata


@datasaver()
def save_database_with_communities(clean_unified_database_with_communities: pd.DataFrame, clean_unified_database_with_communities_path: Path) -> dict:
    """Persist the community assignments as a parquet file."""
    clean_unified_database_with_communities.to_parquet(clean_unified_database_with_communities_path)
    metadata = utils.get_file_metadata(clean_unified_database_with_communities_path)
    return metadata

if __name__ == "__main__":
    sys.exit(_main())