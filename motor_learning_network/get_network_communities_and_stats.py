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
from motor_learning_network.constants import GRAPH_LEVEL_DATA_PATH, FIGURES_PATH, EMAIL, OPENCITATIONS_ACCESS_TOKEN, DEFAULT_UI_PROJECT_ID, DEFAULT_UI_USERNAME, TEAM_NAME
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
step = 0.1
resolutions = np.arange(0.01, 0.1 + step, step) #[0.01, ..., 0.1]
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
        citation_network_path=GRAPH_LEVEL_DATA_PATH/"citation_network.graphml",
        resolutions=resolutions,
        seed=0,
        n_iterations=10
    )
    outputs = [f"leiden_with_resolution_{resolution}" for resolution in resolutions]
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
            "resolution_parameter": None,
            # "n_iterations": n_iterations,
        },
    )
    logger.info(f"Leiden detected {len(communities.communities)} communities.")
    return communities

@parameterize(communities_per_resolution = {"communities": group([source(f"leiden_with_resolution_{resolution}") for resolution in resolutions])})
def communities_df(communities: )

# @dataloader()
# def clean_unified_database(clean_unified_database_path: Path) -> pd.DataFrame:
#     pass
    
def citation_network_with_attributes(citation_network: ig.Graph, clean_unified_database: pd.DataFrame) -> ig.Graph:
    columns = clean_unified_database.columns.to_list()
    clean_unified_database = clean_unified_database.set_index("doi")
    for col in columns:
        citation_network.vs[col] = clean_unified_database.loc[citation_network.vs["name"], col].tolist()
    citation_network.vs["keywords"] = ["|".join(keywords) if keywords else "" for keywords in citation_network.vs["keywords"]]
    citation_network.vs["authors"] = ["|".join(authors) if authors else "" for authors in citation_network.vs["authors"]]
    return citation_network

# def communities_dataframe(
#     leiden_communities: cdlib.NodeClustering, citation_network_with_attributes: ig.Graph
# ) -> pd.DataFrame:
#     """Convert Leiden community assignments to a tidy DataFrame.

#     Columns:
#         - doi: paper DOI (vertex name)
#         - community_id: integer community index (0-based)
#         - community_size: number of members in the community
#     """
#     doi_names = citation_network.vs["name"]
#     records = []
#     for community_id, members in enumerate(leiden_communities.communities):
#         size = len(members)
#         for vertex_idx in members:
#             records.append(
#                 {
#                     "doi": doi_names[vertex_idx],
#                     "community_id": community_id,
#                     "community_size": size,
#                 }
#             )
#     df = pd.DataFrame(records).sort_values(["community_id", "doi"]).reset_index(drop=True)
#     logger.info(
#         f"Communities dataframe: {len(df)} rows, "
#         f"{df['community_id'].nunique()} unique communities."
#     )
#     return df

@datasaver()
def save_communities(communities_dataframe: pd.DataFrame, communities_path: Path) -> dict:
    """Persist the community assignments as a parquet file."""
    communities_dataframe.to_parquet(communities_path)
    metadata = utils.get_file_metadata(communities_path)
    return metadata

if __name__ == "__main__":
    sys.exit(_main())