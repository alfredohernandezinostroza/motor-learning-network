import pandas as pd
import cdlib
import pickle
import igraph as ig
import sys
from hamilton.function_modifiers import dataloader, datasaver
from hamilton.io import utils
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
def graph(graph_path: Path) -> tuple[ig.Graph, dict]:
    with open(graph_path, "rb") as f:
        network: ig.Graph = pickle.read(f)
    metadata = utils.get_file_metadata(graph_path)
    return network, metadata

def leiden_cpm_communities(citation_network: ig.Graph, resolutions: list[float]) -> cdlib.NodeClustering:
    """Detect communities in the citation network using the Leiden algorithm.

    Passes the directed graph directly to cdlib, which uses leidenalg under
    the hood and supports directed graphs natively.

    Args:
        citation_network: Directed igraph citation network.
        leiden_resolution: Resolution parameter for the Leiden algorithm.
            Higher values yield more, smaller communities; lower values
            yield fewer, larger communities. Default is 1.0.

    Returns:
        A cdlib NodeClustering object with the detected communities.
    """
    for resolution in resolutions:
        logger.info(
            f"Running Leiden with resolution={resolution} on directed graph "
            f"({citation_network.vcount()} vertices, {citation_network.ecount()} edges)."
        )
        communities = cdlib.algorithms.cpm(
            citation_network,
            weights=None,
            initial_membership=None,
            resolution_parameter=resolution,
        )
        logger.info(f"Leiden detected {len(communities.communities)} communities.")
    return communities


def communities_dataframe(
    leiden_communities: cdlib.NodeClustering, citation_network: ig.Graph
) -> pd.DataFrame:
    """Convert Leiden community assignments to a tidy DataFrame.

    Columns:
        - doi: paper DOI (vertex name)
        - community_id: integer community index (0-based)
        - community_size: number of members in the community
    """
    doi_names = citation_network.vs["name"]
    records = []
    for community_id, members in enumerate(leiden_communities.communities):
        size = len(members)
        for vertex_idx in members:
            records.append(
                {
                    "doi": doi_names[vertex_idx],
                    "community_id": community_id,
                    "community_size": size,
                }
            )
    df = pd.DataFrame(records).sort_values(["community_id", "doi"]).reset_index(drop=True)
    logger.info(
        f"Communities dataframe: {len(df)} rows, "
        f"{df['community_id'].nunique()} unique communities."
    )
    return df

@datasaver()
def save_communities(communities_dataframe: pd.DataFrame, communities_path: Path) -> dict:
    """Persist the community assignments as a parquet file."""
    communities_dataframe.to_parquet(communities_path)
    metadata = utils.get_file_metadata(communities_path)
    return metadata

if __name__ == "__main__":
    sys.exit(_main())