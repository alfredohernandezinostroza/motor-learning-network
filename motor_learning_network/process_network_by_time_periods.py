import leidenalg
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import cdlib
import pickle
import igraph as ig
import sys
from hamilton.function_modifiers import dataloader, datasaver, value, source, parameterize, inject
from hamilton.io import utils
from hamilton_sdk import adapters
from hamilton import driver
from pathlib import Path
import logging
from tqdm.contrib.logging import logging_redirect_tqdm
from tqdm import tqdm
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

year_ranges = (1960,1980,2000,2026)
citation_networks_output_names = [f"citation_network_until_{year}" for year in year_ranges]
resolutions = [round( i * 0.0001, 4) for i in range(1, 10)] #[0.001, ..., 0.040]
resolutions.extend([round( i * 0.001, 3) for i in range(1, 40)]) #[0.001, ..., 0.040]
# resolutions.extend([round(i * 0.01, 2) for i in range(4, 10)]) #[0.03, ..., 0.09]
# resolutions.extend([round(i * 0.1, 1) for i in range(1, 10)]) #[0.1, ..., 0.9]
logger.info(resolutions)
#####################
##  Aux Functions  ##
#####################

##################
##     Main     ##
##################
def _main() -> int:

    UI_CONFIG = adapters.HamiltonTracker(
        project_id=DEFAULT_UI_PROJECT_ID,
        username=DEFAULT_UI_USERNAME,
        dag_name=CURRENT_FILE_NAME,
        tags={"environment": "DEV", "team": TEAM_NAME, "version": "0.1"},
    )

    ########################
    ## Inputs and Outputs ##
    ########################
    inputs = (dict(
        resolutions=resolutions,
        seed=0,
        n_iterations=10,
        # input paths
        citation_network_path=PROCESSED_DATA_PATH/"citation_network_without_layout_updated_citations.graphml",
        clean_unified_database_path=PROCESSED_DATA_PATH / "clean_unified_database.parquet",
    ) 
)    
    outputs = [f"save_{name}" for name in citation_networks_output_names]
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
    dr.display_all_functions(FIGURES_PATH/f"{CURRENT_FILE_NAME}_all_functions.png",keep_dot=True,deduplicate_inputs=True)
    dr.visualize_execution(outputs, inputs=inputs,output_file_path=FIGURES_PATH/f"{CURRENT_FILE_NAME}.png",keep_dot=False,deduplicate_inputs=True)

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

def citation_network_filtered_by_degree(citation_network: ig.Graph) -> ig.Graph:
    degrees = citation_network.degree(mode='all',loops=False)
    nodes_to_delete = [v.index for v, d in zip(citation_network.vs, degrees) if d < 5]
    citation_network.delete_vertices(nodes_to_delete)
    logger.info(f"Cutoff: < 5 degrees")
    logger.info(f"Deleted {len(nodes_to_delete)} nodes.")
    logger.info(f"Nodes remaining in graph: {citation_network.vcount()}")
    return citation_network

@parameterize(**{citation_network_name: {"year_limit": value(year_limit)} for citation_network_name,year_limit in zip(citation_networks_output_names,year_ranges)})
def filter_by_year_range(citation_network_filtered_by_degree: ig.Graph, year_limit: int) -> ig.Graph:
    valid_vertices = [
        v.index for v in citation_network_filtered_by_degree.vs 
        if v.attributes()['year'] <= year_limit
    ]
    return citation_network_filtered_by_degree.induced_subgraph(valid_vertices)


@parameterize(**{f"{citation_network_name}_with_communities": {"citation_network": source(citation_network_name)} 
                 for citation_network_name in citation_networks_output_names})
def add_leiden_cpm_communities_to_graph(
    citation_network: ig.Graph, 
    resolutions: list[float], 
    n_iterations: int, 
    seed: int
) -> ig.Graph:
    """Detect communities in the citation network using the Leiden algorithm and the constant potts model.
    
    Args:
        citation_network: Directed igraph citation network.
        resolutions: A list of resolution parameters for the Leiden algorithm.
            Higher values yield more, smaller communities; lower values
            yield fewer, larger communities.
        n_iterations: Number of iterations to run the Leiden algorithm.
        seed: Random seed for reproducibility.

    Returns:
        The input igraph graph with new vertex attributes containing the community IDs
        for each resolution evaluated.
    """
    logger.info(
        f"Running Leiden on directed graph ({citation_network.vcount()} vertices, "
        f"{citation_network.ecount()} edges) for {len(resolutions)} resolutions."
    )

    for res in tqdm(resolutions, desc="Leiden resolutions"):
        with logging_redirect_tqdm():
            logger.info(f"Running Leiden with resolution={res}")
            partition = leidenalg.find_partition(
                citation_network,
                leidenalg.CPMVertexPartition, # constant potts model
                resolution_parameter=res,
                initial_membership=None,
                weights=None,
                node_sizes=None,
                seed=seed,
                n_iterations=n_iterations
            )
            attr_name = f"cpm_communities_at_res={res}"
            # partition.membership returns a list of community IDs ordered by vertex index
            citation_network.vs[attr_name] = partition.membership
            logger.info(f"Leiden detected {len(partition)} communities at resolution={res}.")    
    return citation_network

@datasaver()
@parameterize(**{f"save_{citation_network_name}": {
                    "citation_network": source(f"{citation_network_name}_with_communities"),
                    "save_path": value(GRAPH_LEVEL_DATA_PATH/citation_network_name) ,
                    } 
                    for citation_network_name in citation_networks_output_names
                })
def save_citation_network_as_graphml(citation_network: ig.Graph, save_path: Path) -> dict:
    """Saves citation network as a graphml file."""
    citation_network.write(save_path.with_suffix(".graphml"))
    metadata = utils.get_file_metadata(save_path)
    return metadata

if __name__ == "__main__":
    sys.exit(_main())