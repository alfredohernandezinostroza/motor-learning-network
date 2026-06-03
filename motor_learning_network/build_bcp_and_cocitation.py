from fa2 import ForceAtlas2
import sys
import pickle
from hamilton_sdk import adapters
from hamilton import driver
from hamilton.function_modifiers import dataloader, datasaver
from hamilton.io import utils
from pathlib import Path
import logging
import pandas as pd
import igraph as ig
from motor_learning_network.constants import (
    GRAPH_LEVEL_DATA_PATH,
    FIGURES_PATH,
    DEFAULT_UI_PROJECT_ID,
    DEFAULT_UI_USERNAME,
    TEAM_NAME,
)
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
##################
##     Main     ##
##################
def _main() -> int:
    ########################
    ##  UI configuration  ##
    ########################

    UI_CONFIG = adapters.HamiltonTracker(
        project_id=DEFAULT_UI_PROJECT_ID,
        username=DEFAULT_UI_USERNAME,
        dag_name=CURRENT_FILE_NAME,
        tags={"environment": "DEV", "team": TEAM_NAME, "version": "0.1"},
    )

    ########################
    ## Inputs and Outputs ##
    ########################
    inputs = dict(
        #loading paths
        citation_network_path=GRAPH_LEVEL_DATA_PATH / "citation_network_with_topics_new.graphml",        
        #saving paths
        bibliographic_coupling_path=GRAPH_LEVEL_DATA_PATH / "bibliographic_coupling", #the format will be added later  
        cocitation_network_path=GRAPH_LEVEL_DATA_PATH / "cocitation_network", #the format will be added later    
    )
    outputs = [
        #with  layout
        # "save_bibliographic_coupling_as_pickle",
        # "save_bibliographic_coupling_as_graphml",
        # "save_cocitation_network_as_pickle",
        # "save_cocitation_network_as_graphml",
        #without layout
        "save_cocitation_network_as_graphml_without_layout",
        "save_bibliographic_coupling_as_graphml_without_layout",
    ]

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
    dr.display_all_functions(
        FIGURES_PATH / f"{CURRENT_FILE_NAME}_all_functions.png", keep_dot=True
    )
    dr.visualize_execution(
        outputs,
        inputs=inputs,
        output_file_path=FIGURES_PATH / f"{CURRENT_FILE_NAME}.png",
        keep_dot=False,
    )

    ###################
    ##   Execution   ##
    ###################
    if EXECUTE:
        dr.execute(outputs, inputs=inputs)
    return 0


#####################
##  Aux Functions  ##
#####################


#########################
##    DAG Definition   ##
#########################

@dataloader()
def citation_network(citation_network_path: Path) -> tuple[ig.Graph, dict]:
    citation_network = ig.Graph.Read(citation_network_path)
    metadata = utils.get_file_metadata(citation_network_path)
    return citation_network, metadata

def bibliographic_coupling_network(citation_network: ig.Graph) -> ig.Graph:
    bibcoupling_scores = citation_network.bibcoupling()
    bg_graph = ig.Graph.Weighted_Adjacency(bibcoupling_scores, mode="undirected", loops=False)
    for attr in citation_network.vertex_attributes():
        bg_graph.vs[attr] = citation_network.vs[attr]
    return bg_graph

def cocitation_network(citation_network: ig.Graph) -> ig.Graph:
    cocitation_scores = citation_network.cocitation()
    cocitation_graph = ig.Graph.Weighted_Adjacency(cocitation_scores, mode="undirected", loops=False)
    for attr in citation_network.vertex_attributes():
        cocitation_graph.vs[attr] = citation_network.vs[attr]
    return cocitation_graph


@datasaver()
def save_bibliographic_coupling_as_graphml_without_layout(bibliographic_coupling_network: ig.Graph, bibliographic_coupling_path: Path) -> dict:
    """Save the igraph citation network as a graphml file before layout."""
    bibliographic_coupling_path = bibliographic_coupling_path.with_name(f"{bibliographic_coupling_path.stem}_without_layout")
    path = bibliographic_coupling_path.with_suffix(".graphml")
    bibliographic_coupling_network.write(path)
    metadata = utils.get_file_metadata(path)
    return metadata

@datasaver()
def save_cocitation_network_as_graphml_without_layout(cocitation_network: ig.Graph, cocitation_network_path: Path) -> dict:
    """Save the igraph citation network as a graphml file before layout."""
    cocitation_network_path = cocitation_network_path.with_name(f"{cocitation_network_path.stem}_without_layout")
    path = cocitation_network_path.with_suffix(".graphml")
    cocitation_network.write(path)
    metadata = utils.get_file_metadata(path)
    return metadata


def bibliographic_coupling_with_layout(bibliographic_coupling_network: ig.Graph) -> ig.Graph:
    logger.info("Calculating layout for bibliographic coupling network with ForceAtlas2")
    forceatlas2 = ForceAtlas2(verbose=True)
    layout = forceatlas2.forceatlas2_igraph_layout(bibliographic_coupling_network.as_undirected(), iterations=500)
    bibliographic_coupling_network.vs["x"] = [coord[0] for coord in layout]
    bibliographic_coupling_network.vs["y"] = [coord[1] for coord in layout]
    return bibliographic_coupling_network

def cocitation_with_layout(cocitation_network: ig.Graph) -> ig.Graph:
    logger.info("Calculating layout for cocitation network with ForceAtlas2")
    forceatlas2 = ForceAtlas2(verbose=True)
    layout = forceatlas2.forceatlas2_igraph_layout(cocitation_network.as_undirected(), iterations=500)
    cocitation_network.vs["x"] = [coord[0] for coord in layout]
    cocitation_network.vs["y"] = [coord[1] for coord in layout]
    return cocitation_network

@datasaver()
def save_bibliographic_coupling_as_pickle(bibliographic_coupling_with_layout: ig.Graph, bibliographic_coupling_path: Path) -> dict:
    """Save the igraph citation network as a pickle file."""
    path = bibliographic_coupling_path.with_suffix(".pickle")
    with open(path, "wb") as f:
        pickle.dump(bibliographic_coupling_with_layout, f, protocol=pickle.HIGHEST_PROTOCOL)
    metadata = utils.get_file_metadata(path)
    return metadata

@datasaver()
def save_cocitation_network_as_pickle(cocitation_with_layout: ig.Graph, cocitation_network_path: Path) -> dict:
    """Save the igraph citation network as a pickle file."""
    path = cocitation_network_path.with_suffix(".pickle")
    with open(path, "wb") as f:
        pickle.dump(cocitation_with_layout, f, protocol=pickle.HIGHEST_PROTOCOL)
    metadata = utils.get_file_metadata(path)
    return metadata

@datasaver()
def save_bibliographic_coupling_as_graphml(bibliographic_coupling_with_layout: ig.Graph, bibliographic_coupling_path: Path) -> dict:
    """Save the igraph citation network as a graphml file."""
    path = bibliographic_coupling_path.with_suffix(".graphml")
    bibliographic_coupling_with_layout.write(path)
    metadata = utils.get_file_metadata(path)
    return metadata

@datasaver()
def save_cocitation_network_as_graphml(cocitation_with_layout: ig.Graph, cocitation_network_path: Path) -> dict:
    """Save the igraph citation network as a graphml file."""
    path = cocitation_network_path.with_suffix(".graphml")
    cocitation_with_layout.write(path)
    metadata = utils.get_file_metadata(path)
    return metadata

if __name__ == "__main__":
    sys.exit(_main())
