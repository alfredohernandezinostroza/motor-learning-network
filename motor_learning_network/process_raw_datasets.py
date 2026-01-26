#%%
import hamilton as hmlt
from hamilton.io import utils
from hamilton.function_modifiers import dataloader, datasaver
import pandas as pd
from pathlib import Path
import networkx as nx
import bokeh.plotting as bh
# from my_utils import get_mesh_terms
def get_mesh_terms() -> str:
    pass

@dataloader()
def raw_scopus(raw_scopus_path: Path) -> tuple[pd.DataFrame, dict]:
    df = pd.read_csv(raw_scopus_path)
    return df, utils.get_file_and_dataframe_metadata(raw_scopus_path, df)
    
@dataloader()
def raw_pubmed(raw_pubmed_path: Path) -> tuple[pd.DataFrame, dict]:
    df = pd.read_csv(raw_pubmed_path)
    return df, utils.get_file_and_dataframe_metadata(raw_pubmed_path, df)

@dataloader()
def raw_web_of_science(raw_web_of_science_path: Path) -> tuple[pd.DataFrame, dict]:
    df = pd.read_csv(raw_web_of_science_path)
    return df, utils.get_file_and_dataframe_metadata(raw_web_of_science_path, df)

def scopus_db(raw_scopus: pd.DataFrame)  -> pd.DataFrame:
    pass

def pubmed_db(raw_pubmed: pd.DataFrame)  -> pd.DataFrame:
    pass

def web_of_science_db(raw_web_of_science: pd.DataFrame)  -> pd.DataFrame:
    """Clean and typed web of science db"""
    pass

def full_db(
        scopus_db: pd.DataFrame,
        pubmed_db: pd.DataFrame,
        web_of_science_db: pd.DataFrame,
)   -> pd.DataFrame:
    """Concatenates 3 databases and removes duplicates"""
    pass


def network_with_communities(full_db: pd.DataFrame) -> nx.graph:
    """Convert to graph and use the Leiden algorithm to find communities"""
    pass

def characteristic_keywords(network_with_communities: nx.graph) -> pd.DataFrame:
    """Get charactericstic keywords of the communities using TD-IDF"""
    pass

@datasaver()
def characteristic_keywords_plot(characteristic_keywords: pd.DataFrame) -> dict:
    #Example figure
    x = [1, 2, 3, 4, 5]
    y = [4, 5, 5, 7, 2]
    output_file_path = Path("reports"/"custom_filename.html")
    bh.output_file(filename=output_file_path, title="Static HTML file")
    p = bh.figure(sizing_mode="stretch_width", max_width=500, height=250)
    p.scatter(x, y, fill_color="red", size=15)
    bh.save(p)
    metadata = utils.get_file_metadata(output_file_path)
    return metadata

def mesh_terms(full_db: pd.DataFrame) -> pd.Series:
    """Gets MeSH terms for each passed DOI"""
    mesh_terms = dict.fromkeys(full_db['doi'].values)
    for i, row in full_db.iterrows():
        mesh_terms[row['doi']] = get_mesh_terms(row['title'],row['authors'],row['doi'])
    return pd.DataFrame.from_dict(mesh_terms, orient='index')

@datasaver()
def save_mesh_terms(mesh_terms: pd.Series) -> dict:
    path = Path("data","mest_terms")
    mesh_terms.to_parquet(path)
    metadata = utils.get_file_metadata(path)
    return metadata

if __name__ == "__main__":
    import __main__
    from hamilton import driver
    
    dr = (
        driver.Builder()
        .with_modules(__main__)
        .build()
    )
    inputs = dict(raw_scopus_path=Path("data","scopus_raw.csv"),
                  raw_pubmed_path=Path("data","pubmed_raw.csv"),
                  raw_web_of_science_path=Path("data","web_of_science_raw.csv"))
    
    outputs = [
        "characteristic_keywords_plot",
        "save_mesh_terms"
        ]
    # dr.execute(outputs, inputs=inputs)
    dr.visualize_execution(outputs,
                            inputs=inputs, show_legend=True,
                            output_file_path="node_tree.png"
                            )

# %%
