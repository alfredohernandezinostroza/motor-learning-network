from motor_learning_network.constants import GRAPH_LEVEL_DATA_PATH, FIGURES_PATH, RAW_DATA_PATH
import re
import pandas as pd
import numpy as np
from pathlib import Path
import json 
from collections import defaultdict, Counter
from sklearn.feature_extraction.text import TfidfVectorizer
import matplotlib.pyplot as plt
import scipy.sparse
from typing import Final
#in this version purkinje cell (PC) is manually added as synonym

NORM: Final = 'l2'
# NORM: Final = None
IDF_BIAS: Final = 0.0
SYNONYMS_THRESHOLD: Final = 0.99
citation_network_df = pd.read_parquet(GRAPH_LEVEL_DATA_PATH / "clean_unified_database_with_communities_low_res.parquet")
for RESOLUTION in [round(0.001 + i * 0.001, 3) for i in range(1, 9)]: #[0.01, ..., 0.1]
    OUT_DIR: Final = Path("td-idf") / "per-cluster-as-document" / f"res-{RESOLUTION}-threshold-{SYNONYMS_THRESHOLD}-norm-{NORM}-fixidf-{IDF_BIAS}"
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    MODULARITY_META = {float(i): {"label": f"Community {i} at resolution {RESOLUTION}", "color": "#AAAAAA"} for i in range(31)}

    # MODULARITY_META = {
    #     2: {
    #         "displaylabel": "Basic: Adaptation",
    #         "label": "Basic: Adaptation",
    #         "color": "#9A9CFF",
    #     },
    #     7: {
    #         "displaylabel": "Applied: Feedback and\ntraining scheduling",
    #         "label": "Applied: Feedback and\ntraining scheduling",
    #         "color": "#FF891B",
    #     },
    #     12: {
    #         "displaylabel": "Applied: Ecological Dynamics",
    #         "label": "Applied: Ecological Dynamics",
    #         "color": "#FF6587",
    #     },
    #     15: {
    #         "displaylabel": "Applied: Motivation\nand Attention",
    #         "label": "Applied: Motivation\nand Attention",
    #         "color": "#FC001C",
    #     },
    #     11: {
    #         "displaylabel": "Basic: Sequence Learning",
    #         "label": "Basic: Sequence Learning",
    #         "color": "#0018FF",
    #     },
    #     6: {
    #         "displaylabel": "Basic: Motor Cortex",
    #         "label": "Basic: Motor Cortex",
    #         "color": "#54D3FF",
    #     },
    #     5: {
    #         "displaylabel": "Basic: Basal Ganglia",
    #         "label": "Basic: Basal Ganglia",
    #         "color": "#32AC7C",
    #     },
    #     1: {
    #         "displaylabel": "Basic: Cerebellum",
    #         "label": "Basic: Cerebellum",
    #         "color": "#00A50F",
    #     },
    #     10: {"displaylabel": "Motor Imagery", "label": "Motor Imagery", "color": "#B6D315"},
    # }

    def correct_tfidf(X: scipy.sparse.csr_matrix, vectorizer: TfidfVectorizer):
        X_array = X.toarray()
        wrong_idf = vectorizer.idf_
        corrected_idf = wrong_idf - 1.0 + IDF_BIAS
        tf = np.divide(X_array,wrong_idf)
        tfidf = np.multiply(tf, corrected_idf)
        return scipy.sparse.csr_matrix(tfidf)

    def normalize_keyword(keyword: str) -> str:
        """Normalize a keyword to lowercase."""
        return keyword.lower()

    def split_keywords(keywords_value: str):
        """Split a keywords string into individual keywords. Returns list of cleaned keywords.
        
        Keywords are split only by commas that are NOT inside parentheses or square brackets.
        """
        if keywords_value is None:
            return []
        if pd.isna(keywords_value):
            return []
        s = str(keywords_value).strip()
        if not s:
            return []
        
        # Split by commas that are not inside parentheses or square brackets
        parts = []
        current = []
        paren_depth = 0
        bracket_depth = 0
        for char in s:
            if char == '(':
                paren_depth += 1
                current.append(char)
            elif char == ')':
                paren_depth -= 1
                current.append(char)
            elif char == '[':
                bracket_depth += 1
                current.append(char)
            elif char == ']':
                bracket_depth -= 1
                current.append(char)
            elif char == ',' and paren_depth == 0 and bracket_depth == 0:
                # this is a delimiter comma
                part = ''.join(current).strip()
                if part:
                    parts.append(part)
                current = []
            else:
                current.append(char)
        
        # add remaining part
        if current:
            part = ''.join(current).strip()
            if part:
                parts.append(part)
        
        return parts

    # --- New Function to Load Synonym Data ---
    def load_synonym_data(json_path: Path) -> dict:
        """Load the synonym dictionary from a JSON file."""
        if not json_path.exists():
            print(f"Error: Synonym dictionary file not found at {json_path}. Please create it.")
            return {}
        
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                synonym_dict = json.load(f)
            print(f"Loaded {len(synonym_dict)} synonym groups from {json_path.name}.")
            return synonym_dict
        except Exception as e:
            print(f"Error reading or processing {json_path}: {e}")
            return {}
    # ----------------------------------------

    def load_synonym_map(synonym_dict: dict) -> dict:
        """
        Creates a canonical keyword map where all synonyms point to a single
        chosen representative (the normalized name of the original dictionary key).
        The keys and values in the map are normalized (lowercase).
        """
        canonical_map = {}
        
        for key, values in synonym_dict.items():
            # The key is chosen as the canonical form for that group
            canonical_name = normalize_keyword(key)
            
            # Variants include the key itself and all listed values
            all_variants = [key] + values
            
            for variant in all_variants:
                norm_variant = normalize_keyword(variant)
                # Only set the canonical map if the variant hasn't been mapped yet.
                # This ensures that if 'A': ['B'] and 'C': ['D'] overlap, 
                # the first one encountered sets the canonical name.
                if norm_variant not in canonical_map:
                    canonical_map[norm_variant] = canonical_name
        
        return canonical_map
    # ------------------------------------------
    def calculate_canonical_tfidf(citation_network_df: str, synonym_dict_path: Path):
        # Filtering
        df = citation_network_df
        df = df.drop(df[df['keywords'].isin(["Unknown keywords"])].index)
        df = df[df[f"cpm_communities_at_res={RESOLUTION}"].isin(MODULARITY_META)]
        df = df.dropna().reset_index(drop=True)
        # 2. Load and Prepare Synonym Map
        print(f"Loading synonym data from: {synonym_dict_path}")
        synonym_data = load_synonym_data(synonym_dict_path)
        synonym_data['Purkinje Cell'].extend(['Purkinje Cell ( PC )'])
        synonym_map = load_synonym_map(synonym_data)
        
        # 3. Initialize QA Log
        # Maps: Raw Term -> Canonical Term
        qa_log = {} 
        
        # 4. Rewrite Corpus using Canonical Keywords
        print("Rewriting corpus using canonical keywords...")
        canonical_corpus = {}
        all_canonical_keywords = set([])

        for raw_keywords_str, modularity_class in zip(df['keywords'], df[f"cpm_communities_at_res={RESOLUTION}"]):
            if not canonical_corpus.get(modularity_class):
                canonical_corpus[modularity_class] = []
                
            # a. Split the raw string into individual keywords
            # Use a defaultdict here to track processing per paper
            # terms = split_keywords(raw_keywords_str)
            terms = tuple(raw_keywords_str)
            
            # b. Normalize each term and map it to its canonical form
            canonical_terms = []
            
            for raw_term in terms:
                norm_term = normalize_keyword(raw_term)            
                # Use the canonical map, falling back to the term itself if not found
                canonical_term = synonym_map.get(norm_term, norm_term)
                canonical_terms.append(canonical_term)
                all_canonical_keywords.update([canonical_term])
                
                canonical_corpus[modularity_class].append(canonical_term)
                
                # --- QA Logging ---
                # Log the mapping: Raw Term (original case/form) -> Canonical Term
                # This helps track exactly what came from the GEXF and what it became.
                if raw_term not in qa_log:
                    qa_log[raw_term] = canonical_term
                # Optional: Check for inconsistent mapping (if the same raw_term somehow maps to different canonical_terms)
                elif qa_log[raw_term] != canonical_term:
                    print(f"Warning: Inconsistent canonical mapping detected for raw term '{raw_term}'. Mapped to '{qa_log[raw_term]}' and now '{canonical_term}'.")

                
            # c. Reconstruct the document string using canonical terms, separated by spaces
            # The TfidfVectorizer will treat each space-separated canonical term as a feature (token)
            # canonical_doc = "\t".join(canonical_terms)
            # canonical_corpus[modularity_class].append(canonical_doc)
            
        
        for key, values in canonical_corpus.items():
            canonical_corpus[key] = "\t".join(values)
        canonical_corpus = dict(sorted(canonical_corpus.items()))
        # 5. Save the QA Log file

        qa_log_path = OUT_DIR/"qa_canonical_keyword_mapping.json"
        print(f"\nSaving QA log to: {qa_log_path}")
        
        # Sort for cleaner presentation
        sorted_qa_log = dict(sorted(qa_log.items()))
        
        with open(qa_log_path, 'w', encoding='utf-8') as f:
            json.dump(sorted_qa_log, f, indent=4, ensure_ascii=False)
        print(f"Total unique raw keywords processed and logged: {len(qa_log)}")

        # 5.1 Generate QA report: all unique CANONICAL keywords processed
        # all_keywords = set(canonical_corpus)
        all_keywords_sorted = sorted(all_canonical_keywords)
        qa_path = OUT_DIR / "all_canonical_keywords_processed.txt"
        with open(qa_path, 'w') as f:
            f.write(f"Total unique CANONICAL keywords processed: {len(all_keywords_sorted)}\n")
            f.write("=" * 80 + "\n\n")
            for kw in all_keywords_sorted:  
                f.write(f"{kw.title()}\n")
        print(f"\nQA report saved to: {qa_path}")


        # 6. Apply TfidfVectorizer to the Canonical Corpus
        print("\nFitting TfidfVectorizer to the canonical corpus...")
        
        vectorizer = TfidfVectorizer(
            tokenizer=lambda x: x.split('\t'), 
            token_pattern=None,
            lowercase=False,
            norm=NORM
        )
        
        X = vectorizer.fit_transform(canonical_corpus.values())
        
        X = correct_tfidf(X, vectorizer)
        
        # 7. Output Results (Rest of the script remains the same)
        print("\n--- Results ---")
        
        feature_names = vectorizer.get_feature_names_out()
        print(f"Total unique canonical keywords (features): {len(feature_names)}")
        
        # Show example (first cluster)
        if X.shape[0] > 0:        
            cluster_vector = X[0].toarray().flatten()
            
            scores = pd.Series(cluster_vector, index=feature_names)
            scores = scores[scores > 0].sort_values(ascending=False)
            
            print(f"Highest TD-IDF scores (first cluster):\n{scores.head(10)}")
            print(f"\nSum of TD-IDF scores for the first cluster (normalized L2 norm): {np.sum(cluster_vector**2)}")
        
        return X, vectorizer, df

    def sanitize_filename(name):
        """Sanitizes a string for use as a filename."""
        # Replace newlines with spaces for filename compatibility
        name = str(name).replace('\n', ' ') 
        name = re.sub(r'[\\/*?:"<>|]', "", name) # Remove illegal chars
        return re.sub(r'\s+', '_', name).strip() # Replace spaces with underscores
    # ----------


    def _aggregate_top_scores(X, vectorizer, cluster_ids, MODULARITY_META, top_n=3):
        """
        Helper function to aggregate the top N scores and their metadata for all clusters.
        """
        feature_names = vectorizer.get_feature_names_out()
        combined_scores = []

        for i, cluster_id in enumerate(cluster_ids):
            meta = MODULARITY_META.get(cluster_id, {})
            display_label = meta.get('label', f"Cluster {cluster_id}")
            plot_color = meta.get('color', '#1f77b4')

            # Get scores for this cluster
            cluster_vector = X[i].toarray().flatten()
            scores = pd.Series(cluster_vector, index=feature_names)
            scores = scores[scores > 0].sort_values(ascending=False)
            
            # Get top N scores
            top_n_scores = scores.head(top_n)

            for rank, (keyword, score) in enumerate(top_n_scores.items()):
                combined_scores.append({
                    'cluster_id': cluster_id,
                    'cluster_label': display_label.replace('\n', ' '),
                    'cluster_color': plot_color,
                    'keyword': keyword.title(),
                    'score': score,
                    'rank': rank + 1
                })
                
        return combined_scores

    def save_results(X, vectorizer, df, MODULARITY_META, top_n=20):
        """
        Calculates the TF-IDF scores for each cluster, saves the full results
        to a CSV, and generates a histogram for the top N keywords using 
        MODULARITY_META for titles and colors.
        """
        print("\n--- Generating TF-IDF Histograms and CSVs (with Meta Data) ---")
        
        feature_names = vectorizer.get_feature_names_out()
        
        # 1. Robust Cluster ID Mapping
        # Get the unique cluster IDs found in the DataFrame
        df_cluster_ids = sorted(df[f"cpm_communities_at_res={RESOLUTION}"].unique())
        
        # Use the IDs that were actually processed, assuming they correspond to the rows of X
        if len(df_cluster_ids) != X.shape[0]:
            print(f"Warning: Matrix rows ({X.shape[0]}) do not reliably match unique cluster IDs ({len(df_cluster_ids)}). Falling back to sequential numbering.")
            cluster_ids = list(range(X.shape[0]))
        else:
            cluster_ids = df_cluster_ids


        for i in range(X.shape[0]):
            cluster_id = cluster_ids[i]
            
            # --- LOOKUP META DATA ---
            meta = MODULARITY_META.get(cluster_id, {})
            # Use meta data if available, otherwise use defaults
            display_label = meta.get('label', f"Cluster {cluster_id}")
            plot_color = meta.get('color', '#1f77b4') # Default blue if color not found
            
            # 2. Prepare Scores
            cluster_vector = X[i].toarray().flatten()
            scores = pd.Series(cluster_vector, index=feature_names)
            scores = scores[scores > 0].sort_values(ascending=False)
            
            # 3. Save Full Results to CSV
            # Use the sanitized label from the meta data for the filename if available
            file_label = sanitize_filename(meta.get('label', f'{cluster_id}'))
            csv_path = OUT_DIR / f"cluster_{cluster_id}_{file_label}_tfidf_scores.csv"
            
            df_output = pd.DataFrame({
                "canonical_keyword": scores.index,
                "tfidf_score": scores.values
            })
            df_output["canonical_keyword"] = df_output["canonical_keyword"].str.title()
            df_output.to_csv(csv_path, index=False)
            print(f"Saved TF-IDF scores CSV for {display_label} to: {csv_path.name}")
            
            # 4. Create Histogram (Bar Plot)
            top = df_output.head(top_n)
            
            if top.empty:
                print(f"No TF-IDF scores found for {display_label}, skipping plot.")
                continue
                
            labels = top['canonical_keyword'].tolist()[::-1] # Keywords for Y-axis, reversed
            values = top['tfidf_score'].tolist()[::-1]      # Scores for X-axis, reversed

            # Set up plot size (dynamic height based on number of keywords)
            plt.figure(figsize=(10, max(4, len(labels) * 0.35)))
            
            # Use the color from MODULARITY_META
            plt.barh(labels, values, color=plot_color) 
            
            plt.xlabel('TF-IDF Score')
            # Use the display label from MODULARITY_META for the title
            plt.title(f"{display_label} - Top 20 Cluster-Distinguishing Keywords", loc='center') 
            # plt.title(f"{display_label} Top {top_n} Canonical Keywords", loc='center') 
                
            # Adjust layout to fit long labels
            plt.tight_layout()

            # Save the plot
            png_path = OUT_DIR / f"cluster_{cluster_id}_{file_label}_tfidf_histogram.png"
            plt.savefig(png_path, dpi=150)
            plt.close() # Close the plot figure to free up memory

            print(f"Saved TF-IDF histogram for {display_label} to: {png_path.name}")

        return _aggregate_top_scores(X, vectorizer, cluster_ids, MODULARITY_META, top_n=3)

    def plot_top_three_scores_combined(combined_scores_data, MODULARITY_META):
        """
        Plots the top 3 scores of each cluster with its respective color all in one plot.
        """
        if not combined_scores_data:
            print("No combined scores data to plot.")
            return

        # Create a DataFrame for easier manipulation and sorting
        df_combined = pd.DataFrame(combined_scores_data)
        
        # Sort by cluster label (for grouping) and then by score (for order within group)
        df_combined['sort_label'] = df_combined['cluster_label'].str.split(':').str[-1].str.strip() # Sort by the second part of the label
        df_combined = df_combined.sort_values(by=['sort_label', 'score'], ascending=[False, True])
        df_combined = df_combined.drop(columns=['sort_label'])

        # Create the label for the Y-axis: "Keyword (Cluster Label)"
        df_combined['plot_label'] = df_combined.apply(
            lambda row: f"{row['keyword']} ({row['cluster_label']})", axis=1
        )
        
        labels = df_combined['plot_label'].tolist()
        values = df_combined['score'].tolist()
        colors = df_combined['cluster_color'].tolist()
        
        # Use cluster labels for the legend
        legend_handles = [
            plt.Rectangle((0, 0), 1, 1, fc=meta['color'])
            for cluster_id, meta in sorted(MODULARITY_META.items())
            if cluster_id in df_combined['cluster_id'].unique()
        ]
        legend_labels = [
            meta['label'].replace('\n', ' ')
            for cluster_id, meta in sorted(MODULARITY_META.items())
            if cluster_id in df_combined['cluster_id'].unique()
        ]
        
        # Plotting
        plt.figure(figsize=(12, max(6, len(labels) * 0.4)))
        plt.barh(labels, values, color=colors)
        
        plt.xlabel('TF-IDF Score of cluster as a single document')
        plt.title('Top 3 Canonical Keywords by Cluster', loc='center')
        
        # Add legend
        plt.legend(legend_handles, legend_labels, title="Cluster", loc='lower right', framealpha=0.8)
        
        plt.tight_layout()
        
        png_path = OUT_DIR / "combined_top_3_tfidf_histogram.png"
        plt.savefig(png_path, dpi=150)
        plt.close()
        print(f"\nSaved combined top 3 histogram: {png_path.name}")

    synonym_dict_path = RAW_DATA_PATH / f"keyword_synonyms_{SYNONYMS_THRESHOLD}_with_transitivity.json"
    X_matrix, vectorizer_model, dataframe = calculate_canonical_tfidf(citation_network_df, synonym_dict_path)

    top_three_scores = save_results(X_matrix, vectorizer_model, dataframe, MODULARITY_META)

    # Plot the combined top 3 scores
    plot_top_three_scores_combined(top_three_scores, MODULARITY_META)

    print("Script execution successful for resolution {RESOLUTION}")