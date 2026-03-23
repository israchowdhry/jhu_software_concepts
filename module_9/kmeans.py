"""
Module 9 - Grad Cafe Program Clustering with K-Means

This script loads program data, applies TF-IDF vectorization, reduces
dimensions using PCA, and performs K-Means clustering. It generates
initial clusters, determines an approximate optimal k using the elbow
method, assigns clusters to the dataset, and creates GRE comparison
plots for selected program groups.
"""

from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.feature_extraction.text import TfidfVectorizer

INPUT_FILE = "cleaned_gradcafe.json"
INITIAL_K = 50
OPTIMAL_K = 85
MAX_ITER = 100
N_INIT = 5
RANDOM_STATE = 42
CLUSTER_PCA_COMPONENTS = 50
PLOT_PCA_COMPONENTS = 2


def load_and_prepare_data(filepath: str) -> pd.DataFrame:
    """
    Load the JSON dataset and prepare standardized Program and
    University columns.
    """
    df = pd.read_json(filepath)

    required_columns = ["program_name", "university"]
    for column in required_columns:
        if column not in df.columns:
            raise ValueError(f"Required column '{column}' not found in input JSON.")

    df["program_name"] = df["program_name"].astype(str).str.strip()
    df["university"] = df["university"].astype(str).str.strip()

    df["Program"] = df["program_name"]
    df["University"] = df["university"]

    return df


def print_basic_stats(df: pd.DataFrame) -> None:
    """
    Print the number of rows and unique program names.
    """
    print(f"Number of Entries: {len(df)}")
    print(f"Number of Unique Program Input Names: {df['Program'].nunique()}")


def vectorize_programs(df: pd.DataFrame) -> tuple[TfidfVectorizer, object]:
    """
    Convert program names into TF-IDF features.
    """
    vectorizer = TfidfVectorizer(stop_words="english")
    tfidf_matrix = vectorizer.fit_transform(df["Program"])

    print(f"TF-IDF matrix shape: {tfidf_matrix.shape}")
    print(f"TF-IDF matrix type: {type(tfidf_matrix)}")

    return vectorizer, tfidf_matrix


def reduce_dimensions(
    tfidf_matrix: object,
    n_components: int,
) -> tuple[PCA, object]:
    """
    Reduce TF-IDF features using PCA.
    """
    dense_matrix = tfidf_matrix.toarray()
    pca = PCA(n_components=n_components, random_state=RANDOM_STATE)
    pca_features = pca.fit_transform(dense_matrix)

    print(pca_features.shape)
    print(pca)

    return pca, pca_features


def run_kmeans(features: object, n_clusters: int) -> KMeans:
    """
    Fit a K-Means model to the provided feature matrix.
    """
    model = KMeans(
        n_clusters=n_clusters,
        max_iter=MAX_ITER,
        n_init=N_INIT,
        random_state=RANDOM_STATE,
    )
    model.fit(features)
    return model


def plot_initial_clusters(
    pca_features: object,
    labels: object,
    output_path: str,
) -> None:
    """
    Plot and save the initial clustering result.
    """
    plt.figure(figsize=(12, 8))
    scatter = plt.scatter(
        pca_features[:, 0],
        pca_features[:, 1],
        c=labels,
        alpha=0.7,
    )

    plt.title("Initial K-Means Clustering of Grad Cafe Programs (k=50)")
    plt.xlabel("KMeans Distance Direction 1")
    plt.ylabel("KMeans Distance Direction 2")
    plt.legend(*scatter.legend_elements(), title="Cluster", loc="best")
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()


def save_clustered_dataframe_preview(df: pd.DataFrame, output_path: str) -> None:
    """
    Save the first 100 rows of clustered dataframe data as an image.
    """
    preview_df = df[["Program", "University", "Cluster"]].head(100).copy()

    _, ax = plt.subplots(figsize=(14, 20))
    ax.axis("off")

    table = ax.table(
        cellText=preview_df.values,
        colLabels=preview_df.columns,
        loc="center",
    )

    table.auto_set_font_size(False)
    table.set_fontsize(8)
    table.scale(1.2, 1.2)

    plt.title("Clustered DataFrame Preview (First 100 Rows)")
    plt.tight_layout()
    plt.savefig(output_path, bbox_inches="tight")
    plt.close()


def plot_elbow_method(features: object, output_path: str) -> None:
    """
    Plot inertia values for k from 1 to 100 using the elbow method.
    """
    inertias = []
    k_values = range(1, 101)

    for k in k_values:
        model = KMeans(
            n_clusters=k,
            max_iter=MAX_ITER,
            n_init=N_INIT,
            random_state=RANDOM_STATE,
        )
        model.fit(features)
        inertias.append(model.inertia_)

    plt.figure(figsize=(10, 6))
    plt.plot(k_values, inertias, marker="o", label="Inertia")
    plt.title("Elbow Method for Optimal Number of Clusters")
    plt.xlabel("Number of Clusters (k)")
    plt.ylabel("Inertia")
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()


def find_cluster_by_keyword(df: pd.DataFrame, keyword: str) -> int | None:
    """
    Find the most common cluster for rows whose Program contains
    the given keyword.
    """
    subset = df[df["Program"].str.contains(keyword, case=False, na=False)].copy()

    if subset.empty:
        return None

    return int(subset["Cluster"].mode().iloc[0])


def clean_score_column(df: pd.DataFrame, column_name: str) -> pd.Series:
    """
    Convert a score column to numeric values safely.
    """
    return pd.to_numeric(df[column_name], errors="coerce")


def plot_cluster_score_distribution(
    df: pd.DataFrame,
    cluster_id: int,
    cluster_label: str,
    output_path: str,
) -> None:
    """
    Plot GRE score distribution for a given cluster.
    """
    subset = df[df["Cluster"] == cluster_id].copy()

    subset["GRE_V"] = pd.to_numeric(subset["gre_v_score"], errors="coerce")
    subset["GRE_Q"] = pd.to_numeric(subset["gre_score"], errors="coerce")

    plot_data = [
        subset["GRE_Q"].dropna(),
        subset["GRE_V"].dropna(),
    ]

    plt.figure(figsize=(8, 6))
    plt.boxplot(plot_data, tick_labels=["GRE", "GRE V"])

    plt.title(f"GRE and GRE Verbal Scores for {cluster_label} Majors")
    plt.xlabel("GRE Component")
    plt.ylabel("Score")
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()

def main() -> None:
    """
    Run the full clustering and plotting workflow.
    """
    df = load_and_prepare_data(INPUT_FILE)
    print_basic_stats(df)

    _, tfidf_matrix = vectorize_programs(df)

    # Use higher-dimensional PCA output for clustering
    _, cluster_features = reduce_dimensions(
        tfidf_matrix,
        CLUSTER_PCA_COMPONENTS,
    )

    # Use 2D PCA output only for visualization
    _, plot_features = reduce_dimensions(
        tfidf_matrix,
        PLOT_PCA_COMPONENTS,
    )

    initial_model = run_kmeans(cluster_features, INITIAL_K)
    df["Cluster"] = initial_model.labels_

    plot_initial_clusters(
        plot_features,
        initial_model.labels_,
        "initial_cluster.png",
    )

    save_clustered_dataframe_preview(
        df,
        "clustered_dataFrame.png",
    )

    plot_elbow_method(
        cluster_features,
        "elbow.png",
    )

    final_model = run_kmeans(cluster_features, OPTIMAL_K)
    df["Cluster"] = final_model.labels_

    df.to_csv("grad_cafe_clustered.csv", index=False)

    gre_v_col = "gre_v_score"
    gre_q_col = "gre_score"

    if gre_v_col not in df.columns or gre_q_col not in df.columns:
        print(
            "Required GRE columns not found. "
            "Expected 'gre_v_score' and 'gre_score'."
        )
        return

    philosophy_cluster = find_cluster_by_keyword(df, "philosophy")
    cs_cluster = find_cluster_by_keyword(df, "computer science")

    if philosophy_cluster is not None:
        plot_cluster_score_distribution(
            df,
            philosophy_cluster,
            "Philosophy",
            "philosophy.png",
        )
        print(f"Philosophy cluster: {philosophy_cluster}")
    else:
        print("Could not identify a Philosophy cluster.")

    if cs_cluster is not None:
        plot_cluster_score_distribution(
            df,
            cs_cluster,
            "Computer Science",
            "computer_science.png",
        )
        print(f"Computer Science cluster: {cs_cluster}")
    else:
        print("Could not identify a Computer Science cluster.")

    print("All outputs generated successfully.")


if __name__ == "__main__":
    main()
