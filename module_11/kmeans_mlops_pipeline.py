"""
Module 11 - KMeans MLOps Pipeline with MLflow

This script loads cleaned Grad Cafe program data, vectorizes program names
with TF-IDF, reduces dimensions using PCA, trains a KMeans clustering model,
logs parameters and inertia to MLflow, and saves clustered output.
"""

from __future__ import annotations

import mlflow
import mlflow.sklearn
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.feature_extraction.text import TfidfVectorizer

INPUT_FILE = "cleaned_gradcafe.json"
TRACKING_URI = "http://localhost:8080"
EXPERIMENT_NAME = "Module_11_KMeans"
REGISTERED_MODEL_NAME = "Clustering"

TFIDF_MAX_FEATURES = 1000
PCA_COMPONENTS = 25

KMEANS_PARAMS = {
    "max_iter": 500,
    "n_clusters": 25,
    "n_init": 5,
    "random_state": 42,
}


def load_and_prepare_data(filepath: str) -> pd.DataFrame:
    """
    Load the JSON dataset and prepare program and university columns.
    """
    dataframe = pd.read_json(filepath)

    required_columns = ["program_name", "university"]
    missing_columns = [
        column for column in required_columns if column not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {', '.join(missing_columns)}"
        )

    dataframe["program_name"] = dataframe["program_name"].astype(str).str.strip()
    dataframe["university"] = dataframe["university"].astype(str).str.strip()

    return dataframe


def vectorize_program_names(dataframe: pd.DataFrame) -> tuple[TfidfVectorizer, object]:
    """
    Convert program names into TF-IDF features.
    """
    vectorizer = TfidfVectorizer(
        stop_words="english",
        max_features=TFIDF_MAX_FEATURES,
    )
    tfidf_matrix = vectorizer.fit_transform(dataframe["program_name"])
    return vectorizer, tfidf_matrix


def reduce_dimensions(tfidf_matrix: object) -> tuple[PCA, object]:
    """
    Reduce TF-IDF features using PCA.
    """
    dense_matrix = tfidf_matrix.toarray()
    pca_model = PCA(
        n_components=PCA_COMPONENTS,
        random_state=KMEANS_PARAMS["random_state"],
    )
    reduced_features = pca_model.fit_transform(dense_matrix)
    return pca_model, reduced_features


def train_kmeans(features: object) -> KMeans:
    """
    Fit a KMeans model to the reduced feature matrix.
    """
    model = KMeans(
        n_clusters=KMEANS_PARAMS["n_clusters"],
        max_iter=KMEANS_PARAMS["max_iter"],
        n_init=KMEANS_PARAMS["n_init"],
        random_state=KMEANS_PARAMS["random_state"],
    )
    model.fit(features)
    return model


def save_clustered_output(dataframe: pd.DataFrame, labels: object) -> None:
    """
    Save clustered results to a CSV file.
    """
    output_dataframe = dataframe.copy()
    output_dataframe["cluster"] = labels
    output_dataframe.to_csv("grad_cafe_clustered.csv", index=False)


def log_to_mlflow(
    model: KMeans,
    vectorizer: TfidfVectorizer,
    pca_model: PCA,
) -> None:
    """
    Log parameters, inertia, and models to MLflow.
    """
    mlflow.set_tracking_uri(TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    with mlflow.start_run() as run:
        mlflow.log_params(KMEANS_PARAMS)
        mlflow.log_metric("inertia", float(model.inertia_))
        mlflow.log_param("pca_components", PCA_COMPONENTS)
        mlflow.log_param("tfidf_max_features", TFIDF_MAX_FEATURES)

        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="kmeans_model",
            registered_model_name=REGISTERED_MODEL_NAME,
        )
        mlflow.sklearn.log_model(
            sk_model=vectorizer,
            artifact_path="tfidf_vectorizer",
        )
        mlflow.sklearn.log_model(
            sk_model=pca_model,
            artifact_path="pca_model",
        )

        print(f"Run logged successfully: {run.info.run_id}")
        print(f"Inertia: {model.inertia_:.4f}")


def main() -> None:
    """
    Run the full clustering and MLflow tracking workflow.
    """
    dataframe = load_and_prepare_data(INPUT_FILE)
    vectorizer, tfidf_matrix = vectorize_program_names(dataframe)
    pca_model, reduced_features = reduce_dimensions(tfidf_matrix)
    kmeans_model = train_kmeans(reduced_features)
    save_clustered_output(dataframe, kmeans_model.labels_)
    log_to_mlflow(kmeans_model, vectorizer, pca_model)
    print("Module 11 pipeline completed successfully.")


if __name__ == "__main__":
    main()
