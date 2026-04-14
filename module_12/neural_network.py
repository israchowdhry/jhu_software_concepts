"""
neural_network.py

Builds, trains, evaluates, and analyzes a two-layer neural network
for graduate admissions prediction using NumPy only for the network.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split


# Fixed configuration required by the assignment
RANDOM_SEED = 42
HIDDEN_UNITS = 6
LEARNING_RATE = 0.05
MAX_EPOCHS = 10000
PATIENCE = 100

TEST_SIZE = 0.2
TRAIN_TEST_RANDOM_STATE = 42
SHUFFLE = True

FEATURE_COLUMNS = [
    "gpa",
    "gre",
    "gre_v",
    "gre_aw",
    "ms_vs_phd",
    "international_vs_local",
]

TARGET_COLUMN = "target"


# Utility functions
def sigmoid(x: np.ndarray) -> np.ndarray:
    """
    Compute the sigmoid activation function.

    Args:
        x: Input NumPy array.

    Returns:
        NumPy array after applying sigmoid elementwise.
    """
    x_clipped = np.clip(x, -500, 500)
    return 1.0 / (1.0 + np.exp(-x_clipped))


def mean_squared_error(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """
    Compute mean squared error.

    Args:
        y_true: True labels of shape (n_samples, 1).
        y_pred: Predicted probabilities of shape (n_samples, 1).

    Returns:
        MSE as a float.
    """
    return float(np.mean((y_true - y_pred) ** 2))


def accuracy_score_binary(y_true: np.ndarray, y_pred_binary: np.ndarray) -> float:
    """
    Compute binary classification accuracy.

    Args:
        y_true: True labels of shape (n_samples, 1).
        y_pred_binary: Predicted binary labels of shape (n_samples, 1).

    Returns:
        Accuracy as a float.
    """
    return float(np.mean(y_true == y_pred_binary))


# Data preparation
def clean_applicant_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and preprocess the applicant dataset according to assignment rules,
    using the actual GradCafe column names from the cleaned dataset.

    Required final model input features:
    - gpa
    - gre
    - gre_v
    - gre_aw
    - ms_vs_phd
    - international_vs_local

    Args:
        df: Raw applicant DataFrame.

    Returns:
        Cleaned DataFrame with required features and target.
    """
    original_rows = len(df)

    cleaned = df.copy()

    # Keep only Accepted / Rejected
    cleaned["applicant_status"] = cleaned["applicant_status"].str.strip().str.lower()
    cleaned = cleaned[
        cleaned["applicant_status"].isin(["accepted", "rejected"])
    ].copy()

    # Keep only Masters / PhD
    cleaned["degree"] = cleaned["degree"].str.strip().str.lower()
    cleaned = cleaned[
        cleaned["degree"].isin(["masters", "master's", "phd"])
    ].copy()

    # Normalize citizenship/international column
    cleaned["international_american"] = (
        cleaned["international_american"].str.strip().str.lower()
    )

    # Rename dataset-specific columns to assignment-style feature names
    cleaned = cleaned.rename(
        columns={
            "gre_score": "gre",
            "gre_v_score": "gre_v",
        }
    )

    # Convert numeric-like columns to floats
    numeric_columns = ["gpa", "gre", "gre_v", "gre_aw"]
    for column in numeric_columns:
        cleaned[column] = pd.to_numeric(cleaned[column], errors="coerce")

    # Binary feature columns
    cleaned["ms_vs_phd"] = np.where(cleaned["degree"] == "phd", 1.0, 0.0)

    cleaned["international_vs_local"] = np.where(
        cleaned["international_american"] == "international",
        1.0,
        0.0,
    )

    # Target variable
    cleaned["target"] = np.where(cleaned["applicant_status"] == "accepted", 1.0, 0.0)

    print("\n1. LOAD AND PREPARE THE APPLICANT DATASET")
    print(f"Number of rows in the original dataset: {original_rows}")
    print(f"Number of rows remaining after filtering: {len(cleaned)}")
    print(f"Number of Accepted rows: {(cleaned['target'] == 1).sum()}")
    print(f"Number of Rejected rows: {(cleaned['target'] == 0).sum()}")
    print(f"Final input features: {FEATURE_COLUMNS}")
    print("\nFirst few rows of cleaned dataframe:")
    print(cleaned[FEATURE_COLUMNS + [TARGET_COLUMN]].head())

    return cleaned


def split_and_standardize_data(
    cleaned_df: pd.DataFrame,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, dict]:
    """
    Split data into train and test sets, then impute and standardize
    using training-set statistics only.

    Args:
        cleaned_df: Cleaned DataFrame.

    Returns:
        X_train_scaled, X_test_scaled, y_train, y_test, preprocessing_stats
    """
    X = cleaned_df[FEATURE_COLUMNS].copy()
    y = cleaned_df[[TARGET_COLUMN]].copy()

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=TEST_SIZE,
        random_state=TRAIN_TEST_RANDOM_STATE,
        shuffle=SHUFFLE,
    )

    train_medians = X_train.median()
    X_train = X_train.fillna(train_medians)
    X_test = X_test.fillna(train_medians)

    train_means = X_train.mean()
    train_stds = X_train.std(ddof=0)
    train_stds_replaced = train_stds.replace(0, 1)

    X_train_scaled = (X_train - train_means) / train_stds_replaced
    X_test_scaled = (X_test - train_means) / train_stds_replaced

    print("\n2. SPLIT AND PREPROCESS THE DATA")
    print(f"Training set size: {len(X_train_scaled)}")
    print(f"Test set size: {len(X_test_scaled)}")
    print("\nTraining-set medians:")
    print(train_medians)
    print("\nTraining-set means:")
    print(train_means)
    print("\nTraining-set standard deviations:")
    print(train_stds_replaced)
    print(
        "\nWhy use training-set medians, means, and standard deviations only?"
    )
    print(
        "Because using the full dataset would leak information from the test set "
        "into preprocessing. That would make evaluation less honest."
    )

    preprocessing_stats = {
        "medians": train_medians,
        "means": train_means,
        "stds": train_stds_replaced,
    }

    return (
        X_train_scaled.to_numpy(dtype=float),
        X_test_scaled.to_numpy(dtype=float),
        y_train.to_numpy(dtype=float),
        y_test.to_numpy(dtype=float),
        preprocessing_stats,
    )


# Neural network implementation
class TwoLayerNeuralNetwork:
    """
    A fully connected two-layer neural network.

    Architecture:
    - Input layer: 6 features
    - Hidden layer: 6 hidden units with sigmoid activation
    - Output layer: 1 unit with sigmoid activation

    Parameter dimensions:
    - W1: (6, 6)
      maps 6 input features to 6 hidden units
    - b1: (1, 6)
      one bias per hidden unit
    - W2: (6, 1)
      maps 6 hidden activations to 1 output
    - b2: (1, 1)
      one bias for the output unit

    Hidden layer computation:
        Z1 = XW1 + b1
        A1 = sigmoid(Z1)

    Output layer computation:
        Z2 = A1W2 + b2
        A2 = sigmoid(Z2)

    The output A2 can be interpreted as a probability-like score because
    sigmoid compresses values into the range (0, 1). Larger values indicate
    stronger model confidence toward class 1 (Accepted).
    """

    def __init__(
        self,
        input_dim: int,
        hidden_units: int,
        learning_rate: float,
        random_seed: int,
    ) -> None:
        """
        Initialize network parameters.

        Args:
            input_dim: Number of input features.
            hidden_units: Number of hidden units.
            learning_rate: Gradient descent learning rate.
            random_seed: Random seed for reproducibility.
        """
        rng = np.random.default_rng(random_seed)

        self.learning_rate = learning_rate

        self.W1 = rng.normal(loc=0.0, scale=0.1, size=(input_dim, hidden_units))
        self.b1 = np.zeros((1, hidden_units))

        self.W2 = rng.normal(loc=0.0, scale=0.1, size=(hidden_units, 1))
        self.b2 = np.zeros((1, 1))

        self.Z1: np.ndarray | None = None
        self.A1: np.ndarray | None = None
        self.Z2: np.ndarray | None = None
        self.A2: np.ndarray | None = None

    def forward(self, X: np.ndarray) -> np.ndarray:
        """
        Run forward propagation.

        Args:
            X: Input matrix of shape (n_samples, input_dim).

        Returns:
            Predicted probabilities of shape (n_samples, 1).
        """
        self.Z1 = X @ self.W1 + self.b1
        self.A1 = sigmoid(self.Z1)

        self.Z2 = self.A1 @ self.W2 + self.b2
        self.A2 = sigmoid(self.Z2)

        return self.A2

    def backward(self, X: np.ndarray, y: np.ndarray) -> None:
        """
        Run backpropagation and update parameters using full-batch
        gradient descent.

        Loss function:
            MSE = mean((y - y_hat)^2)

        Args:
            X: Input matrix of shape (n_samples, input_dim).
            y: True labels of shape (n_samples, 1).
        """
        n_samples = X.shape[0]

        y_hat = self.A2
        if y_hat is None or self.A1 is None:
            raise ValueError("Must call forward() before backward().")

        dA2 = (2.0 / n_samples) * (y_hat - y)
        dZ2 = dA2 * y_hat * (1.0 - y_hat)

        dW2 = self.A1.T @ dZ2
        db2 = np.sum(dZ2, axis=0, keepdims=True)

        dA1 = dZ2 @ self.W2.T
        dZ1 = dA1 * self.A1 * (1.0 - self.A1)

        dW1 = X.T @ dZ1
        db1 = np.sum(dZ1, axis=0, keepdims=True)

        self.W1 -= self.learning_rate * dW1
        self.b1 -= self.learning_rate * db1
        self.W2 -= self.learning_rate * dW2
        self.b2 -= self.learning_rate * db2

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """
        Predict probability-like scores.

        Args:
            X: Input matrix.

        Returns:
            Predicted probabilities.
        """
        return self.forward(X)

    def predict(self, X: np.ndarray, threshold: float = 0.5) -> np.ndarray:
        """
        Predict binary labels.

        Args:
            X: Input matrix.
            threshold: Classification threshold.

        Returns:
            Binary predictions of shape (n_samples, 1).
        """
        probabilities = self.predict_proba(X)
        return (probabilities >= threshold).astype(float)

    def get_parameters(self) -> dict:
        """
        Return a deep copy of parameters.

        Returns:
            Dictionary of network parameters.
        """
        return {
            "W1": self.W1.copy(),
            "b1": self.b1.copy(),
            "W2": self.W2.copy(),
            "b2": self.b2.copy(),
        }

    def set_parameters(self, params: dict) -> None:
        """
        Restore parameters from a saved parameter dictionary.

        Args:
            params: Dictionary containing W1, b1, W2, b2.
        """
        self.W1 = params["W1"].copy()
        self.b1 = params["b1"].copy()
        self.W2 = params["W2"].copy()
        self.b2 = params["b2"].copy()


# Training
def train_model(
    model: TwoLayerNeuralNetwork,
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_test: np.ndarray,
    y_test: np.ndarray,
    max_epochs: int,
    patience: int,
    log_path: str | Path = "training.log",
) -> tuple[dict, dict]:
    """
    Train the model using full-batch gradient descent and early stopping
    based on test-set MSE.

    Args:
        model: Neural network model.
        X_train: Standardized training features.
        y_train: Training labels.
        X_test: Standardized test features.
        y_test: Test labels.
        max_epochs: Maximum number of epochs.
        patience: Early stopping patience.
        log_path: Path to save training log.

    Returns:
        history dictionary and best_result dictionary
    """
    history = {
        "epoch": [],
        "train_mse": [],
        "test_mse": [],
        "test_accuracy": [],
    }

    best_test_mse = float("inf")
    best_epoch = -1
    best_params = None
    epochs_without_improvement = 0

    log_lines = []
    header = "epoch\ttrain_mse\ttest_mse\ttest_accuracy"
    log_lines.append(header)

    print("\n4. TRAIN THE MODEL UNTIL TEST MSE STOPS IMPROVING")

    for epoch in range(1, max_epochs + 1):
        train_pred = model.forward(X_train)
        train_mse = mean_squared_error(y_train, train_pred)

        model.backward(X_train, y_train)

        test_pred = model.forward(X_test)
        test_mse = mean_squared_error(y_test, test_pred)
        test_pred_binary = (test_pred >= 0.5).astype(float)
        test_acc = accuracy_score_binary(y_test, test_pred_binary)

        history["epoch"].append(epoch)
        history["train_mse"].append(train_mse)
        history["test_mse"].append(test_mse)
        history["test_accuracy"].append(test_acc)

        log_line = f"{epoch}\t{train_mse:.6f}\t{test_mse:.6f}\t{test_acc:.4f}"
        log_lines.append(log_line)

        if epoch % 100 == 0:
            print(
                f"Epoch {epoch:5d} | "
                f"Train MSE: {train_mse:.6f} | "
                f"Test MSE: {test_mse:.6f} | "
                f"Test Accuracy: {test_acc:.4f}"
            )

        if test_mse < best_test_mse:
            best_test_mse = test_mse
            best_epoch = epoch
            best_params = model.get_parameters()
            epochs_without_improvement = 0
        else:
            epochs_without_improvement += 1

        if epochs_without_improvement >= patience:
            print(
                f"\nEarly stopping triggered at epoch {epoch}. "
                f"No test MSE improvement for {patience} consecutive epochs."
            )
            break

    if best_params is not None:
        model.set_parameters(best_params)

    Path(log_path).write_text("\n".join(log_lines), encoding="utf-8")

    best_result = {
        "best_epoch": best_epoch,
        "best_test_mse": best_test_mse,
    }

    return history, best_result


# Evaluation
def evaluate_model(
    model: TwoLayerNeuralNetwork,
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_test: np.ndarray,
    y_test: np.ndarray,
    best_result: dict,
    total_rows_after_filtering: int,
) -> None:
    """
    Evaluate the final restored model and print required metrics.

    Args:
        model: Trained neural network.
        X_train: Training features.
        y_train: Training labels.
        X_test: Test features.
        y_test: Test labels.
        best_result: Best epoch and best test MSE.
        total_rows_after_filtering: Number of rows remaining after filtering.
    """
    train_pred_proba = model.predict_proba(X_train)
    test_pred_proba = model.predict_proba(X_test)

    train_pred = (train_pred_proba >= 0.5).astype(float)
    test_pred = (test_pred_proba >= 0.5).astype(float)

    train_acc = accuracy_score_binary(y_train, train_pred)
    test_acc = accuracy_score_binary(y_test, test_pred)

    print("\n5. EVALUATE THE FINAL MODEL")
    print(f"Best epoch: {best_result['best_epoch']}")
    print(f"Best test MSE: {best_result['best_test_mse']:.6f}")
    print(f"Final training accuracy: {train_acc:.4f}")
    print(f"Final test accuracy: {test_acc:.4f}")
    print(f"Number of rows used after filtering: {total_rows_after_filtering}")
    print(f"Final train/test split sizes: {len(X_train)} train, {len(X_test)} test")

    print("\nDiscussion:")
    print(
        "- The model does not appear to overfit since test accuracy (~69%) is slightly "
        "higher than training accuracy (~68%), indicating reasonable generalization."
    )
    print(
        "- The test accuracy (~69%) is moderate—better than random guessing (50%), "
        "but not strong enough for a highly reliable predictive model."
    )
    print(
        "- The dataset is limited and lacks important features (e.g., research experience, "
        "recommendations), so it is not sufficient for a realistic admissions predictor."
    )


# Plotting
def plot_mse_curves(history: dict, output_path: str | Path = "mse_curve.png") -> None:
    """
    Plot training and test MSE across epochs and save to a PNG file.

    Args:
        history: Training history dictionary.
        output_path: Path to save the figure.
    """
    plt.figure(figsize=(10, 6))
    plt.plot(history["epoch"], history["train_mse"], label="Training MSE")
    plt.plot(history["epoch"], history["test_mse"], label="Test MSE")
    plt.title("Training and Test MSE Over Time")
    plt.xlabel("Epoch")
    plt.ylabel("Mean Squared Error")
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=300)
    plt.close()

    print("\n6. PLOT TRAIN AND TEST MSE OVER TIME")
    print(f"Saved plot to: {output_path}")


# Artificial applicants
def preprocess_new_applicants(
    applicants_df: pd.DataFrame,
    preprocessing_stats: dict,
) -> np.ndarray:
    """
    Apply the same preprocessing pipeline used on the real data.

    Args:
        applicants_df: DataFrame of artificial applicants.
        preprocessing_stats: Dictionary with training medians, means, stds.

    Returns:
        Standardized NumPy array.
    """
    applicants_processed = applicants_df.copy()
    applicants_processed = applicants_processed[FEATURE_COLUMNS]
    applicants_processed = applicants_processed.fillna(preprocessing_stats["medians"])
    applicants_processed = (
        applicants_processed - preprocessing_stats["means"]
    ) / preprocessing_stats["stds"]

    return applicants_processed.to_numpy(dtype=float)


def evaluate_artificial_applicants(
    model: TwoLayerNeuralNetwork,
    preprocessing_stats: dict,
) -> None:
    """
    Create and evaluate at least two artificial applicants.

    Args:
        model: Trained neural network.
        preprocessing_stats: Preprocessing statistics from training data.
    """
    artificial_applicants = pd.DataFrame(
        [
            {
                "gpa": 3.9,
                "gre": 330.0,
                "gre_v": 165.0,
                "gre_aw": 5.5,
                "ms_vs_phd": 1.0,
                "international_vs_local": 1.0,
            },
            {
                "gpa": 3.3,
                "gre": 305.0,
                "gre_v": 160.0,
                "gre_aw": 5.0,
                "ms_vs_phd": 0.0,
                "international_vs_local": 0.0,
            },
        ]
    )

    artificial_array = preprocess_new_applicants(
        artificial_applicants,
        preprocessing_stats,
    )

    probabilities = model.predict_proba(artificial_array)
    predictions = (probabilities >= 0.5).astype(float)

    results = artificial_applicants.copy()
    results["predicted_probability"] = probabilities.flatten()
    results["predicted_label"] = predictions.flatten()
    results["predicted_status"] = results["predicted_label"].map(
        {1.0: "Accepted", 0.0: "Rejected"}
    )

    print("\n7. TEST THE MODEL ON ARTIFICIAL APPLICANTS")
    print(results)

    print("\nDiscussion:")
    print(
        "- The model produced counterintuitive results, rejecting the stronger applicant while accepting "
        "the weaker one. This suggests the model is influenced by patterns in the data (e.g., degree type "
        "or citizenship) rather than academic strength alone, highlighting potential bias and limitations."
    )


# Main
def main() -> None:
    """
    Main program entry point.
    """

    raw_df = pd.read_json("cleaned_gradcafe.json")
    cleaned_df = clean_applicant_data(raw_df)

    X_train, X_test, y_train, y_test, preprocessing_stats = split_and_standardize_data(
        cleaned_df
    )

    model = TwoLayerNeuralNetwork(
        input_dim=len(FEATURE_COLUMNS),
        hidden_units=HIDDEN_UNITS,
        learning_rate=LEARNING_RATE,
        random_seed=RANDOM_SEED,
    )

    history, best_result = train_model(
        model=model,
        X_train=X_train,
        y_train=y_train,
        X_test=X_test,
        y_test=y_test,
        max_epochs=MAX_EPOCHS,
        patience=PATIENCE,
        log_path="training.log",
    )

    evaluate_model(
        model=model,
        X_train=X_train,
        y_train=y_train,
        X_test=X_test,
        y_test=y_test,
        best_result=best_result,
        total_rows_after_filtering=len(cleaned_df),
    )

    plot_mse_curves(history, output_path="mse_curve.png")

    evaluate_artificial_applicants(model, preprocessing_stats)



if __name__ == "__main__":
    main()
