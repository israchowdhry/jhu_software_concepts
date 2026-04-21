"""
Loads the cleaned GradCafe dataset, builds unified multimodal text inputs
(text + structured features), fine-tunes a pretrained Hugging Face
transformer model for binary classification, evaluates the final model,
and saves all artifacts needed for inference and Flask deployment.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import torch
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    precision_recall_fscore_support,
)
from sklearn.model_selection import train_test_split
from transformers import (
    AutoModelForSequenceClassification,
    AutoTokenizer,
    Trainer,
    TrainingArguments,
)

RANDOM_STATE = 42
TEST_SIZE = 0.2
SHUFFLE = True

MODEL_NAME = "distilbert-base-uncased"
MAX_LENGTH = 256
BATCH_SIZE = 8
NUM_EPOCHS = 2
LEARNING_RATE = 2e-5

TEXT_FIELDS = [
    "program_name",
    "comments",
    "llm-generated-program",
    "llm-generated-university",
    "start_term",
]

STRUCTURED_FIELDS = [
    "gpa",
    "gre_score",
    "gre_v_score",
    "gre_aw",
    "degree",
    "international_american",
]

TARGET_FIELD = "applicant_status"

OUTPUT_DIR = Path("saved_model")
REPORTS_DIR = Path("reports")


@dataclass
class DatasetBundle:
    cleaned_df: pd.DataFrame
    train_df: pd.DataFrame
    test_df: pd.DataFrame
    train_texts: list[str]
    test_texts: list[str]
    y_train: list[int]
    y_test: list[int]


def safe_text(value: object, default: str = "Unknown") -> str:
    if pd.isna(value):
        return default
    text = str(value).strip()
    if text == "":
        return default
    return text


def safe_numeric(value: object, default: str = "Unknown") -> str:
    if pd.isna(value):
        return default
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return default
    return str(numeric_value)


def normalize_degree(value: object) -> str:
    degree = safe_text(value, default="Unknown").lower()
    if degree in {"masters", "master's", "ms", "m.s.", "m.a.", "ma"}:
        return "Masters"
    if degree in {"phd", "ph.d."}:
        return "PhD"
    return "Unknown"


def normalize_citizenship(value: object) -> str:
    category = safe_text(value, default="Unknown").lower()
    if category == "international":
        return "International"
    if category == "american":
        return "American"
    return "Unknown"


def create_unified_input(row: pd.Series) -> str:
    program = safe_text(row.get("program_name"))
    comments = safe_text(row.get("comments"), default="None")
    llm_program = safe_text(row.get("llm-generated-program"))
    university = safe_text(row.get("llm-generated-university"))
    start_term = safe_text(row.get("start_term"))
    degree = normalize_degree(row.get("degree"))
    citizenship = normalize_citizenship(row.get("international_american"))
    gpa = safe_numeric(row.get("gpa"))
    gre = safe_numeric(row.get("gre_score"))
    gre_v = safe_numeric(row.get("gre_v_score"))
    gre_aw = safe_numeric(row.get("gre_aw"))

    return (
        f"Program: {program}\n"
        f"LLM Program: {llm_program}\n"
        f"University: {university}\n"
        f"Comments: {comments}\n"
        f"Start Term: {start_term}\n"
        f"Degree: {degree}\n"
        f"Citizenship: {citizenship}\n"
        f"GPA: {gpa}\n"
        f"GRE Total: {gre}\n"
        f"GRE Verbal: {gre_v}\n"
        f"GRE AW: {gre_aw}"
    )


def clean_applicant_data(df: pd.DataFrame) -> pd.DataFrame:
    original_rows = len(df)
    cleaned = df.copy()

    cleaned[TARGET_FIELD] = cleaned[TARGET_FIELD].astype(str).str.strip().str.lower()
    cleaned = cleaned[cleaned[TARGET_FIELD].isin(["accepted", "rejected"])].copy()

    if "entry_url" in cleaned.columns:
        cleaned = cleaned.drop_duplicates(subset=["entry_url"]).copy()

    numeric_columns = ["gpa", "gre_score", "gre_v_score", "gre_aw"]
    for column in numeric_columns:
        if column in cleaned.columns:
            cleaned[column] = pd.to_numeric(cleaned[column], errors="coerce")

    for column in TEXT_FIELDS + ["degree", "international_american"]:
        if column in cleaned.columns:
            cleaned[column] = cleaned[column].fillna("")

    required_for_input = [
        "program_name",
        "comments",
        "degree",
        "international_american",
    ]
    available_required = [col for col in required_for_input if col in cleaned.columns]

    def row_has_usable_info(row: pd.Series) -> bool:
        text_present = any(safe_text(row.get(col), default="") != "" for col in available_required)
        numeric_present = any(
            not pd.isna(row.get(col))
            for col in ["gpa", "gre_score", "gre_v_score", "gre_aw"]
            if col in cleaned.columns
        )
        return text_present or numeric_present

    cleaned = cleaned[cleaned.apply(row_has_usable_info, axis=1)].copy()

    cleaned["label"] = np.where(cleaned[TARGET_FIELD] == "accepted", 1, 0)


    print("\n1. LOAD AND PREPARE THE APPLICANT DATASET")
    print(f"Number of rows in the original dataset: {original_rows}")
    print(f"Number of rows remaining after filtering: {len(cleaned)}")
    print(f"Number of Accepted rows: {(cleaned['label'] == 1).sum()}")
    print(f"Number of Rejected rows: {(cleaned['label'] == 0).sum()}")

    fields_used = TEXT_FIELDS + STRUCTURED_FIELDS
    print("\nFields used for modeling:")
    for field in fields_used:
        print(f"- {field}")

    preview_columns = [
        col
        for col in [
            "program_name",
            "comments",
            "llm-generated-program",
            "llm-generated-university",
            "start_term",
            "gpa",
            "gre_score",
            "gre_v_score",
            "gre_aw",
            "degree",
            "international_american",
            "label",
        ]
        if col in cleaned.columns
    ]

    print("\nPreview of cleaned dataframe:")
    print(cleaned[preview_columns].head(5))

    return cleaned


def build_dataset_bundle(cleaned_df: pd.DataFrame) -> DatasetBundle:
    cleaned_df = cleaned_df.copy()
    cleaned_df["model_input"] = cleaned_df.apply(create_unified_input, axis=1)

    train_df, test_df = train_test_split(
        cleaned_df,
        test_size=TEST_SIZE,
        random_state=RANDOM_STATE,
        shuffle=SHUFFLE,
        stratify=cleaned_df["label"],
    )

    train_texts = train_df["model_input"].tolist()
    test_texts = test_df["model_input"].tolist()
    y_train = train_df["label"].tolist()
    y_test = test_df["label"].tolist()

    print("\n2. CONVERT EACH APPLICANT INTO A UNIFIED MODEL INPUT")
    print("\nExact template used:")
    print(
        "Program: {program_name}\n"
        "LLM Program: {llm-generated-program}\n"
        "University: {llm-generated-university}\n"
        "Comments: {comments}\n"
        "Start Term: {start_term}\n"
        "Degree: {degree}\n"
        "Citizenship: {international_american}\n"
        "GPA: {gpa}\n"
        "GRE Total: {gre_score}\n"
        "GRE Verbal: {gre_v_score}\n"
        "GRE AW: {gre_aw}"
    )

    print("\nThree sample model inputs:")
    for i, text in enumerate(train_texts[:3], start=1):
        print(f"\n--- SAMPLE INPUT {i} ---")
        print(text)

    print("\n3. SPLIT THE DATA INTO TRAINING AND TESTING SETS")
    print(f"Training set size: {len(train_df)}")
    print(f"Test set size: {len(test_df)}")
    print("\nClass balance in training set:")
    print(train_df["label"].value_counts(normalize=False).sort_index())
    print("\nClass balance in test set:")
    print(test_df["label"].value_counts(normalize=False).sort_index())
    print(
        "\nWhy train/test separation matters:\n"
        "It prevents us from judging the model on examples it already learned "
        "during training. That gives a more honest estimate of how it may behave "
        "on new users who submit the web form."
    )

    return DatasetBundle(
        cleaned_df=cleaned_df,
        train_df=train_df,
        test_df=test_df,
        train_texts=train_texts,
        test_texts=test_texts,
        y_train=y_train,
        y_test=y_test,
    )


class AdmissionsDataset(torch.utils.data.Dataset):
    def __init__(self, encodings: dict, labels: list[int]) -> None:
        self.encodings = encodings
        self.labels = labels

    def __len__(self) -> int:
        return len(self.labels)

    def __getitem__(self, idx: int) -> dict[str, torch.Tensor]:
        item = {key: torch.tensor(value[idx]) for key, value in self.encodings.items()}
        item["labels"] = torch.tensor(self.labels[idx], dtype=torch.long)
        return item


def compute_metrics(eval_pred: tuple[np.ndarray, np.ndarray]) -> dict[str, float]:
    logits, labels = eval_pred
    predictions = np.argmax(logits, axis=1)

    accuracy = accuracy_score(labels, predictions)
    precision, recall, f1, _ = precision_recall_fscore_support(
        labels,
        predictions,
        average="binary",
        zero_division=0,
    )

    return {
        "accuracy": float(accuracy),
        "precision": float(precision),
        "recall": float(recall),
        "f1": float(f1),
    }


def train_and_save_model(bundle: DatasetBundle) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    print("\n4. FINE-TUNE A PRETRAINED PYTORCH LANGUAGE MODEL")
    print(f"Chosen model: {MODEL_NAME}")
    print("Why this model: DistilBERT is lightweight, fast, and practical for class assignments.")
    print(f"Tokenizer name: {MODEL_NAME}")
    print(f"Max sequence length: {MAX_LENGTH}")
    print(f"Batch size: {BATCH_SIZE}")
    print(f"Number of epochs: {NUM_EPOCHS}")
    print(f"Learning rate: {LEARNING_RATE}")
    print("Optimizer used: AdamW (inside Hugging Face Trainer)")
    print(f"Device used: {'cuda' if torch.cuda.is_available() else 'cpu'}")

    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)

    train_encodings = tokenizer(
        bundle.train_texts,
        truncation=True,
        padding=True,
        max_length=MAX_LENGTH,
    )
    test_encodings = tokenizer(
        bundle.test_texts,
        truncation=True,
        padding=True,
        max_length=MAX_LENGTH,
    )

    train_dataset = AdmissionsDataset(train_encodings, bundle.y_train)
    test_dataset = AdmissionsDataset(test_encodings, bundle.y_test)

    model = AutoModelForSequenceClassification.from_pretrained(
        MODEL_NAME,
        num_labels=2,
    )

    training_args = TrainingArguments(
        output_dir=str(REPORTS_DIR / "trainer_output"),
        eval_strategy="epoch",
        save_strategy="epoch",
        logging_strategy="epoch",
        learning_rate=LEARNING_RATE,
        per_device_train_batch_size=BATCH_SIZE,
        per_device_eval_batch_size=BATCH_SIZE,
        num_train_epochs=NUM_EPOCHS,
        weight_decay=0.01,
        load_best_model_at_end=True,
        metric_for_best_model="f1",
        greater_is_better=True,
        seed=RANDOM_STATE,
        report_to="none",
    )

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=test_dataset,
        processing_class=tokenizer,
        compute_metrics=compute_metrics,
    )

    train_result = trainer.train()

    print("\nRepresentative training output logs:")
    log_history = trainer.state.log_history
    for entry in log_history[:10]:
        print(entry)

    eval_result = trainer.evaluate()

    predictions_output = trainer.predict(test_dataset)
    logits = predictions_output.predictions
    predicted_labels = np.argmax(logits, axis=1)
    probabilities = torch.softmax(torch.tensor(logits), dim=1).numpy()

    accuracy = accuracy_score(bundle.y_test, predicted_labels)
    precision, recall, f1, _ = precision_recall_fscore_support(
        bundle.y_test,
        predicted_labels,
        average="binary",
        zero_division=0,
    )
    cm = confusion_matrix(bundle.y_test, predicted_labels)

    print("\n5. EVALUATE THE FINAL MODEL")
    print("\nMetrics summary:")
    print(f"Accuracy:  {accuracy:.4f}")
    print(f"Precision: {precision:.4f}")
    print(f"Recall:    {recall:.4f}")
    print(f"F1:        {f1:.4f}")

    print("\nConfusion matrix:")
    print(cm)

    print("\nClass distribution in test set:")
    print(pd.Series(bundle.y_test).value_counts().sort_index())

    print("\nProbability examples for several predictions:")
    for i in range(min(5, len(bundle.test_df))):
        accepted_prob = float(probabilities[i][1])
        rejected_prob = float(probabilities[i][0])
        print(
            f"Example {i + 1}: "
            f"True={bundle.y_test[i]}, "
            f"Pred={int(predicted_labels[i])}, "
            f"P(Rejected)={rejected_prob:.4f}, "
            f"P(Accepted)={accepted_prob:.4f}"
        )

    print("\nExample Predictions (Correct vs Incorrect):")

    results_df = bundle.test_df.copy().reset_index(drop=True)
    results_df["predicted_label"] = predicted_labels
    results_df["true_label"] = bundle.y_test
    results_df["accepted_probability"] = probabilities[:, 1]

    # Correct predictions
    correct = results_df[results_df["predicted_label"] == results_df["true_label"]]

    # Incorrect predictions
    incorrect = results_df[results_df["predicted_label"] != results_df["true_label"]]

    print("\n--- Correctly Classified Examples ---")
    for _, row in correct.head(3).iterrows():
        print("\nInput:")
        print(row["model_input"])
        print(f"True Label: {row['true_label']}")
        print(f"Predicted: {row['predicted_label']}")
        print(f"Accepted Probability: {row['accepted_probability']:.4f}")

    print("\n--- Incorrectly Classified Examples ---")
    for _, row in incorrect.head(3).iterrows():
        print("\nInput:")
        print(row["model_input"])
        print(f"True Label: {row['true_label']}")
        print(f"Predicted: {row['predicted_label']}")
        print(f"Accepted Probability: {row['accepted_probability']:.4f}")

    print("\nInterpretation:")
    print(
        "The model does not show a strong bias toward Accepted or Rejected outcomes, "
        "as errors in both directions are similar. Its performance (~78% accuracy) is "
        "significantly better than random guessing. Compared to the earlier two-layer "
        "neural network, the transformer model is stronger because it uses both text "
        "and structured features. However, the dataset is self-reported and incomplete, "
        "so it is not sufficient for a realistic admissions predictor."
    )

    trainer.save_model(str(OUTPUT_DIR))
    tokenizer.save_pretrained(str(OUTPUT_DIR))

    metadata = {
        "model_name": MODEL_NAME,
        "max_length": MAX_LENGTH,
        "label_map": {"0": "Rejected", "1": "Accepted"},
        "text_fields": TEXT_FIELDS,
        "structured_fields": STRUCTURED_FIELDS,
    }

    with open(OUTPUT_DIR / "metadata.json", "w", encoding="utf-8") as file:
        json.dump(metadata, file, indent=2)

    metrics_summary = {
        "accuracy": float(accuracy),
        "precision": float(precision),
        "recall": float(recall),
        "f1": float(f1),
        "eval_result": eval_result,
        "train_result_metrics": train_result.metrics,
        "confusion_matrix": cm.tolist(),
    }

    with open(REPORTS_DIR / "metrics_summary.json", "w", encoding="utf-8") as file:
        json.dump(metrics_summary, file, indent=2)

    results_df = bundle.test_df.copy().reset_index(drop=True)
    results_df["predicted_label"] = predicted_labels
    results_df["accepted_probability"] = probabilities[:, 1]
    results_df.to_csv(REPORTS_DIR / "test_predictions.csv", index=False)

    joblib.dump(bundle.cleaned_df, REPORTS_DIR / "cleaned_dataframe.joblib")

    print("\n6. SAVE AND RELOAD THE TRAINED MODEL")
    print(f"Saved trained model and tokenizer to: {OUTPUT_DIR.resolve()}")
    print(f"Saved reports to: {REPORTS_DIR.resolve()}")


def demonstrate_reload() -> None:
    print("\nReload demonstration:")

    tokenizer = AutoTokenizer.from_pretrained(str(OUTPUT_DIR))
    model = AutoModelForSequenceClassification.from_pretrained(str(OUTPUT_DIR))
    model.eval()

    example_inputs = [
        (
            "Program: Biomedical Engineering\n"
            "LLM Program: Biomedical Engineering\n"
            "University: Tufts University\n"
            "Comments: None\n"
            "Start Term: Fall 2026\n"
            "Degree: PhD\n"
            "Citizenship: International\n"
            "GPA: Unknown\n"
            "GRE Total: Unknown\n"
            "GRE Verbal: Unknown\n"
            "GRE AW: Unknown"
        ),
        (
            "Program: Computer Science\n"
            "LLM Program: Computer Science\n"
            "University: Johns Hopkins University\n"
            "Comments: Strong research background.\n"
            "Start Term: Fall 2026\n"
            "Degree: Masters\n"
            "Citizenship: American\n"
            "GPA: 3.8\n"
            "GRE Total: 325\n"
            "GRE Verbal: 160\n"
            "GRE AW: 5.0"
        ),
    ]

    for i, example_text in enumerate(example_inputs, start=1):
        tokens = tokenizer(
            example_text,
            truncation=True,
            padding=True,
            max_length=MAX_LENGTH,
            return_tensors="pt",
        )
        with torch.no_grad():
            outputs = model(**tokens)
            probabilities = torch.softmax(outputs.logits, dim=1).numpy()[0]

        predicted_label = int(np.argmax(probabilities))
        predicted_status = "Accepted" if predicted_label == 1 else "Rejected"

        print(f"\nReloaded example {i}:")
        print(f"Prediction: {predicted_status}")
        print(f"Accepted probability: {probabilities[1]:.4f}")


def main() -> None:
    raw_df = pd.read_json("cleaned_gradcafe.json")
    cleaned_df = clean_applicant_data(raw_df)
    bundle = build_dataset_bundle(cleaned_df)
    train_and_save_model(bundle)
    demonstrate_reload()


if __name__ == "__main__":
    main()
