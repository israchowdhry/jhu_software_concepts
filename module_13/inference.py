"""
Loads the saved transformer model and tokenizer, converts one user
submission into the same unified text format used during training,
and returns a prediction plus probability.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

MODEL_DIR = Path("saved_model")


def safe_text(value: object, default: str = "Unknown") -> str:
    if value is None:
        return default
    text = str(value).strip()
    if text == "":
        return default
    return text


def safe_numeric(value: object, default: str = "Unknown") -> str:
    if value is None:
        return default
    text = str(value).strip()
    if text == "":
        return default
    try:
        numeric_value = float(text)
    except (TypeError, ValueError):
        return default
    return str(numeric_value)


def normalize_degree(value: object) -> str:
    degree = safe_text(value).lower()
    if degree in {"masters", "master's", "ms", "m.s.", "m.a.", "ma"}:
        return "Masters"
    if degree in {"phd", "ph.d."}:
        return "PhD"
    return "Unknown"


def normalize_citizenship(value: object) -> str:
    citizenship = safe_text(value).lower()
    if citizenship == "international":
        return "International"
    if citizenship == "american":
        return "American"
    return "Unknown"


def build_model_input(form_data: dict) -> str:
    return (
        f"Program: {safe_text(form_data.get('program_name'))}\n"
        f"LLM Program: {safe_text(form_data.get('llm-generated-program', form_data.get('program_name')))}\n"
        f"University: {safe_text(form_data.get('llm-generated-university', form_data.get('university')))}\n"
        f"Comments: {safe_text(form_data.get('comments'), default='None')}\n"
        f"Start Term: {safe_text(form_data.get('start_term'))}\n"
        f"Degree: {normalize_degree(form_data.get('degree'))}\n"
        f"Citizenship: {normalize_citizenship(form_data.get('international_american'))}\n"
        f"GPA: {safe_numeric(form_data.get('gpa'))}\n"
        f"GRE Total: {safe_numeric(form_data.get('gre_score'))}\n"
        f"GRE Verbal: {safe_numeric(form_data.get('gre_v_score'))}\n"
        f"GRE AW: {safe_numeric(form_data.get('gre_aw'))}"
    )


class AdmissionsPredictor:
    def __init__(self, model_dir: str | Path = MODEL_DIR) -> None:
        self.model_dir = Path(model_dir)

        with open(self.model_dir / "metadata.json", "r", encoding="utf-8") as file:
            self.metadata = json.load(file)

        self.max_length = int(self.metadata["max_length"])
        self.label_map = {int(k): v for k, v in self.metadata["label_map"].items()}

        self.tokenizer = AutoTokenizer.from_pretrained(str(self.model_dir))
        self.model = AutoModelForSequenceClassification.from_pretrained(str(self.model_dir))
        self.model.eval()

    def predict(self, form_data: dict) -> dict:
        model_input = build_model_input(form_data)

        tokens = self.tokenizer(
            model_input,
            truncation=True,
            padding=True,
            max_length=self.max_length,
            return_tensors="pt",
        )

        with torch.no_grad():
            outputs = self.model(**tokens)
            probabilities = torch.softmax(outputs.logits, dim=1).cpu().numpy()[0]

        predicted_label = int(np.argmax(probabilities))
        prediction = self.label_map[predicted_label]

        return {
            "model_input": model_input,
            "predicted_label": predicted_label,
            "prediction": prediction,
            "accepted_probability": float(probabilities[1]),
            "rejected_probability": float(probabilities[0]),
        }


def demo_inference() -> None:
    predictor = AdmissionsPredictor()

    example = {
        "program_name": "Biomedical Engineering",
        "university": "Tufts University",
        "comments": None,
        "start_term": "Fall 2026",
        "degree": "PhD",
        "international_american": "International",
        "gpa": None,
        "gre_score": None,
        "gre_v_score": None,
        "gre_aw": None,
    }

    result = predictor.predict(example)
    print(result)


if __name__ == "__main__":
    demo_inference()
