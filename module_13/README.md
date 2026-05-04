SSH url: git@github.com:israchowdhry/jhu_software_concepts.git

Name: Isra Chowdhry (ichowdh6)

Module Info: Module 13 Assignment: Scale & LM Deployment Assignment Due on 04/29/2026 at 11:59 EST

Approach: 
# Grad Cafe Admissions Predictor (Module 13)

This project builds a machine learning pipeline to predict graduate admissions outcomes using both structured and unstructured data. It extends earlier work from Module 12 by replacing a simple two-layer neural network with a fine-tuned transformer model.

---

## Overview

The goal of this project is to predict whether an applicant will be **Accepted** or **Rejected** using data from Grad Cafe. The model combines:

- Structured features (GPA, GRE, degree type, citizenship)
- Unstructured text (comments, program name, university)

A pretrained **DistilBERT** transformer model is fine-tuned to perform binary classification on this combined input.

---

## Step 1 – Columns Used and Why

The model uses both text-based and structured applicant fields.

The text fields include:
- program_name
- comments
- llm-generated-program
- llm-generated-university
- start_term

These fields were chosen because they describe the applicant’s program, university, admission cycle, and any additional context. They allow the transformer model to learn patterns from natural language and program descriptions.

The structured fields include:
- gpa
- gre_score
- gre_v_score
- gre_aw
- degree
- international_american

These fields represent common admissions-related factors such as academic performance, degree level, and applicant category. These values were converted into labeled text so they could be processed by the transformer model alongside the free-text fields.

---

## Tokenizer Choice

The AutoTokenizer associated with distilbert-base-uncased was used so that the tokenization process matches the pretrained transformer model.

This is important because the tokenizer determines how input text is split into tokens, and the model expects inputs in the same format it was originally trained on.

The distilbert-base-uncased model was chosen because it is smaller and faster than full BERT while still providing strong performance for text classification tasks. The “uncased” version also helps standardize input by ignoring capitalization differences, which is useful for inconsistent scraped data.


## Project Structure

- **train_model.py** – data preprocessing, training, and evaluation  
- **inference.py** – model loading and prediction logic  
- **run.py** – Flask web application  
- **cleaned_gradcafe.json** – input dataset  

- **saved_model/** – trained model and tokenizer (generated)  
- **reports/** – metrics and predictions (generated)  

- **templates/**
  - index.html  
  - will_you_get_in.html  

- **static/**
  - style.css


---

## Setup Instructions

### Install Dependencies

```bash
pip install flask pandas numpy scikit-learn torch transformers accelerate joblib
```

## Training the Model

Training is performed using a pretrained transformer model (DistilBERT).

### Recommended Setup (Google Colab with GPU)

1. Enable GPU:
   - Runtime → Change runtime type → Select **T4 GPU**

2. Upload required files:
   - `train_model.py`
   - `cleaned_gradcafe.json`

3. Install dependencies and run training:

```python
!pip install transformers accelerate scikit-learn pandas numpy torch joblib
!python train_model.py
```
4. Download the trained model:
```python
!zip -r model.zip saved_model reports
```
## Training Output

The training process generates:

- saved_model/ – trained model and tokenizer
- reports/metrics_summary.json – evaluation metrics
-reports/test_predictions.csv – predictions on test data

## Running the Application

After downloading and placing saved_model/ in your project directory:

```bash
python run.py
```
Then open in your browser:

http://127.0.0.1:8080/

## Web Application

The Flask app includes a page called “Will You Get In?”

Users can enter:

Program name
University
Comments
Start term
Degree
Citizenship
GPA and GRE scores

The application:

- Converts inputs into a unified text format
- Runs inference using the trained transformer model
- Displays:
  - Prediction (Accepted / Rejected)
  - Probability scores

## Model Details

- Model: distilbert-base-uncased
- Framework: PyTorch + Hugging Face Transformers
- Task: Binary classification
- Input: Combined structured and text features
- Training: Fine-tuning using the Hugging Face Trainer API


## Evaluation Metrics

Example performance:

- Accuracy: ~78%
- Precision: ~76%
- Recall: ~75%
- F1 Score: ~76%

The model performs significantly better than random guessing and improves upon the earlier two-layer neural network.

## Limitations
- Data is self-reported and incomplete
- Missing values are common
- Important admissions factors are not included (e.g., essays, recommendations)
- Predictions may be biased or inaccurate

## Disclaimer

This project is a course assignment and is not a real admissions decision system.

Predictions are based on incomplete and self-reported data and should not be used for real-world decisions.

## Educational Value

This project demonstrates:

- Fine-tuning pretrained transformer models
- Combining structured and unstructured data
- Building an end-to-end machine learning pipeline
- Integrating machine learning into a web application