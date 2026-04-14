SSH url: git@github.com:israchowdhry/jhu_software_concepts.git

Name: Isra Chowdhry (ichowdh6)

Module Info: Module 12 Assignment: Two-Layer Neural Network Assignment Due on 04/20/2026 at 11:59 EST

Approach: 
# Module 12: Neural Network for Graduate Admissions Prediction

## Overview

This program builds, trains, evaluates, and analyzes a simple two-layer neural network for predicting graduate admissions outcomes (Accepted vs. Rejected). The model is implemented from scratch using NumPy and follows the assignment requirements, including data preprocessing, forward propagation, backpropagation, and gradient descent. The goal is to understand how a neural network operates at a low level while applying it to a real-world-style classification problem.

---

## What the Program Does

- Loads and preprocesses applicant data from a JSON file
- Filters valid records (Accepted/Rejected and Masters/PhD only)
- Converts numeric fields and creates required binary features
- Splits data into training and testing sets (80/20)
- Standardizes features using training-set statistics
- Builds a two-layer neural network using NumPy
- Trains the model using forward propagation and backpropagation
- Applies early stopping based on test MSE
- Evaluates model performance (accuracy and MSE)
- Generates a plot of training and test MSE over time
- Tests the model on artificial applicant examples
- Prints analysis and reflection results

---

## Files Included

- `neural_network.py`  
  Main Python script containing preprocessing, model implementation, training, evaluation, and analysis.

- `cleaned_gradcafe.json`  
  Input dataset containing applicant information in JSON format.

- `mse_curve.png`  
  Plot showing training and test MSE over epochs.

- `training.log`  
  Log file containing training progress (epoch, MSE, accuracy).

- `README.md`  
  This file.

- `writeup.pdf`  
  Document containing training results, evaluation, graph, artificial applicant analysis, and reflection.

---

## How to Run the Code

1. Ensure Python 3.10+ is installed.

2. Install required packages (if needed):
   ```bash
   pip install numpy pandas matplotlib scikit-learn
   ```
3. Place cleaned_gradcafe.json in same folder

4. Run the script:
   ```bash
   python neural_network.py
   ```
   
## Outputs Produced

When the program is executed, it produces:

- Printed outputs:
    - Dataset statistics (row counts, features)
    - Training/test split information
    - Training progress (MSE and accuracy every 100 epochs)
    - Final evaluation results (accuracy and MSE)
    - Artificial applicant predictions
- Generated files:
  - mse_curve.png — visualization of training vs. test MSE over time
  - training.log — full training log with epoch-by-epoch results
