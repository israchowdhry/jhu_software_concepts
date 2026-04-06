SSH url: git@github.com:israchowdhry/jhu_software_concepts.git

Name: Isra Chowdhry (ichowdh6)

Module Info: Module 11 Assignment: MLOps Pipeline Assignment Due on 04/12/2026 at 11:59 EST

Approach: 

This project implements a KMeans clustering pipeline on Grad Cafe program data and tracks the model using MLflow. The script loads a cleaned dataset, converts program names into numerical features using TF-IDF vectorization, reduces dimensionality using PCA, and then applies KMeans clustering. The purpose of this assignment is to demonstrate a simple MLOps workflow by logging model parameters, tracking performance metrics, and registering the trained model using MLflow.

The main file included in this project is kmeans_mlops_pipeline.py, which contains the full pipeline for data loading, preprocessing, clustering, and MLflow tracking. A requirements.txt file is also included to list all required Python packages. In addition, three screenshots are provided: cluster_run.png, which shows the successful MLflow experiment run; cluster_details.png (split into two pngs: cluster_details1.png and cluster_details2.png since the metrics and parameters were not fitting into one screenshot), which displays the logged parameters and inertia metric; and model_details.png, which shows the registered model in MLflow.

To run the project, first navigate to the module_11 folder. Then install the required dependencies using pip. Next, start the MLflow server locally using localhost on port 8080. In a separate terminal, run the Python script to execute the pipeline. Once the script finishes running, open a browser and go to http://localhost:8080
 to view the experiment, run details, and registered model in the MLflow interface.

The script performs several steps. It begins by loading the cleaned Grad Cafe dataset and preparing the necessary columns. It then converts program names into TF-IDF features and reduces the feature space using PCA to improve clustering performance. After that, it trains a KMeans model using the required parameters and logs the model’s inertia along with all parameters to MLflow. The trained model is also registered in MLflow, and the clustered dataset is saved as a CSV file.

The clustering model uses 25 clusters as specified in the assignment. PCA is applied prior to clustering to reduce dimensionality and improve efficiency. MLflow is used to track the experiment, making it easy to view parameters, compare runs, and manage the trained model.

Known bugs: No known bugs