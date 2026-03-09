SSH url: git@github.com:israchowdhry/jhu_software_concepts.git

Name: Isra Chowdhry (ichowdh6)

Module Info: Module 8 Assignment: Data Preparation & Statistics Assignment Due on 03/15/2026 at 11:59 EST

Approach: 

This assignment implements a complete data cleaning and exploratory analysis workflow using Grad Café admissions data. The goal of the notebook is to demonstrate a reproducible data pipeline that retrieves raw data from Amazon S3, cleans and validates the dataset using Python data science tools, performs statistical analysis, generates visualizations, and exports the cleaned dataset back to S3 for future use.

The workflow begins by downloading the Grad Café dataset from an Amazon S3 bucket using the boto3 library. The original dataset is stored as a JSONL file, which is converted into a standard JSON format and loaded into a Pandas DataFrame for processing. A copy of the original DataFrame is preserved before cleaning to maintain reproducibility and allow comparisons between the raw and processed datasets.

The data cleaning stage prepares the dataset for analysis by standardizing column formats, converting numeric fields such as GPA and GRE scores into numeric data types, and removing invalid or incomplete records where necessary. Additional validation checks ensure that values fall within reasonable ranges and that categorical fields such as degree type and application outcome are consistent. During this step, a missingness summary table is generated to measure the number and percentage of missing values across each column, which helps evaluate the completeness of the dataset.

After the dataset is cleaned, descriptive statistics are calculated using Pandas and NumPy for key numeric variables including GPA, GRE total score, GRE verbal score, and GRE analytical writing score. These statistics include the mean, median, standard deviation, minimum, maximum, and quartile values. Potential outliers are then identified using SciPy’s z-score method, which flags observations with unusually large deviations from the mean.

The notebook also performs several statistical analyses to explore relationships within the dataset. Pearson correlation tests are used to measure the relationship between GRE-related metrics and GPA. A two-sample t-test compares GPA distributions between accepted and rejected applicants to evaluate whether the two groups differ significantly. Additionally, a chi-square test is used to examine whether application outcomes are associated with degree type, using a contingency table of categorical variables.

To further explore patterns in the data, several visualizations are generated using Matplotlib. These plots include scatter plots comparing GRE and GPA values, bar charts showing the distribution of applicants by degree type and residency status, histograms illustrating the timing of acceptance decisions, and a correlation heatmap for key numeric variables. These visualizations help reveal trends and relationships that may not be immediately visible through summary statistics alone.

A short analytical report summarizing the statistical findings and key insights from the dataset is included in analytics.pdf. This report discusses the average GRE score of accepted PhD applicants, the average GPA of accepted Master’s applicants, the results of correlation and hypothesis testing, and the remaining limitations of the dataset.

Finally, once the cleaning and analysis process is complete, the processed dataset is exported as cleaned_gradcafe.json and uploaded back to the Amazon S3 bucket using boto3. This ensures that the cleaned dataset can be reused for downstream analysis or future research.

The notebook is designed to run sequentially from top to bottom, allowing the entire data processing pipeline to be reproduced by restarting the kernel and executing all cells. The outputs of the notebook include the cleaned dataset, summary statistics tables, analytical report, and several visualizations that illustrate the results of the analysis.

Known bugs: No known bugs