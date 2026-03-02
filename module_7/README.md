SSH url: git@github.com:israchowdhry/jhu_software_concepts.git

Name: Isra Chowdhry (ichowdh6)

Module Info: Module 7 Assignment: Cloud Computing Assignment Due on 03/09/2026 at 11:59 EST

Approach: 

This module demonstrates a secure AWS workflow and a cloud-based data engineering pipeline. The objective of this assignment was to configure a secure AWS environment, build a data pipeline that pulls a dataset from Amazon S3 into an Amazon SageMaker notebook using boto3, and deploy the previously built Module 6 microservice architecture to an EC2 instance using Docker Compose. The work simulates a real-world cloud deployment scenario involving IAM security, object storage, notebook-based data processing, and reproducible containerized deployment.

For security, the AWS root account was protected using Multi-Factor Authentication (MFA). A separate IAM user named dailyWork-<initials> was created for day-to-day operations, and appropriate AWS-managed policies were attached to allow access to S3, SageMaker, EC2, and IAM functionality required for this assignment. The SageMaker notebook was configured with an IAM role restricted specifically to the grad-cafe S3 bucket. No AWS credentials were hardcoded in any source files. Instead, boto3 automatically used the notebook’s IAM role for authentication.

In Part 1, an S3 bucket named grad-cafe-<initials> was created with public access fully blocked. The file llm_extend_applicant_data.jsonl was uploaded to this bucket. A SageMaker notebook instance (ml.t2.medium) was launched, and a notebook named grad-cafe-pipeline.ipynb was created inside the module_7/ directory. The pipeline logic is implemented in module_7/src/s3_fetch.py, which contains two functions. The first function, download_from_s3, creates a boto3 S3 client and downloads the JSONL dataset from S3 into the local SageMaker environment. The second function, jsonl_to_json, converts the JSONL file (one JSON object per line) into a properly formatted JSON list file.

To run the pipeline, open the SageMaker notebook grad-cafe-pipeline.ipynb and execute all cells. The notebook downloads the dataset from S3 into the module_7/ directory and converts it into the required output file named applicant_data_SM.json. The notebook prints confirmation messages verifying that the file was downloaded, converted successfully, and exists in the expected location.

Execution and Output Details:

The notebook must be run inside the SageMaker notebook instance environment. From the Jupyter interface, navigate to the module_7/ directory and open grad-cafe-pipeline.ipynb. Running all cells executes the boto3 download and transformation logic. The input file llm_extend_applicant_data.jsonl is downloaded from the S3 bucket into the local SageMaker file system. The transformed output file applicant_data_SM.json is written locally inside the module_7/ directory. The file is not uploaded back to S3; it remains stored in the notebook environment unless explicitly moved.

Linting was performed on the source file src/s3_fetch.py using pylint. The command used was pylint src/s3_fetch.py, and the file achieved a score of 10.00/10. The notebook may optionally be linted using nbqa pylint grad-cafe-pipeline.ipynb. The requirements.txt file includes boto3, pylint, and nbqa to support reproducibility. No secrets or AWS credentials are included in the repository.

In Part 2, an EC2 instance (t3.micro) was launched using either Ubuntu 22.04 LTS or Amazon Linux 2023. Security groups were configured to allow SSH access (port 22) and application access (port 8080) from my IP address only. PostgreSQL (5432) and RabbitMQ management (15672) were not exposed publicly. Docker and Docker Compose were installed on the EC2 instance, and the Module 6 multi-container system (Flask web service, worker, Postgres, and RabbitMQ) was deployed using a compose file located at module_7/ec2/docker-compose.ec2.yml. The stack was started using docker compose -f docker-compose.ec2.yml --env-file .env up -d, and services were verified with docker compose ps. The running application was successfully accessed via http://<EC2_PUBLIC_IPV4>:8080, confirming a live cloud deployment.

After completion of the assignment, both the SageMaker notebook instance and the EC2 instance were stopped to prevent unnecessary charges. The infrastructure was not deleted, as it will be reused in Module 8.

This module demonstrates secure AWS configuration, controlled IAM usage, S3-based object storage, boto3 integration within SageMaker, JSON data transformation, and production-style Docker Compose deployment on EC2.

NOTES: to know ec2 deployment read: EC2_DEPLOYMENT.md

Known bugs: No known bugs
