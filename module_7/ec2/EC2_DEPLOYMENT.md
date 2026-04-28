# EC2 Deployment

For Part 2 of this assignment, I deployed my Module 6 multi-container 
microservice architecture to an Amazon EC2 instance to demonstrate a 
cloud-based production-style deployment.

## EC2 Setup

I launched an EC2 instance using the Ubuntu 22.04 LTS AMI with instance 
type t3.micro. During setup, I created a new key pair for SSH access and 
configured the security group to allow:

- SSH (port 22)
- Application traffic (port 8080)

Access was restricted to my IP address only. PostgreSQL (5432) and 
RabbitMQ management (15672) were not exposed publicly.

## Connecting and Installing Docker

After the instance entered the “Running” state and passed status checks, 
I connected via SSH using my .pem key file.

I then installed Docker and Docker Compose by:
- Adding Docker’s official repository
- Installing Docker engine packages
- Enabling the Docker service
- Adding my user to the docker group

Installation was verified using:
- `docker --version`
- `docker compose version`

## Setting Up the Application

I created a directory: module_7/ec2/. Inside it, I added a compose file named: docker-compose.ec2.yml. 


This file defined four services:
- **db** (PostgreSQL 16)
- **rabbitmq** (RabbitMQ 3.13 management image)
- **web** (DockerHub image: israchowdhry/module_6:web-v1)
- **worker** (DockerHub image: israchowdhry/module_6:worker-v1)

The web service exposed port 8080. Both web and worker services used an 
external `.env` file for configuration.

I created the `.env` file with:
- POSTGRES_* variables
- DATABASE_URL
- RABBITMQ_URL

## Running the Application

I pulled the Docker images from DockerHub and started the stack: docker compose --env-file .env -f docker-compose.ec2.yml up -d


I verified container health using: docker compose ps


This confirmed PostgreSQL and RabbitMQ were healthy and all services 
were running.

## Troubleshooting Issues

During deployment, I encountered a 500 Internal Server Error when 
accessing the web application.

After inspecting logs with `docker logs`, I found that the `applicants` 
table did not exist in the database.

I resolved this by:
- Executing the `init.sql` file inside the PostgreSQL container
- Running `load_data.py` from the web container to populate data

I also corrected environment variable issues:
- Ensured DATABASE_URL and RABBITMQ_URL were set correctly
- Fixed RabbitMQ virtual host encoding

After recreating containers, the worker connected successfully and began 
processing tasks.

## Final Deployment

Once the database was initialized and populated, I restarted the web and 
worker services.

The application became accessible, confirming a successful deployment.

I captured screenshots of:
- EC2 instance
- Security group configuration
- `docker compose ps` output
- Live web application

## Shutdown

After verification, I stopped the EC2 instance to prevent unnecessary 
charges, as instructed.

The infrastructure remains intact for use in Module 8.

