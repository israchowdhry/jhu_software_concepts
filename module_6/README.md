SSH url: SSH url: git@github.com:israchowdhry/jhu_software_concepts.git


Dockerhub repo link: https://hub.docker.com/repository/docker/israchowdhry/module_6/general


Name: Isra Chowdhry (ichowdh6)

Module Info: Module 6 Assignment: Deploy Anywhere Assignment  Due on 03/01/2026 at 11:59 EST

Approach:

In Module 6, we redesigned our previous synchronous ETL pipeline into a distributed, asynchronous architecture using RabbitMQ, a background worker process, and PostgreSQL. The primary goal of this redesign was to decouple user interactions from heavy data processing tasks so that the web application remains responsive while ingestion and analytics work execute independently. Rather than performing scraping and database insertion directly within the Flask request lifecycle (as in Module 5), the system now follows a message-driven architecture in which the web application publishes tasks to a message broker and a dedicated worker consumes and processes those tasks.

The system is deployed using Docker Compose and consists of five services: a Flask web service, a RabbitMQ message broker, a worker container, a PostgreSQL database, and a seed container used for initial data loading. The Flask web application exposes two endpoints: POST /pull-data and POST /update-analysis. When a user presses a button in the UI, the web service does not perform any scraping or database work directly. Instead, it publishes a durable message to a RabbitMQ direct exchange named tasks, which is bound to a durable queue named tasks_q using the routing key tasks. These AMQP entities are declared as durable so that messages survive container restarts. The web service returns HTTP 202 (Accepted), indicating that the request has been queued for background processing. This design ensures that the UI remains responsive and that long-running operations do not block HTTP threads.

The worker service is implemented as a long-running Python process that connects to RabbitMQ using the RABBITMQ_URL environment variable. Upon startup, it declares the same durable exchange and queue, ensuring idempotent AMQP configuration. To enforce backpressure and prevent concurrent database conflicts, the worker configures basic_qos(prefetch_count=1), which guarantees that only one message is processed at a time. Each incoming message contains JSON with a kind field and an optional payload. The worker routes messages using a task map that dispatches "scrape_new_data" to handle_scrape_new_data() and "recompute_analytics" to handle_recompute_analytics().

Each message is processed inside its own PostgreSQL transaction using psycopg. When a message is received, the worker opens a database connection using DATABASE_URL, invokes the appropriate handler, and commits only if the handler completes successfully. If an exception occurs, the transaction is rolled back and the message is negatively acknowledged with basic_nack(requeue=False) to prevent infinite retry loops. Successful transactions result in basic_ack, ensuring at-least-once delivery semantics while maintaining data integrity.

The core ingestion logic is implemented in handle_scrape_new_data(). To support incremental ingestion, we implemented a watermarking system using a dedicated table named ingestion_watermarks, which stores a source, a last_seen value, and an updated_at timestamp. At the beginning of each ingestion task, the worker reads the current watermark for the data source. It then loads raw records from a mounted JSONL file (used as the data source in this module) and filters them to include only records newer than the watermark. This filtering step iterates through the dataset, compares each record’s identifier (such as URL or timestamp) to the stored watermark, and tracks the maximum identifier encountered. The time complexity of this operation is O(n), where n is the number of records.

After filtering, each record is normalized to match the schema of the applicants table. Normalization ensures consistent formatting of program names (constructed as "university - program_name"), consistent handling of GRE and GPA fields, and compatibility with both dashed and underscored LLM-generated field names. The normalized records are batch-inserted into PostgreSQL using parameterized SQL statements to prevent SQL injection. To guarantee idempotency, inserts use ON CONFLICT (url) DO NOTHING, leveraging a UNIQUE constraint on the url column to prevent duplicate entries. Only after a successful insert operation does the system advance the watermark to the maximum identifier observed in the batch. This ensures that the watermark always reflects committed data and prevents skipped or partially processed records.

The handle_recompute_analytics() function recomputes database-level statistics used by the UI. In this module, we execute ANALYZE applicants to refresh PostgreSQL planner statistics. In a production environment, this step could refresh materialized views or recompute summary tables. Importantly, this handler also runs inside a dedicated transaction and follows the same commit/acknowledge pattern as ingestion tasks.

The PostgreSQL schema is designed with reliability and idempotency in mind. The applicants table uses a SERIAL primary key and a UNIQUE constraint on url, which serves as a natural identifier for each application entry. All inserts are parameterized, and destructive updates are avoided. The watermark table provides incremental ingestion capability and ensures that repeated task execution does not duplicate records.

Overall, this architecture introduces distributed systems principles absent from the previous module, including message-driven processing, durable queues, idempotent consumers, transactional integrity, and backpressure control. By separating the web layer from the ETL layer and introducing RabbitMQ as a broker, the system becomes more scalable, fault-tolerant, and production-ready. The web application remains lightweight and responsive, while heavy data processing occurs safely in the background. This approach significantly improves reliability and aligns with real-world event-driven architecture patterns.

Known bugs: No known bugs 

