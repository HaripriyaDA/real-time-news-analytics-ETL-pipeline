# real-time-news-analytics-ETL-pipeline

An end-to-end Big Data engineering pipeline that ingests, streams, serializes, and analyzes live global news data using a decoupled producer/consumer architecture.

## Architecture Overview
1. **Data Ingestion (Producer):** A Python script queries the third-party REST NewsAPI for specific keyword payloads and streams targeted JSON fields to an active Apache Kafka cluster.
2. **Data Streaming & Storage (Consumer):** A Python-based Kafka Consumer polls the message broker topic, extracts the string data, structure-formats it into a clean CSV, and commits the finalized data directly to a Hadoop HDFS cluster.
3. **Data Analytics (Apache Hive):** An external data warehouse schema points directly to the HDFS location, executing MapReduce workloads via HiveQL to aggregate trend insights and track news volume metrics.

## Tech Stack
* **Languages:** Python, SQL (HiveQL)
* **Big Data & Streaming Frameworks:** Apache Kafka, Apache Hive, Hadoop HDFS
* **Development Environments:** Jupyter Notebook, PyCharm, Linux/GCP SSH CLI

## Repository Structure
* `NewsAPI_Getting_Articles.ipynb`: Initial API endpoint testing and exploratory data payloads.
* `Mid_Producer.py`: Production script handling live client configuration, automated keyword fetches, and Kafka payload serialization.
* `Mid_Consumer.py`: Multi-record polling script tracking messaging brokers, file system exports, and HDFS directory routing.

## Sample Analytical Queries Executed (Hive)
```sql
-- Aggregating volume metrics by media source
SELECT source, COUNT(*) AS total_articles
FROM NewsTable
WHERE source IN ('CNN', 'NBC News', 'Fox News', 'USA Today')
GROUP BY source
ORDER BY total_articles DESC;
