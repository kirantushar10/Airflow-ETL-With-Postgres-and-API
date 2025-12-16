# 🚀 **Airflow ETL Pipeline with Postgres & NASA API**
<div align="center">

✨ An end-to-end ETL pipeline built with Apache Airflow, Docker, PostgreSQL, and NASA’s APOD API ✨
Extract • Transform • Load • Orchestrate — fully automated data workflows

🛠️ Apache Airflow • 🐘 PostgreSQL • 🐳 Docker • 🌌 NASA APOD API • 🐍 Python

</div>

## 🌟 Overview

This project demonstrates a production-style ETL (Extract, Transform, Load) pipeline orchestrated using Apache Airflow.

The pipeline automatically retrieves data from NASA’s Astronomy Picture of the Day (APOD) API, processes the response, and stores structured data into a PostgreSQL database.
All services run inside Docker containers, providing a reproducible and isolated environment.

This project showcases real-world data engineering workflows, including API ingestion, transformation logic, database loading, and orchestration with Airflow DAGs.


## 🎯 Project Objectives

### ✔️ Build an automated ETL pipeline using Apache Airflow
### ✔️ Integrate an external REST API
### ✔️ Perform data transformation using Python
### ✔️ Load structured data into PostgreSQL
### ✔️ Use Docker for reproducible environments
### ✔️ Demonstrate Airflow hooks, operators, and DAG design

---

## 🧩 Core Components
### 🌀 Apache Airflow (Orchestration)

- Defines and schedules the ETL workflow using a DAG

- Manages task dependencies and execution order

- Provides monitoring, logging, retries, and failure handling

### 🌌 NASA APOD API (Data Source)

- The pipeline extracts data from NASA’s Astronomy Picture of the Day API.

- Data is fetched daily using HTTP requests.

### 🐘 PostgreSQL (Data Storage)

- Stores extracted and transformed data

- Runs inside a Docker container

- Uses Docker volumes for data persistence

- Interacted with using Airflow’s PostgresHook

### 🐳 Docker & Docker Compose

- Containerizes: Apache Airflow and PostgreSQL

- Ensures consistent environments across machines

- Simplifies setup and deployment

---

## 🔄 ETL Workflow
### 1️⃣ Extract

- Uses Airflow SimpleHttpOperator

- Sends a GET request to the NASA APOD API

- Retrieves JSON response containing astronomy metadata

### 2️⃣ Transform

- Implemented using Airflow TaskFlow API (@task)

- Parses and validates API response

- Extracts required fields

- Prepares data for database insertion

### 3️⃣ Load

- Uses Airflow PostgresHook

- Inserts transformed data into PostgreSQL

- Automatically creates the target table if it does not exist

## ⏰ Scheduling & Automation

- Pipeline runs on a daily schedule

- Task dependencies: Extract → Transform → Load order

- Airflow handles: Retries, Logging and Failure alerts

## 📁 Project Structure

```bash

.
├── dags/                  # Airflow DAG definitions
├── tests/dags/            # DAG tests
├── .astro/                # Astro CLI configuration
├── Dockerfile             # Custom Airflow image
├── docker-compose.yml     # Airflow + Postgres services
├── requirements.txt       # Python dependencies
├── packages.txt           # System-level packages
├── .gitignore
├── .dockerignore
├── LICENSE
└── README.md
```


