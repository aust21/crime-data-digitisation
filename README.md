# CrimeStats Digitization

A project to simulate the digitization of South African crime statistics, effectively storing them on secure cloud systems (AWS).

# Features

- Data Ingestion: Reads crime data from a flat file stored in an AWS bucket.
- Data Storage: Loads processed data into a PostgreSQL database on AWS.
- Real-Time Processing: Fast access to frequently queried records using Redis caching.

# Tech Stack

- Python: Core programming language for data processing and pipeline management.
- PostgreSQL: Relational database to store digitized crime data.
- Airflow: For scheduling and automating the pipeline

# Getting Started

## Prerequisites

Make sure you have the following installed:

- Python 3.x

### Installation

1. Clone the repo

```bash
git clone https://github.com/aust21/crime-stats-digitisation.git
cd crime-stats-digitisation
```

2. Create a virtual environment

- On Linux

```bash
python3 -m venv venv
source venv/bin/activate
```

- Windows

```bash
python -m venv venv
venv\Scripts\activate
```

3. Install dependencies

```bash
pip install -r requirements.txt
```
