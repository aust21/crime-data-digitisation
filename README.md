# CrimeStats Digitization
A project to simulate the digitization of South African crime statistics, effectively storing them on secure cloud systems.

# Features
- Data Ingestion: Reads crime data from flat files (CSV, Excel, etc.).
- Data Storage: Loads processed data into a PostgreSQL database.
- Real-Time Processing: Fast access to frequently queried records using Redis caching.

# Tech Stack
- Python: Core programming language for data processing and pipeline management.
- PostgreSQL: Relational database to store digitized crime data.
- Redis: Used for caching and real-time data access.
- Google Cloud Storage: For storing raw crime data files (optional).

# Getting Started
## Prerequisites
Make sure you have the following installed:

- Python 3.x
- PostgreSQL
- Redis server
- Google Cloud SDK (if using Google Cloud Storage)

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