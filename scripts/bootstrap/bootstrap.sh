#!/bin/bash
set -x
set -e 

# Install mandatory packages
sudo yum install -y python3-pip
sudo yum install -y git

# Install and activate Python virtual environment
python3 -m pip install --user virtualenv
python3 -m venv airflow_virtual
source airflow_virtual/bin/activate

# Install PostgreSQL
sudo dnf install -y postgresql-server
sudo postgresql-setup --initdb
sudo systemctl start postgresql.service
sudo systemctl enable postgresql.service

# Change password Encryption in PostgreSQL
sudo sed -i 's/^#password_encryption = md5/password_encryption = scram-sha-256/' /var/lib/pgsql/data/postgresql.conf
sudo echo 'host    all             all             0.0.0.0/0            scram-sha-256' | sudo tee -a /var/lib/pgsql/data/pg_hba.conf
sudo systemctl restart postgresql.service

# Create a new user and database in PostgreSQL
sudo -u postgres psql -c "CREATE DATABASE airflow_db;"
sudo -u postgres psql -c "CREATE USER airflow WITH PASSWORD 'airflow-testing_12';"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow;"

# Install Redis for broker
sudo dnf install -y redis
sudo systemctl start redis
sudo systemctl enable redis

# Airflow installation
mkdir -p /home/airflow/airflow_home
export AIRFLOW_HOME=/home/airflow/airflow_home

export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow:airflow-testing_12@127.0.0.1:5432/airflow_db"
export AIRFLOW__CORE__EXECUTOR=CeleryExecutor
export AIRFLOW__CELERY__RESULT_BACKEND="db+postgresql://airflow:airflow-testing_12@127.0.0.1:5432/airflow_db"
export AIRFLOW__CELERY__BROKER_URL="redis://:@127.0.0.1:6379/0"
export AIRFLOW__CORE__FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
export AIRFLOW__CORE__LOAD_EXAMPLES='false'
export AIRFLOW__LOGGING__BASE_LOG_FOLDER="/home/airflow/airflow_home/logs"

echo "export AIRFLOW_HOME=/home/airflow/airflow_home" >> ~/.bashrc

pip install "apache-airflow[celery]==2.7.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.1/constraints-3.9.txt"
pip install psycopg2-binary

# Initialize Airflow database
airflow db init

# Start Airflow services
airflow webserver -D
airflow scheduler -D
airflow celery worker -D
airflow flower -D

# Create an Airflow user
airflow users create --username airflow --firstname airflow --lastname test --role Admin --email data.analytics@cashflows.com

# Create default connections
airflow connections create-default-connections

echo "Airflow setup complete."