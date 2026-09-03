# Custom Airflow image for the job market pipeline.
#
# Replaces _PIP_ADDITIONAL_REQUIREMENTS, which installed dependencies on every
# container start. That was slow, non-deterministic, and could not install the
# dbt version this project is developed against: the stock apache/airflow:2.8.0
# image runs Python 3.8, while dbt-core 1.11 requires Python >= 3.9. Left to
# resolve freely, pip fell back to a dbt-core 1.9.0 beta and also bumped
# Airflow's pinned Jinja2.
#
# Two deliberate choices here:
#   1. Same Airflow version (2.8.0), just the python3.11 variant. Changing only
#      the interpreter avoids a metadata database migration.
#   2. dbt lives in its own virtualenv at /opt/dbt-venv. dbt and Airflow share
#      transitive dependencies (Jinja2, click, protobuf) with incompatible
#      pins, so keeping them in separate environments means neither can break
#      the other. The DAG calls /opt/dbt-venv/bin/dbt by absolute path; the venv
#      is intentionally NOT added to PATH, so it cannot shadow Airflow's python.

FROM apache/airflow:2.8.0-python3.11

# Dependencies the PythonOperator tasks import, so these must live in Airflow's
# own environment.
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt

# dbt, isolated. Pinned to match the versions used for local development so the
# DAG and a developer laptop run identical transformation code.
COPY requirements-dbt.txt /tmp/requirements-dbt.txt

# The image runs as the unprivileged `airflow` user, which cannot write to
# /opt. Briefly become root to create the directory, then drop back so the
# venv itself is built and owned by the user that will run it.
USER root
RUN mkdir -p /opt/dbt-venv && chown airflow:root /opt/dbt-venv
USER airflow

# PIP_USER=false is required: the Airflow image sets PIP_USER=true globally, and
# a --user install is illegal inside a virtualenv.
RUN PIP_USER=false python -m venv /opt/dbt-venv \
 && PIP_USER=false /opt/dbt-venv/bin/pip install --no-cache-dir --upgrade pip \
 && PIP_USER=false /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt \
 && /opt/dbt-venv/bin/dbt --version
