import os

RDS_CONFIG = {
    "host":     os.environ.get("RDS_HOST", ""),
    "port":     int(os.environ.get("RDS_PORT", 5432)),
    "dbname":   os.environ.get("RDS_NAME", "postgres"),
    "user":     os.environ.get("RDS_USER", "pipeline_user"),
    "password": os.environ.get("RDS_PASSWORD", "")
}
