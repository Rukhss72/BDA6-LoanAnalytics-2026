## HDFS Raw Ingestion Proof

HDFS services running (evidence: jps shows NameNode/DataNode/SecondaryNameNode).

Created HDFS zones:
- /loan_project/raw
- /loan_project/curated

Uploaded raw dataset (UNCHANGED):
- /loan_project/raw/loan.csv
- Size: ~1.1G
- Row count (excluding header): 2,260,668

Reproducible ingestion command:
- ingest/ingest_raw.sh ~/loan.csv
