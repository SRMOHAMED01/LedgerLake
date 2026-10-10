"""
OpenMetadata Ingestion Script (Palier F)
This script demonstrates how to configure and trigger metadata ingestion for the LedgerLake project.
It relies on the `openmetadata-ingestion` Python package.

Prerequisites:
    pip install "openmetadata-ingestion[dbt,datalake]"

Usage:
    python ingest_metadata.py
"""

import yaml
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.ometa.ometa_api import server_config

# Configuration to connect to the local OpenMetadata server
server_config_dict = {
    "hostPort": "http://localhost:8585/api",
    "authProvider": "openmetadata",
    "securityConfig": {
        "jwtToken": "<REPLACE_WITH_YOUR_OM_ADMIN_TOKEN>"
    }
}

# Example YAML workflow configuration for MinIO Data Lake
datalake_ingestion_config = """
source:
  type: datalake
  serviceName: ledgerlake_minio
  serviceConnection:
    config:
      type: Datalake
      configSource:
        securityConfig:
          awsAccessKeyId: ledgerlake
          awsSecretAccessKey: changeme
          awsRegion: us-east-1
          endPointURL: http://localhost:9000
  sourceConfig:
    config:
      type: DatabaseMetadata
      markDeletedTables: true
sink:
  type: metadata-rest
  config: {}
workflowConfig:
  openMetadataServerConfig:
    hostPort: http://localhost:8585/api
    authProvider: openmetadata
    securityConfig:
      jwtToken: "<REPLACE_WITH_YOUR_OM_ADMIN_TOKEN>"
"""

def main():
    print("Initialize OpenMetadata connection...")
    # ometa = OpenMetadata(server_config.OpenMetadataConnection.parse_obj(server_config_dict))
    # print(f"Connected successfully. Version: {ometa.health_check()}")
    
    print("To run the ingestion, save the YAML config to a file (e.g. datalake.yaml) and run:")
    print("  metadata ingest -c datalake.yaml")
    
    print("\nNext, you should also create a dbt.yaml ingestion config to link the dbt models (Gold Layer) to the Silver tables.")

if __name__ == "__main__":
    main()
