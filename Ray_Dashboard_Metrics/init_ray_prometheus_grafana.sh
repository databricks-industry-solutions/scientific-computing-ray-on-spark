#!/usr/bin/env bash
###
# Script Name: init_ray_prometheus_grafana.sh
# Description: This script initializes a Global Ray Cluster and sets up Prometheus+Grafana metrics for use in the Ray dashboard exposed through the Databricks driver proxy.
# Testing: This script has been validated on both AWS & Azure Databricks on DBRs: 15.4 ML LTS, 16.1 ML
# Author: tj@databricks.com
# Date: 2025-01-29
# Version: 2.0
# Optional Modifications:
#   - Line 119: update Global Ray Cluster settings per https://docs.databricks.com/en/machine-learning/ray/ray-create.html#starting-a-global-mode-ray-cluster
#   - Line 145 & 161: update Prometheus+Grafana init files per your internal logging needs 
###

set -euo pipefail
IFS=$'\n\t'
cd "/"

# Version Identifiers and Ports
PROMETHEUS_VERSION="3.0.1"
GRAFANA_VERSION="9.2.4"
RAY_DASHBOARD_PORT="9999"

# PART 0: Helper functions
create_directory() {
  # Create attached disk directory if not exists
  local dir="$1"
  if [[ ! -d ${dir} ]]; then
    mkdir -p "${dir}"
  fi
}

determine_cloud() {
  # Determine cloud provider for current cluster
  local cloud_conf
  cloud_conf=$(grep -oP '(?<=databricks.instance.metadata.cloudProvider = ")[^"]*' /databricks/common/conf/deploy.conf) || {
    echo "[ERROR] Could not determine cloud provider."
    exit 1
  }
  echo "${cloud_conf}"
}

determine_org_id() {
  # Determine organization id for current workspace
  local org_id
  org_id=$(grep -oP '/organization\K[^?]+' /databricks/hive/conf/hive-site.xml) || {
    echo "[ERROR] Could not determine Org ID."
    exit 1
  }
  echo "${org_id}"
}

get_data_plane_url() {
  # Determine control plane URL from cloud provider
  local cloud="$1"
  local org_id="$2"
  if [[ "$cloud" == "Azure" ]]; then
    # Azure workspaces have CP URL in conf file
    cpurl_base=$(grep 'databricks.manager.defaultControlPlaneClientUrl' /databricks/common/conf/deploy.conf | awk -F'=' '{print $2}' | tr -d ' ') || {
      echo "[ERROR] Could not determine control plane URL for Azure workspace."
      exit 1
    }
    # Add dataplane component to URL before org ID
    dpurl=$(echo $cpurl_base | sed 's/\([0-9]\)/dp-\1/')
    echo "${dpurl}"
  elif [[ "$cloud" == "AWS" ]]; then
    # AWS workspaces have standardized URLs
    dpurl="dbc-dp-${org_id}.cloud.databricks.com"
    echo "${dpurl}"
  else
    echo "[ERROR] Cannot determine data plane URL from cloud input."
    exit 1
  fi
}

# PART 1: Initialize environment variables
BASE_DIR="/local_disk0/tmp"
create_directory $BASE_DIR

CLOUD=$(determine_cloud)
echo "[INFO] Using Cloud=$CLOUD"

ORG_ID=$(determine_org_id)
echo "[INFO] Using Org ID=$ORG_ID"

# Cluster ID available to init script as env variable (see docs)
CLUSTER_ID=$DB_CLUSTER_ID
echo "[INFO] Using Cluster Id=$CLUSTER_ID"

DPURL=$(get_data_plane_url $CLOUD $ORG_ID)
echo "[INFO] Using Data Plane URL=$DPURL"

# Determine Ray Dashboard port access to Grafana UI. Grafana uses port 3000 by default
IFRAME_HOST=https://"${DPURL//\"/}"/driver-proxy/o/"${ORG_ID//\"/}"/"${CLUSTER_ID//\"/}"/3000
echo "[INFO] Using ray_grafana_iframe_host=$IFRAME_HOST"
# Env variable must be set for Ray dashboard init
export RAY_GRAFANA_IFRAME_HOST=$IFRAME_HOST

# Determine how many worker nodes in cluster config from RAY_MAX_WORKERS environment variable
# NOTE: this must be set in the Databricks cluster config! Otherwise, set to 1
RAY_MAX_WORKERS=${RAY_MAX_WORKERS:-1}
echo "[INFO] Intializing Ray Cluster with max workers=$RAY_MAX_WORKERS"

# PART 2: Init global Ray on Spark cluster on Driver
if [[ ! -z $DB_IS_DRIVER ]] && [[ $DB_IS_DRIVER = TRUE ]] ; then

  cd "$BASE_DIR"
  # Install pyspark
  /databricks/python/bin/pip install -qq py4j pyspark

  # Create Python script for Ray global cluster. See Ray docs for full options.
  cat >$BASE_DIR/rayonsparkinit.py <<EOL
import os
del os.environ['DATABRICKS_RUNTIME_VERSION']

from pyspark.sql import SparkSession
from ray.util.spark import setup_global_ray_cluster

spark = SparkSession.builder.getOrCreate()

setup_global_ray_cluster(
    min_worker_nodes=1,
    max_worker_nodes=$RAY_MAX_WORKERS,
    is_blocking=False, 
    head_node_options={
            "dashboard_port":$RAY_DASHBOARD_PORT
            }
    )
EOL

  #Initialize Ray on Spark cluster via Python subprocess
  nohup python3 $BASE_DIR/rayonsparkinit.py > rayonsparkinit.log 2>&1 &
  PID=$!
  sleep 20
  if ! kill -0 "${PID}" 2>/dev/null; then
    echo "[ERROR] Ray on Spark init process terminated unexpectedly. Try executing Python code on running cluster first."
    exit 1
  fi
  # Ray on Spark cluster starts in <3 min
  sleep 120


  # PART 3: Config Prometheus and Grafana integration to Ray

  # Replace Prometheus yml file
  cat <<EOL > ${BASE_DIR}/ray/session_latest/metrics/prometheus/prometheus.yml 
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
# Scrape from each ray node as defined in the service_discovery.json provided by ray.
- job_name: 'ray'
  file_sd_configs:
  - files:
    - '$BASE_DIR/ray/prom_metrics_service_discovery.json'
EOL


  # Replace Grafana ini file
  cat <<EOL > ${BASE_DIR}/ray/session_latest/metrics/grafana/grafana.ini
[server]
domain = ${DPURL//\"/}
root_url = /driver-proxy/o/${ORG_ID//\"/}/${CLUSTER_ID//\"/}/3000/
serve_from_sub_path = false

[security]
allow_embedding = true

[auth.anonymous]
enabled = true
org_name = Main Org.
org_role = Viewer

[paths]
provisioning = $BASE_DIR/ray/session_latest/metrics/grafana/provisioning
EOL


  # PART 4: Get and start Prometheus and Grafana

  # Get Prometheus and Grafana
  sudo wget -q https://github.com/prometheus/prometheus/releases/download/v${PROMETHEUS_VERSION}/prometheus-${PROMETHEUS_VERSION}.linux-amd64.tar.gz -P ${BASE_DIR}/
  tar xfz ${BASE_DIR}/prometheus-*.tar.gz -C ${BASE_DIR}

  sudo wget -q https://dl.grafana.com/oss/release/grafana-${GRAFANA_VERSION}.linux-amd64.tar.gz -P ${BASE_DIR}/
  tar xfz ${BASE_DIR}/grafana-*.tar.gz -C ${BASE_DIR}

  # Start Prometheus with options
  nohup ${BASE_DIR}/prometheus*amd64/prometheus --config.file=${BASE_DIR}/ray/session_latest/metrics/prometheus/prometheus.yml > prometheus.log 2>&1 &

  # Wait until Prometheus is initialized
  sleep 30

  # Start Grafana with options
  nohup ${BASE_DIR}/grafana-${GRAFANA_VERSION}/bin/grafana-server --config=${BASE_DIR}/ray/session_latest/metrics/grafana/grafana.ini --homepath=${BASE_DIR}/grafana-${GRAFANA_VERSION} web > grafana.log 2>&1 &

  # Set Env variable for Ray dashboard URL
  RAY_DASHBOARD_URL="https://"${DPURL//\"/}"/driver-proxy/o/"${ORG_ID//\"/}"/"${CLUSTER_ID//\"/}"/${RAY_DASHBOARD_PORT//\"/}/#/overview"
  export RAY_DASHBOARD_URL=$RAY_DASHBOARD_URL

  echo "[INFO] Setup completed! Check logs in ${BASE_DIR}/ for details."
  echo "[INFO] Ray Dashboard is running. Access via: ${RAY_DASHBOARD_URL}"
  echo "[INFO] Grafana is running on port 3000. Access via: ${IFRAME_HOST}"

else
  # If running on worker nodes, no setup required as Ray handles cluster metrics
  sleep 1
fi