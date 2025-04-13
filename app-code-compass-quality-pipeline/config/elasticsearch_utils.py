from elasticsearch import Elasticsearch
import json
import os
from logging_utils import log_structured

def save_metrics_job_fail(metrics: dict):
    es_host = os.getenv("ES_HOST", "http://elasticsearch:9200")
    es_user = os.getenv("ES_USER")
    es_pass = os.getenv("ES_PASS")
    es_index = "compass_dt_datametrics_fail"

    es = Elasticsearch([es_host], basic_auth=(es_user, es_pass))

    try:
        es.index(index=es_index, document=metrics)
        log_structured(level="INFO", message="Métricas de falha salvas no Elasticsearch")
    except Exception as e:
        log_structured(level="ERROR", message="Erro ao salvar métricas no Elasticsearch", details={"error": str(e)})