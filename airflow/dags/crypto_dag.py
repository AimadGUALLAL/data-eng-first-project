
"""
DAG Crypto Data Pipeline
"""
import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

# IMPORTANT: Ajouter le chemin de votre projet
PROJECT_ROOT = r"/opt/airflow/crypto-data-pipeline"  # ← MODIFIER ICI
sys.path.insert(0, PROJECT_ROOT)

# Maintenant on peut importer vos modules
from config import Config
from src.pipeline import collect_coingecko_prices, collect_binance_tickers, collect_binance_ohlcv
import pandas as pd
import json
import hashlib
from io import StringIO

logger = logging.getLogger(__name__)





# Arguments par défaut
default_args = {
    'owner': '3imad',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}





def send_summary(**context):
    """Tâche finale: Résumé de la collecte"""
    ti = context['task_instance']
    
    prices = ti.xcom_pull(task_ids='collect_prices', key='prices_count') or 0
    tickers = ti.xcom_pull(task_ids='collect_tickers', key='tickers_count') or 0
    ohlcv = ti.xcom_pull(task_ids='collect_ohlcv', key='ohlcv_count') or 0
    
    summary = f"""
      Résumé Collection Crypto Data   : 

      * Exécution: {context['execution_date'].strftime('%Y-%m-%d %H:%M')}  
                                           
      * Prix CoinGecko:    {prices:4d} lignes     
      * Tickers Binance:   {tickers:4d} lignes     
      * OHLCV Binance:     {ohlcv:4d} lignes     
                                           
      TOTAL:             {prices + tickers + ohlcv:4d} lignes     
    
    """

    
    logger.info(summary)
    print(summary)
    
    return f"Total: {prices + tickers + ohlcv} records"


# Créer le DAG
with DAG(
    dag_id='crypto_data_collection',
    default_args=default_args,
    description='Collection crypto avec déduplication',
    schedule_interval='*/15 * * * *',  # Toutes les 15 minutes
    catchup=False,  # Ne pas exécuter les runs passés
    max_active_runs=1,  # Une seule exécution à la fois
    tags=['crypto', 'production', 'windows'],
) as dag:
    
    # Définir les tâches
    task_prices = PythonOperator(
        task_id='collect_prices',
        python_callable=collect_coingecko_prices,
        provide_context=True,
    )
    
    task_tickers = PythonOperator(
        task_id='collect_tickers',
        python_callable=collect_binance_tickers,
        provide_context=True,
    )
    
    task_ohlcv = PythonOperator(
        task_id='collect_ohlcv',
        python_callable=collect_binance_ohlcv,
        provide_context=True,
    )
    
    task_summary = PythonOperator(
        task_id='send_summary',
        python_callable=send_summary,
        provide_context=True,
    )
    
    # Définir les dépendances (tout en parallèle puis résumé)
    [task_prices, task_tickers, task_ohlcv] >> task_summary