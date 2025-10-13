# Base image avec Python 3.11
FROM apache/airflow:2.8.0-python3.11

# Métadonnées
# LABEL maintainer="your-email@company.com"
# LABEL description="Custom Airflow image for Crypto Data Pipeline"
# LABEL version="1.0.0"

# Copier les requirements
COPY airflow/requirements.txt /requirements.txt

# Passer en mode root pour installer des packages
USER root

# Installation de packages système (si nécessaire)
# Décommenter si besoin:
# RUN apt-get update && apt-get install -y \
#     gcc \
#     g++ \
#     postgresql-client \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/*

# Installer les dépendances Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

# Revenir à l'utilisateur airflow (sécurité)
USER airflow

# Healthcheck pour vérifier que Python fonctionne
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import airflow; print('Airflow OK')" || exit 1

# Point d'entrée par défaut (hérité de l'image de base)
# ENTRYPOINT ["/entrypoint"]