FROM astrocrpublic.azurecr.io/runtime:3.1-9

RUN pip install \
    apache-airflow-providers-http \
    apache-airflow-providers-postgres
