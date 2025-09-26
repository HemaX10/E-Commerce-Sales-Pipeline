FROM apache/airflow:2.7.1

# RUN apt-get update && \
#     apt-get install -y --no-install recommends \
#     build-essential \ 
#     libpq-dev \ 
#     git \ 
#     && apt-get clean && \
#     rm -rf /var/lib/apt/lists/*


RUN python -m pip install --upgrade pip && \
    pip install \
    dbt-core==1.8.2 \
    dbt-postgres==1.8.2 \
    pandas \
    requests \
    psycopg2-binary \
    openpyxl \
    pytz

WORKDIR /usr/src/dbt

COPY . . 

ENV DBT_PROFILES_DIR=/usr/src/dbt

CMD ["python" , "-m" , "dbt" , "--help"]