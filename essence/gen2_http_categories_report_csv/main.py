import functions_framework
import requests
from requests.auth import HTTPBasicAuth
from google.cloud import bigquery
import pandas as pd
from io import StringIO

report_urls = [
    'https://www.esennce.cz/looker_reports/categories_report/2024.csv',
    'https://www.esennce.cz/looker_reports/categories_report/2023.csv',
    'https://www.esennce.cz/looker_reports/categories_report/2022.csv',
]


# @functions_framework.http
def run(request):
    # Get login and password from the request body
    # body = request.get_json(silent=True)
    body = {
        "login": "looker",
        "password": "jf8nc.aol39af-l6ogq",
    }
    login = body['login']
    password = body['password']
    print("request", request)
    print("BODY", body)
    print("login", login)
    print("password", password)

    # Create auth
    auth = HTTPBasicAuth(login, password)
    global_df = pd.DataFrame()

    for url in report_urls:
        response = requests.get(url, auth=auth)
        content = response.content.decode('utf-8')
        # cr = csv.reader(content.splitlines(), delimiter=';')

        df = pd.read_csv(StringIO(content), delimiter=';')
        df.info()
        # response.raise_for_status()
        # df = pd.read_csv(response.content.decode('utf-8'))

        print(f"Downloaded: {url} - {df.shape[0]} rows")
        global_df = pd.concat([global_df, df])

    print(f"Total rows: {global_df.shape[0]}")
    global_df_json = global_df.to_json(orient='records')

    # insert to bigquery
    insert_data_into_bigquery(
        global_df_json, "essence", "categories_report", "categories_report_table")

    return "OK"


def insert_data_into_bigquery(data, project_id, dataset_id, table_id):
    """Insert a list of dictionaries into BigQuery, creating the table if it doesn't exist."""

    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Define the dataset and table references
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    # Check if the table exists
    try:
        table = client.get_table(table_ref)
        print(f"Table {dataset_id}.{table_id} exists.")
    except Exception as e:
        # If the table does not exist, create it with the predefined schema
        print(
            f"Table {dataset_id}.{table_id} not found. Creating table with provided schema...")

        # Create the table with predefined schema
        table = bigquery.Table(table_ref, schema=get_bq_schema())
        table = client.create_table(table)  # Make an API request.
        print(f"Created table {dataset_id}.{table_id}.")

    # Prepare the job configuration
    job_config = bigquery.LoadJobConfig(
        schema=get_bq_schema(),  # Use predefined schema with nested fields
        # Overwrite the existing data
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # Load data into BigQuery
    job = client.load_table_from_json(data, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete

    print(f"Loaded {job.output_rows} rows into {dataset_id}:{table_id}.")


def get_bq_schema():
    """Manually define the BigQuery schema.

    Created by providing ChatGPT with the text of parameters and their
    types in the API docs:
      - link: https://upgatesapiv2.docs.apiary.io/#reference/objednavky/objednavky/seznam-objednavek
    """
    schema = [
        # Root-level fields
        bigquery.SchemaField("Datum", "DATE"),
        bigquery.SchemaField("Kategorie", "STRING"),
        bigquery.SchemaField("Podkategorie", "STRING"),
        bigquery.SchemaField("Trzba", "FLOAT"),
        bigquery.SchemaField("Marze", "FLOAT"),
        bigquery.SchemaField("MarzePropad", "FLOAT"),
        bigquery.SchemaField("TrzbaHruba", "FLOAT"),
    ]

    return schema
