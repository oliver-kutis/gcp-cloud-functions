import functions_framework
import requests
# Import HTTP basic auth from requests
from requests.auth import HTTPBasicAuth
from google.cloud import bigquery
import pandas as pd
import json
from io import StringIO
# from google.colab import auth
# auth.authenticate_user()

report_urls = [
    'https://www.esennce.cz/looker_reports/categories_report/2024.csv',
    'https://www.esennce.cz/looker_reports/categories_report/2023.csv',
    'https://www.esennce.cz/looker_reports/categories_report/2022.csv',
]
cols = [
    'Datum',
    'Kategorie',
    'Podkategorie',
    'Trzba',
    'Marze',
    'MarzePropad',
    'TrzbaHruba',
]


@functions_framework.http
def run(request):
    gcp_log(
        "NOTICE",
        f"----- Function started -----",
        dict(input_params=request.get_json(silent=True))
    )
    # Get login and password from the request body
    body = request.get_json(silent=True)
    login = body['login']
    password = body['password']
    project = body['project']
    dataset = body['dataset']
    table = body['table']

    # Create auth
    auth = HTTPBasicAuth(login, password)
    global_df = pd.DataFrame()

    for url in report_urls:
        try:
            response = requests.get(url, auth=auth)
            content = response.content.decode('utf-8')
            if response.status_code != 200:
                return gcp_log(
                    "ERROR",
                    f"Error while downloading data. Status code: {response.status_code}; Response body: {content}",
                    dict(
                        error_message=f"{content}",
                    )
                )
            # cr = csv.reader(content.splitlines(), delimiter=';')

        except Exception as e:
            return gcp_log(
                "ERROR",
                f"Error while while downloading data. Exception: {e}",
                dict(
                    error_message=f"{e}",
                )
            )

        try:
            df = pd.read_csv(StringIO(content), delimiter=';')
            # response.raise_for_status()
            # df = pd.read_csv(response.content.decode('utf-8'))

            print(f"Downloaded: {url} - {df.shape[0]} rows")
            global_df = pd.concat([global_df, df])

            global_df.columns = cols
            global_df_json = global_df.to_json(orient='records')
            global_df_json = json.loads(global_df_json)

            gcp_log("INFO", "Starting load to bigquery", dict())
            insert_data_into_bigquery(
                global_df_json, project, dataset, table)

        except Exception as e:
            return gcp_log(
                "ERROR",
                f"Error while processing / loading data to bigquery. Exception: {e}",
                dict(
                    error_message=f"{e}",
                )
            )

    return gcp_log("NOTICE", "----- Function finished successfully -----", dict())


def insert_data_into_bigquery(data, project_id, dataset_id, table_id):
    """Insert a list of dictionaries into BigQuery, creating the table if it doesn't exist."""

    try:
        # Initialize BigQuery client
        client = bigquery.Client(project=project_id)

        # Define the dataset and table references
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        # Check if the table exists
        try:
            table = client.get_table(table_ref)
            gcp_log("INFO", f"Table {dataset_id}.{table_id} exists.", dict())
        except Exception as e:
            # If the table does not exist, create it with the predefined schema
            gcp_log(
                "INFO", f"Table {dataset_id}.{table_id} not found. Creating table with provided schema...", dict())

            # Create the table with predefined schema
            table = bigquery.Table(table_ref, schema=get_bq_schema())
            table = client.create_table(table)  # Make an API request.
            gcp_log("INFO", f"Created table {dataset_id}.{table_id}.", dict())

        # Prepare the job configuration
        job_config = bigquery.LoadJobConfig(
            schema=get_bq_schema(),  # Use predefined schema with nested fields
            # Overwrite the existing data
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        # Load data into BigQuery
        gcp_log(
            "INFO", f"Loading data into {dataset_id}.{table_id}...", dict())
        job = client.load_table_from_json(
            data, table_ref, job_config=job_config)
        job.result()  # Wait for the job to complete

        return gcp_log("INFO",
                       f"Loaded {job.output_rows} rows into {dataset_id}.{table_id}.",
                       dict())

    except Exception as e:
        return gcp_log("ERROR",
                       f"Failed to load data into {dataset_id}.{table_id}. Exception: {e}",
                       dict(error_message=f"{e}"))


def get_bq_schema():
    """Manually define the BigQuery schema.

    Created by providing ChatGPT with the text of parameters and their
    types in the API docs:
      #reference/objednavky/objednavky/seznam-objednavek
      - link: https://upgatesapiv2.docs.apiary.io/
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


def gcp_log(severity, message, additional_log_fields=None):
    """
    Logs a message into Google Cloud Functions logs.

    Args:
        severity (str): The severity of the log message. Supported values are:
            "DEBUG", "INFO", "NOTICE", "WARNING", "ERROR", "CRITICAL", "ALERT",
            "EMERGENCY".
        message (str): The message to be logged.
        **kwargs: Additional keyword arguments to be added to the log entry.
    """
    if additional_log_fields is None:
        additional_log_fields = {}

    # Add client_name to additional_log_fields if it is present in inputs_dict
    # if 'client_name' in inputs_dict:
    #     additional_log_fields['client_name'] = inputs_dict['client_name']

    # additional_log_fields = {**GLOBAL_LOG_FIELDS, **additional_log_fields}

    log_entry = dict(
        severity=severity.upper(),
        message=message,
        pipeline_component="Esennce: Categories table generator",
        **additional_log_fields
    )

    print(json.dumps(log_entry))

    if severity.upper() == "ERROR":
        return ({"error": message, "details": additional_log_fields, }, 400)

    return ({"message": message, "details": additional_log_fields}, 200)
