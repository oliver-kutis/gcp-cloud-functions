import functions_framework
import requests
from google.cloud import bigquery
import json

base_url = 'https://eyerim.sk/backend/api/crm-data/'

@functions_framework.http
def run(request):
    gcp_log(
        "NOTICE",
        f"----- Function started -----",
        dict(input_params=request.get_json(silent=True))
    )
    # Get token and query params from request
    body = request.get_json(silent=True)

    try: 
        params = {
            "date-from": body['date_from'],
            "date-to": body['date_to'],
            "country": body['country'],
        }
        headers = {
            "Authorization": body['auth_token'],
            "Content-Type": "application/json"
        }
        
        response = requests.get(base_url, params=params, headers=headers)
        if response.status_code != 200:
            return gcp_log(
                "ERROR",
                f"Error while downloading data. Status code: {response.status_code};",
                dict(
                    error_message=f"{response.json()}",
                )
            )
        data = response.json()

        gcp_log("INFO", f"Downloaded: {base_url} - {len(data)} rows", dict())

        bq_result = insert_data_into_bigquery(
            data, 
            project_id="datalake-mktg",
            dataset_id="sales_l1",
            table_id="crm_orders"
        )


        if (bq_result[1] == 400):
            return bq_result

        return gcp_log("NOTICE", "----- Function finished successfully -----", dict())

    except Exception as e:
        return gcp_log(
            "ERROR",
            f"Error while downloading / parsing data. Exception: {e}",
            dict(
                error_message=f"{e}",
            )
        )

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
        bigquery.SchemaField("OrderId", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("OrderStatus", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("OrderDate", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("OrderCurrency", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("OrderCountry", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("OrderSource", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ProductLine", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ProductLineRevenue", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("ProductLineGrossMargin", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("ProductQuantity", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField(
            "CustomerDetails",
            "RECORD",
            mode="NULLABLE",
            fields=[
                bigquery.SchemaField("CustomerId", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("CustomerName", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("CustomerEmail", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("CustomerPhone", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("CustomerStreet", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("CustomerCity", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("CustomerCountry", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("CustomerZip", "STRING", mode="NULLABLE"),
            ],
        ),
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
        **additional_log_fields
    )

    print(json.dumps(log_entry))

    if severity.upper() == "ERROR":
        return ({"error": message, "details": additional_log_fields, }, 400)

    return ({"message": message, "details": additional_log_fields}, 200)
