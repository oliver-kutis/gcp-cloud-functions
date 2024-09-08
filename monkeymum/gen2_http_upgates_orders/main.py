import functions_framework
import requests
from requests.auth import HTTPBasicAuth
from google.cloud import bigquery
import json
# Authenticate to Google Cloud (cez data@niftyminds.cz)
# from google.colab import auth
# auth.authenticate_user()

# UPGates API credentials
UPGATES_LOGIN = "09348859"
UPGATES_API_KEY = "dKfhVNHiGlghQRpnMYe4"
UPGATES_API_URL = "https://monkey-mum.admin.s12.upgates.com/api/v2/orders"

DEFAULTS_ARG_VALUES = {
    "start_date": "2022-01-01",
    "end_date": None,
    # 'keboola_endpoint_url': 'https://queue.north-europe.azure.keboola.com',
    # 'keboola_job_max_runtime_seconds': 360,
}
GLOBAL_LOG_FIELDS = {
    # 'pipeline_phase': 'ERP - Upgates - Orders'
}
FUNCTION_ARGS = {
    "required": ['pipeline_phase', 'execution_id', 'client_name', 'upgates_api_url',
                 'upgates_login', 'upgates_api_key', 'bigquery_config'],
    "optional": ['start_date', 'end_date']
}

# TODO: Handle these fields with a different login in the check_request_args function
BIGQUERY_CONFIG_ARGS = [
    'project_id', 'dataset_id', 'table_id',  # 'schema_storage_location'
]


# FUNCTION_ARGS = {
#     "required": ['execution_id', 'keboola_orchestration_trigger_run_id', 'client_name', 'keboola_storage_api_token'],
#     "optional": ['keboola_job_max_runtime_seconds', 'keboola_endpoint_url']
# }

inputs_dict = dict()


@functions_framework.http
def run(request):
    """
    This function is an HTTP Cloud Function that expects an HTTP POST request with
    a JSON body containing the following parameters:
    {
        "pipeline_phase": "ERP - Upgates - Orders",
        "execution_id": "123456789",
        "client_name": "client_name",
        "upgates_api_url": "https://monkey-mum.admin.s12.upgates.com/api/v2/orders",
        "upgates_login": "123456789",
        "upgates_api_key": "API_KEY",
        (optional) "start_date": "2022-01-01", (default: 2022-01-01)
        (optional) "end_date": "2022-02-01", (default: None)
        "bigquery_config": {
            "project_id": "PROJECT_ID",
            "dataset_id": "DATASET_ID",
            "table_id": "TABLE_ID",
            TODO: "schema_storage_location" : "gs://BUCKET_NAME"    
        }
    }
    """

    # Check the request arguments
    check_request_args_result = check_request_args(request)
    if (check_request_args_result[1] == 400):
        return check_request_args_result
    else:
        inputs_dict = check_request_args_result[0]

    gcp_log(
        "INFO",
        "----- Function started -----",
        dict(input_params=request.get_json(silent=True))
    )

    get_orders_result = get_orders(
        start_date=inputs_dict['start_date'], end_date=inputs_dict['end_date'])

    if (get_orders_result[1] == 400):
        return get_orders_result
    else:
        orders = get_orders_result[0]

    # Project: 'monkeymum'
    # Dataset: 'upgates'
    # Table: 'orders'
    bq_result = insert_data_into_bigquery(
        orders, bq_config=inputs_dict["bigquery_config"])

    if (bq_result[1] == 400):
        return bq_result

    gcp_log(
        "INFO",
        "----- Function finished -----",
        dict(input_params=request.get_json(silent=True))
    )

    return "Function finished."


# def insert_data_into_bigquery(data, project_id, dataset_id, table_id):
#     """Insert a list of dictionaries into BigQuery, creating the table if it doesn't exist."""

#     # Initialize BigQuery client
#     client = bigquery.Client(project=project_id)

#     # Define the dataset and table references
#     dataset_ref = client.dataset(dataset_id)
#     table_ref = dataset_ref.table(table_id)

#     # Check if the table exists
#     try:
#         table = client.get_table(table_ref)
#         print(f"Table {dataset_id}.{table_id} exists.")
#     except Exception as e:
#         # If the table does not exist, create it with the predefined schema
#         print(
#             f"Table {dataset_id}.{table_id} not found. Creating table with provided schema...")

#         # Create the table with predefined schema
#         table = bigquery.Table(table_ref, schema=get_bq_schema())
#         table = client.create_table(table)  # Make an API request.
#         print(f"Created table {dataset_id}.{table_id}.")

#     # Prepare the job configuration
#     job_config = bigquery.LoadJobConfig(
#         schema=get_bq_schema(),  # Use predefined schema with nested fields
#         # Overwrite the existing data
#         write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
#     )

#     # Load data into BigQuery
#     job = client.load_table_from_json(data, table_ref, job_config=job_config)
#     job.result()  # Wait for the job to complete

#     print(f"Loaded {job.output_rows} rows into {dataset_id}:{table_id}.")

def insert_data_into_bigquery(data, bq_config):
    """Insert a list of dictionaries into BigQuery, creating the table if it doesn't exist."""

    try:
        # Get BigQuery configuration
        project_id = bq_config['project_id']
        dataset_id = bq_config['dataset_id']
        table_id = bq_config['table_id']

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


def get_orders(start_date=None, end_date=None):
    """
      Fetch orders from Upgates API between given start and end dates.
    """
    try:
        # Create authentication
        auth_header = HTTPBasicAuth(UPGATES_LOGIN, UPGATES_API_KEY)
        # API params for start and end dates
        params = {
            "creation_time_from": start_date,
            "creation_time_to": end_date,
        }

        gcp_log(
            "INFO",
            "Fetching orders...",
            dict(
                start_date=start_date,
                end_date=end_date
            )
        )

        try:
            # Make an initial API call to get the number of pages
            response = requests.get(
                UPGATES_API_URL, auth=auth_header, params=params)

            # Handle API response
            if response.status_code == 200:
                # Extract data from the response
                data = response.json()
                # Array of orders, will be extented with other pages
                orders = data['orders']
                # Get number of pages for the date range
                number_of_pages = data['number_of_pages']
            else:
                return gcp_log(
                    "ERROR",
                    f"Error while fetching data from API (page = 1). Status code: {response.status_code}",
                    dict(
                        error_message=f"{response.text}",
                    )
                )

        except Exception as e:
            return gcp_log(
                "ERROR",
                f"Error while fetching data from API (page = 1). Exception: {e}",
                dict(
                    error_message=f"{e}",
                )
            )

            # The API is pages, so iterate the pages after initial request where
            # page is set to 1. Done to determine number of pages for future
            # requests. Notice that loops start from the page 2 and not 1 (already loaded)
        for page in range(1, number_of_pages + 1):
            # Print every 10th page about the progress
            if page % 10 == 0:
                gcp_log(
                    "INFO",
                    f"Page {page} of {number_of_pages}",
                    dict(
                        page=page,
                        number_of_pages=number_of_pages
                    )
                )

            # Set the page param for the API call
            params['page'] = page
            # Make the API call
            response = requests.get(
                UPGATES_API_URL, auth=auth_header, params=params)
            # Handle API response
            if response.status_code == 200:
                # Extract data from the response
                temp_data = response.json()

                # Extend the orders array so it is filled
                # with all data after the loop is finished
                if 'orders' in temp_data:
                    orders += temp_data['orders']
            else:
                return gcp_log(
                    "ERROR",
                    f"Error while fetching data from API (page = {page}). Status code: {response.status_code}",
                    dict(
                        error_message=f"{response.text}",
                    )
                )

    except Exception as e:
        return gcp_log(
            "ERROR",
            f"Error while fetching data from API. Exception: {e}",
            dict(
                error_message=f"{e}",
            )
        )

    gcp_log(
        "INFO",
        f"Orders fetched: {len(orders)}",
        dict(
            start_date=start_date,
            end_date=end_date
        )
    )

    return (orders, 200)


def check_request_args(request):
    request_json = request.get_json(silent=False)
    inputs_dict = dict()
    job_phase = "check_request_args"
    # Log that the check of args started
    gcp_log(
        "INFO",
        "Checking the request args...",
        dict(job_phase="check_request_args")
    )
    if not request_json:
        return gcp_log(
            "ERROR",
            "The body of the request is missing. Please enter a body with valid values for: {FUNCTION_ARGS}.",
            dict(job_phase=job_phase,
                 job_phase_detail="request_body")
        )

    # Check optional args
    for arg in FUNCTION_ARGS['optional']:
        if arg in request_json and request_json[arg] is not None:
            inputs_dict[arg] = request_json[arg]
        else:
            inputs_dict[arg] = DEFAULTS_ARG_VALUES[arg]
            gcp_log(
                "WARNING",
                f"The '{arg}' wasn't provided or the value is None. Using default value = '{DEFAULTS_ARG_VALUES[arg]}' instead.",
                dict(job_phase=job_phase,
                     job_phase_detail="optional_args",
                     arg_details={
                         "name": arg,
                         "value": DEFAULTS_ARG_VALUES[arg]
                     })
            )

    # Check required args
    for arg in FUNCTION_ARGS['required']:
        if arg in request_json and request_json[arg] is not None:
            inputs_dict[arg] = request_json[arg]
            if arg in ['client_name', 'execution_id']:
                GLOBAL_LOG_FIELDS[arg] = request_json[arg]
            if arg == 'bigquery_config':
                for bq_arg in BIGQUERY_CONFIG_ARGS:
                    if bq_arg in request_json[arg] and request_json[arg][bq_arg] is not None:
                        inputs_dict[bq_arg] = request_json[arg][bq_arg]
                    else:
                        return gcp_log(
                            "ERROR",
                            f"The required {arg}.{bq_arg} wasn't provided or the value is None. Please enter a value for '{bq_arg}'.",
                            dict(job_phase=job_phase,
                                 job_phase_detail="required_args",
                                 arg_details={
                                     "name": f"{arg}.{bq_arg}",
                                     "value": request_json[arg].get(bq_arg),

                                 })
                        )
        else:
            return gcp_log(
                "ERROR",
                f"The required '{arg}' wasn't provided or the value is None. Please enter a value for '{arg}'.",
                dict(job_phase=job_phase,
                     job_phase_detail="required_args",
                     arg_details={
                         "name": arg,
                         "value": request_json.get(arg),

                     })
            )

    GLOBAL_LOG_FIELDS['input_params'] = inputs_dict
    gcp_log(
        "NOTICE",
        f"All request arguments checked. Parameters: {inputs_dict}",
        {"parameters_all": inputs_dict}
    )

    return (inputs_dict, 200)


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

    additional_log_fields = {**GLOBAL_LOG_FIELDS, **additional_log_fields}

    log_entry = dict(
        severity=severity.upper(),
        message=message,
        component="Keboola Orchestration-v2 Trigger",
        **additional_log_fields
    )

    print(json.dumps(log_entry))

    if severity.upper() == "ERROR":
        return ({"error": message, "details": additional_log_fields}, 400)

    return ({"message": message, "details": additional_log_fields}, 200)


def get_bq_schema():
    """Manually define the BigQuery schema.

    Created by providing ChatGPT with the text of parameters and their
    types in the API docs:
      - link: https://upgatesapiv2.docs.apiary.io/#reference/objednavky/objednavky/seznam-objednavek
    """
    schema = [
        # Root-level fields
        bigquery.SchemaField("order_number", "STRING"),
        bigquery.SchemaField("order_id", "FLOAT"),
        bigquery.SchemaField("case_number", "STRING"),
        bigquery.SchemaField("external_order_number", "STRING"),
        bigquery.SchemaField("uuid", "STRING"),
        bigquery.SchemaField("language_id", "STRING"),
        bigquery.SchemaField("currency_id", "STRING"),
        bigquery.SchemaField("default_currency_rate", "STRING"),
        bigquery.SchemaField("prices_with_vat_yn", "BOOLEAN"),
        bigquery.SchemaField("status_id", "FLOAT"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("paid_date", "STRING"),
        bigquery.SchemaField("tracking_code", "STRING"),
        bigquery.SchemaField("tracking_url", "STRING"),
        bigquery.SchemaField("resolved_yn", "BOOLEAN"),
        bigquery.SchemaField("oss_yn", "BOOLEAN"),
        bigquery.SchemaField("internal_note", "STRING"),
        bigquery.SchemaField("last_update_time", "STRING"),
        bigquery.SchemaField("creation_time", "STRING"),
        bigquery.SchemaField("variable_symbol", "STRING"),
        bigquery.SchemaField("total_weight", "FLOAT"),
        bigquery.SchemaField("order_total", "FLOAT"),
        bigquery.SchemaField("order_total_before_round", "FLOAT"),
        bigquery.SchemaField("order_total_rest", "FLOAT"),
        bigquery.SchemaField("invoice_number", "STRING"),
        bigquery.SchemaField("origin", "STRING"),
        bigquery.SchemaField("admin_url", "STRING"),

        # Nested 'customer' object
        bigquery.SchemaField("customer", "RECORD", fields=[
            bigquery.SchemaField("email", "STRING"),
            bigquery.SchemaField("phone", "STRING"),
            bigquery.SchemaField("code", "STRING"),
            bigquery.SchemaField("customer_id", "FLOAT"),
            bigquery.SchemaField("customer_pricelist_id", "FLOAT"),
            bigquery.SchemaField("pricelist_name", "STRING"),
            bigquery.SchemaField("pricelist_percent", "FLOAT"),
            bigquery.SchemaField("firstname_invoice", "STRING"),
            bigquery.SchemaField("surname_invoice", "STRING"),
            bigquery.SchemaField("street_invoice", "STRING"),
            bigquery.SchemaField("city_invoice", "STRING"),
            bigquery.SchemaField("state_invoice", "STRING"),
            bigquery.SchemaField("zip_invoice", "STRING"),
            bigquery.SchemaField("country_id_invoice", "STRING"),
            bigquery.SchemaField("postal_yn", "BOOLEAN"),
            bigquery.SchemaField("firstname_postal", "STRING"),
            bigquery.SchemaField("surname_postal", "STRING"),
            bigquery.SchemaField("street_postal", "STRING"),
            bigquery.SchemaField("city_postal", "STRING"),
            bigquery.SchemaField("state_postal", "STRING"),
            bigquery.SchemaField("zip_postal", "STRING"),
            bigquery.SchemaField("country_id_postal", "STRING"),
            bigquery.SchemaField("company_postal", "STRING"),
            bigquery.SchemaField("company_yn", "BOOLEAN"),
            bigquery.SchemaField("company", "STRING"),
            bigquery.SchemaField("ico", "STRING"),
            bigquery.SchemaField("dic", "STRING"),
            bigquery.SchemaField("vat_payer_yn", "BOOLEAN"),
            bigquery.SchemaField("customer_note", "STRING"),
            bigquery.SchemaField("agreements", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("valid_to", "STRING"),
                bigquery.SchemaField("status", "BOOLEAN"),
            ]),
        ]),

        # Repeated 'products' array
        bigquery.SchemaField("products", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("product_id", "FLOAT"),
            bigquery.SchemaField("option_set_id", "FLOAT"),
            bigquery.SchemaField("type", "STRING"),
            bigquery.SchemaField("uuid", "STRING"),
            bigquery.SchemaField("parent_uuid", "STRING"),
            bigquery.SchemaField("code", "STRING"),
            bigquery.SchemaField("code_supplier", "STRING"),
            bigquery.SchemaField("supplier", "STRING"),
            bigquery.SchemaField("ean", "STRING"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("adult_yn", "BOOLEAN"),
            bigquery.SchemaField("unit", "STRING"),
            bigquery.SchemaField("length", "STRING"),
            bigquery.SchemaField("length_unit", "STRING"),
            bigquery.SchemaField("quantity", "FLOAT"),
            bigquery.SchemaField("price_per_unit", "FLOAT"),
            bigquery.SchemaField("price", "FLOAT"),
            bigquery.SchemaField("price_with_vat", "FLOAT"),
            bigquery.SchemaField("price_without_vat", "FLOAT"),
            bigquery.SchemaField("vat", "FLOAT"),
            bigquery.SchemaField("buy_price", "FLOAT"),
            bigquery.SchemaField("recycling_fee", "FLOAT"),
            bigquery.SchemaField("weight", "FLOAT"),
            bigquery.SchemaField("availability", "STRING"),
            bigquery.SchemaField("stock_position", "STRING"),
            bigquery.SchemaField("invoice_info", "STRING"),
            bigquery.SchemaField("parameters", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("value", "STRING"),
            ]),
            bigquery.SchemaField("configurations", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("values", "RECORD", mode="REPEATED", fields=[
                    bigquery.SchemaField("value", "STRING"),
                ]),
                bigquery.SchemaField("operation", "STRING"),
                bigquery.SchemaField("price", "FLOAT"),
            ]),
            bigquery.SchemaField("categories", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("category_id", "FLOAT"),
                bigquery.SchemaField("code", "STRING"),
            ]),
            bigquery.SchemaField("image_url", "STRING"),
        ]),

        # Optional 'discount_voucher', 'quantity_discount', 'loyalty_points' fields
        bigquery.SchemaField("discount_voucher", "RECORD", fields=[
            bigquery.SchemaField("code", "STRING"),
            bigquery.SchemaField("type", "STRING"),
            bigquery.SchemaField("percent", "STRING"),
            bigquery.SchemaField("price", "STRING"),
            bigquery.SchemaField("amount", "FLOAT"),
            bigquery.SchemaField("discounts", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("price", "FLOAT"),
                bigquery.SchemaField("vat", "FLOAT"),
            ]),
        ]),
        bigquery.SchemaField("quantity_discount", "RECORD", fields=[
            bigquery.SchemaField("type", "STRING"),
            bigquery.SchemaField("percent", "STRING"),
            bigquery.SchemaField("price", "STRING"),
            bigquery.SchemaField("amount", "FLOAT"),
            bigquery.SchemaField("discounts", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("price", "FLOAT"),
                bigquery.SchemaField("vat", "FLOAT"),
            ]),
        ]),
        bigquery.SchemaField("loyalty_points", "RECORD", fields=[
            bigquery.SchemaField("one_point_for", "FLOAT"),
            bigquery.SchemaField("amount", "FLOAT"),
            bigquery.SchemaField("discounts", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("price", "FLOAT"),
                bigquery.SchemaField("vat", "FLOAT"),
            ]),
        ]),

        # Nested 'shipment' object
        bigquery.SchemaField("shipment", "RECORD", fields=[
            bigquery.SchemaField("id", "FLOAT"),
            bigquery.SchemaField("code", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("price", "FLOAT"),
            bigquery.SchemaField("vat", "FLOAT"),
            bigquery.SchemaField("affiliate_id", "STRING"),
            bigquery.SchemaField("affiliate_name", "STRING"),
            bigquery.SchemaField("type", "STRING"),
            bigquery.SchemaField("packeta_carrier_id", "FLOAT"),
        ]),

        # Nested 'payment' object
        bigquery.SchemaField("payment", "RECORD", fields=[
            bigquery.SchemaField("id", "FLOAT"),
            bigquery.SchemaField("code", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("price", "FLOAT"),
            bigquery.SchemaField("vat", "FLOAT"),
            bigquery.SchemaField("eet_yn", "BOOLEAN"),
            bigquery.SchemaField("type", "STRING"),
        ]),

        # Repeated 'attachments' array
        bigquery.SchemaField("attachments", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("url", "STRING"),
            bigquery.SchemaField("code", "STRING"),
        ]),

        # Repeated 'metas' array
        bigquery.SchemaField("metas", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("key", "STRING"),
            bigquery.SchemaField("type", "STRING"),
            bigquery.SchemaField("value", "STRING"),
            bigquery.SchemaField("values", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("language", "STRING"),
                bigquery.SchemaField("value", "STRING"),
            ]),
        ]),
    ]

    return schema
