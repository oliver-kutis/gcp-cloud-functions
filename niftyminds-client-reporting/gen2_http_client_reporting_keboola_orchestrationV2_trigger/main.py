import functions_framework
import time
import requests
import json

KEBOOLA_COMPONENT_NAME = "kds-team.app-orchestration-trigger-queue-v2"
KEBOOLA_MODE = "run"
DEFAULTS_ARG_VALUES = {
    'keboola_endpoint_url': 'https://queue.north-europe.azure.keboola.com',
    'keboola_job_max_runtime_seconds': 360,
}
GLOBAL_LOG_FIELDS = {
    'pipeline_phase': 'Keboola orchestration trigger'
}
FUNCTION_ARGS = {
    "required": ['execution_id', 'keboola_orchestration_trigger_run_id', 'client_name', 'keboola_storage_api_token'],
    "optional": ['keboola_job_max_runtime_seconds', 'keboola_endpoint_url']
}

inputs_dict = dict()


@functions_framework.http
def run(request):
    """
    This function is an HTTP Cloud Function that expects an HTTP POST request with
    a JSON body containing the following parameters:
        - keboola_endpoint_url: The URL of the Keboola Orchestrator API
        - keboola_storage_api_token: The Storage API token to use for authentication
        - keboola_orchestration_trigger_run_id: The ID of the orchestration trigger run
        - keboola_job_max_runtime_seconds: The maximum amount of time the job should run

    If the job finishes successfully, the function returns "Good". If the job fails
    or times out, the function returns "Bad".
    """

    gcp_log(
        "NOTICE",
        f"----- Function started -----",
        dict(job_phase="start", input_params=request.get_json(silent=True))
    )
    check_request_args_result = check_request_args(request)
    # If error in check_request_args, return the error message
    if (check_request_args_result[1] == 400):
        return check_request_args_result
    else:
        inputs_dict = check_request_args_result[0]

    create_job_result = create_job(inputs_dict['keboola_endpoint_url'], inputs_dict['keboola_storage_api_token'],
                                   inputs_dict['keboola_orchestration_trigger_run_id'])
    # If errror in create_job, return the error message
    if create_job_result[1] == 400:
        return create_job_result
    else:
        job_id = create_job_result[0]

    check_job_status_result = check_job_status(inputs_dict['keboola_endpoint_url'], inputs_dict['keboola_storage_api_token'], job_id,
                                               inputs_dict['keboola_job_max_runtime_seconds'])
    if check_job_status_result[1] == 400:
        return check_job_status_result
    else:
        message = f"Function finished. {check_job_status_result[0]} for '{inputs_dict['client_name']}'.",
        gcp_log(
            "NOTICE",
            message,
            dict(job_phase="result", status_code=check_job_status_result[1]),
        )
        return (message, check_job_status_result[1])


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
            if arg == 'client_name':
                GLOBAL_LOG_FIELDS['client_name'] = request_json['client_name']
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

    if severity.upper() == "ERROR":
        return ({"error": message, "details": additional_log_fields}, 400)

    return ({"message": message, "details": additional_log_fields}, 200)


def create_job(keboola_endpoint_url, keboola_storage_api_token, keboola_orchestration_trigger_run_id):
    """
    Creates a new job in the Keboola Orchestrator API.

    Args:
        keboola_endpoint_url (str): The URL of the Keboola Orchestrator API.
        keboola_storage_api_token (str): The Storage API token to use for authentication.
        keboola_orchestration_trigger_run_id (str): The ID of the orchestration trigger run.

    Returns:
        str: The ID of the created job.

    Raises:
        Exception: If the job creation fails.
    """

    # Log information that job will be created
    gcp_log(
        "INFO",
        f"Creating a job for orchestration trigger with id: {keboola_orchestration_trigger_run_id}",
        {
            "job_phase": "create_job",
            "parameters_all": inputs_dict
        }
    )
    try:
        result = requests.post(
            url=f"{keboola_endpoint_url}/jobs",
            headers={
                "X-StorageApi-Token": keboola_storage_api_token,
                "X-KBC-RunId": keboola_orchestration_trigger_run_id,
            },
            json={
                "component": KEBOOLA_COMPONENT_NAME,
                "config": keboola_orchestration_trigger_run_id,
                "mode": KEBOOLA_MODE,
                "configRowIds": [],
            }
        )
        result_json = result.json()
        if 'error' in result_json:
            return gcp_log(
                "ERROR",
                f"The function failed to start a job for orchestration trigger with id: {keboola_orchestration_trigger_run_id}. The response from API: {result_json['error']}",
                dict(job_phase="create_job",
                     job_phase_detail="error_response_from_api",
                     error_message=result_json['error'])

            )
        job_id = result_json.get("id")

        return (job_id, 200)
    except Exception as e:
        # gcp_log("ERROR", f"The function failed to start a job for a orchestration trigger with id: {keboola_orchestration_trigger_run_id}", **dict(
        #     GLOBAL_LOG_FIELDS, job_phase="create_job"))
        return gcp_log(
            "ERROR",
            f"Exception in create_job for orchestration trigger with id: {keboola_orchestration_trigger_run_id}. Exception: {e}",
            dict(
                job_phase="create_job",
                job_phase_detail="error_creating_job",
                error_message=f"{e}"
            )
        )


def check_job_status(keboola_endpoint_url, keboola_storage_api_token, keboola_job_id, keboola_job_max_runtime_seconds):
    start_time = time.time()
    job_status = None
    job_phase = "check_job_status"

    gcp_log(
        "INFO",
        f"Checking the status of the job with id: {keboola_job_id}",
        dict(job_phase="check_job_status")
    )

    try:
        while time.time() - start_time < keboola_job_max_runtime_seconds:
            elapsed_time = round(time.time() - start_time, 2)

            result = requests.get(
                url=f"{keboola_endpoint_url}/jobs/{keboola_job_id}",
                headers={
                    "X-StorageApi-Token": keboola_storage_api_token,
                }
            )
            result_json = result.json()
            job_status = result_json.get("status")

            # Print the time elapsed every 30 seconds
            if int(elapsed_time) % 30 == 0 and int(elapsed_time) != 0:
                gcp_log(
                    "INFO",
                    f"Job ({keboola_job_id}) status check... Time elapsed: {elapsed_time} seconds.",
                    dict(job_phase=job_phase, details=result_json)
                )

            if 'error' in result_json:
                kill_job(keboola_endpoint_url,
                         keboola_storage_api_token, keboola_job_id)

                return gcp_log(
                    "ERROR",
                    f"The function failed to check the status of the job with id: {keboola_job_id}. The response from API: {result_json['error']}",
                    dict(job_phase=job_phase,
                         job_phase_detail="error_response_from_api",
                         error_message=result_json['error'])
                )
            if job_status in ["failed", "success"]:
                if job_status == "failed":
                    severity = "ERROR"
                else:
                    severity = "INFO"

                return gcp_log(
                    severity,
                    f"The job with id: {keboola_job_id} finished with status: {job_status}.",
                    dict(job_phase=job_phase,
                         job_phase_detail="job_finished", job_status=job_status)
                )
                break

            time.sleep(10)

        # return True
        if job_status not in ['failed', 'success']:
            kill_job(keboola_endpoint_url,
                     keboola_storage_api_token, keboola_job_id)
            return gcp_log(
                "ERROR",
                f"Job with ID: {keboola_job_id} was terminated due to timeout = {keboola_job_max_runtime_seconds} seconds.",
                dict(job_phase=job_phase,
                     job_phase_detail="job_timeout", job_status=job_status)
            )

            # return ["Killed", job_status]
        else:
            return ("Executed", 200)
    except Exception as e:

        kill_job(keboola_endpoint_url,
                 keboola_storage_api_token, keboola_job_id)

        return gcp_log(
            "ERROR",
            f"The function failed to check the status of the job with id: {keboola_job_id}.",
            dict(job_phase=job_phase,
                 job_phase_detail="error_checking_job", error_message=f"{e}")
        )

    # kill_job_response = kill_job(
    #     keboola_endpoint_url, keboola_storage_api_token, keboola_job_id)


def kill_job(keboola_endpoint_url, keboola_storage_api_token, keboola_job_id):
    gcp_log(
        "INFO",
        f"Killing job with ID: {keboola_job_id}.",
        dict(job_phase="kill_job")
    )
    try:
        result = requests.post(
            url=f"{keboola_endpoint_url}/jobs/{keboola_job_id}/kill",
            headers={
                "X-StorageApi-Token": keboola_storage_api_token,
            }
        )

        if result.status_code == 200:
            gcp_log(
                "INFO",
                f"Job with ID: {keboola_job_id} killed successfully.",
                dict(job_phase="kill_job")
            )
            # gcp_log(
            #     "ERROR",
            #     f"Job with ID: {keboola_job_id} was terminated due to timeout = {keboola_job_max_runtime_seconds}.",
            # )
        else:
            return gcp_log(
                "ERROR",
                f"Failed to kill job with ID: {keboola_job_id}. Status Code: {result.status_code}",
                dict(job_phase="kill_job")
            )
            # print(
            #     f"Failed to kill job {keboola_job_id}. Status Code: {result.status_code}")

        return ("Success", 200)
    except Exception as e:
        return gcp_log(
            "ERROR",
            f"Failed to kill job with ID: {keboola_job_id}.",
            dict(job_phase="kill_job", error_message=f"{e}")
        )
