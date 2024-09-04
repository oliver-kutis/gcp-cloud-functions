import functions_framework
import json
from slack_sdk import WebClient

# DEFAULTS_ARG_VALUES = {
#     'project_id': 'niftyminds-client-reporting',
#     'location': 'europe-west3',
#     'git_commitish': "main",
# }
GLOBAL_LOG_FIELDS = {
    'pipeline_phase': 'Slack notification'
}
FUNCTION_ARGS = {
    "required": ['execution_id', 'slack_oauth_token', 'client_name', 'blocks'],
    "optional": []
}
inputs_dict = dict()


@functions_framework.http
def run(request):
    """
    This function is an HTTP Cloud Function that expects an HTTP POST request with
    a JSON body containing the following parameters:

    {
        "slack_oauth_token": "token-value",
        "message": "Hello, World!",
        (optional) "channel": "client-reporting-alerts"
    }

    If the job finishes successfully, the function returns "Good". If the job fails
    or times out, the function returns "Bad".
    """

    gcp_log(
        "NOTICE",
        f"----- Function started -----",
        dict(job_phase="start", input_params=request.get_json(silent=True))
    )

    check_request_args_result = check_request_args(request)
    if (check_request_args_result[1] == 400):
        return check_request_args_result
    else:
        inputs_dict = check_request_args_result[0]

    slack_notification_result = send_slack_notification(
        inputs_dict['blocks'], inputs_dict['slack_oauth_token'])

    if slack_notification_result[1] == 400:
        return slack_notification_result

    gcp_log(
        "NOTICE",
        f"Function finished successfully.",
        dict(job_phase="result", status_code=slack_notification_result[1])
    )
    return (f"Slack notification sent successfully for {inputs_dict['client_name']}.", 200)


def send_slack_notification(blocks, token):
    client = WebClient(token=token)
    try:
        response = client.chat_postMessage(
            channel="#client-reporting-alerts",
            # text=message,
            blocks=blocks,
            username="gcp-cloud-functions",
            icon_emoji=":robot_face:",
        )
        if response.status_code == 200:
            gcp_log(
                "NOTICE",
                f"Slack notification sent successfully.",
                dict(job_phase="send_slack_notification",
                     response_from_api=f"{response.data}")
            )
            return (response, 200)
        else:
            return gcp_log(
                "ERROR",
                f"Failed to send Slack notification. Status Code: {response.status_code}",
                dict(job_phase="send_slack_notification",
                     error_response_from_api=f"{response.data}")
            )
    except Exception as e:
        return gcp_log(
            "ERROR",
            f"Failed to send Slack notification. Exception: {e}",
            dict(job_phase="send_slack_notification",
                 error_response_from_api=f"{e}")
        )


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
        else:
            return gcp_log(
                "ERROR",
                f"The '{arg}' wasn't provided or the value is None. Please enter a value for '{arg}'.",
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

    # print("-----inputs_dict", inputs_dict)
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
        component="Dataform trigger",
        **additional_log_fields
    )

    print(json.dumps(log_entry))

    if severity.upper() == "ERROR":
        return ({"error": message, "details": additional_log_fields, }, 400)

    return ({"message": message, "details": additional_log_fields}, 200)
