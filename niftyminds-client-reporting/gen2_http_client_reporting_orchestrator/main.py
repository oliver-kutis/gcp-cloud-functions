import functions_framework
import time
import requests
import urllib.parse
import json
import datetime

CLOUD_FUNCTION_URLS = {
    "keboola_trigger": "https://europe-west1-niftyminds-client-reporting.cloudfunctions.net/gen2_http_client_reporting_keboola_orchestrationV2_trigger",
    "dataform_trigger": "https://europe-west1-niftyminds-client-reporting.cloudfunctions.net/gen2_http_client_reporting_dataform_trigger",
    "slack_notification": "https://europe-west1-niftyminds-client-reporting.cloudfunctions.net/gen2_http_client_reporting_slack_alerting"
}
# DEFAULTS_ARG_VALUES = {
#     'cf_keboola_trigger_url': 'niftyminds-client-reporting',
#     'location': 'europe-west3',
#     'git_commitish': "main",
# }
GLOBAL_LOG_FIELDS = {
    'pipeline_phase': 'Client reporting orchestrator'
}
FUNCTION_ARGS = {
    "global": {
        "required": ["client_name"],
    },
    "keboola_trigger": {
        "required": ['execution_id', 'keboola_orchestration_trigger_run_id', 'keboola_storage_api_token'],
        "optional": ['keboola_endpoint_url', 'keboola_job_max_runtime_seconds'],
    },
    "dataform_trigger": {
        "required": ['execution_id', 'dataform_repo_name'],
        "optional": ['git_commitish',
                     'project_id', 'location']
    },
    "slack_notification": {
        "required": ['execution_id', 'slack_oauth_token'],
    }
}

logs_url = """https://console.cloud.google.com/logs/
                query;query=jsonPayload.execution_id%3D123456%0A
                jsonPayload.pipeline_phase%3D%22Keboola%20orchestration%20trigger%22%0A
                severity%20%3E%20%22WARNING%22;
                duration=P7D?
                project=niftyminds-client-reporting"""
logs_url = """https://console.cloud.google.com/logs/
                query;query=jsonPayload.execution_id%3D123456%0A
                jsonPayload.pipeline_phase%3D%22Keboola%20orchestration%20trigger%22%0A
                severity%20%3E%20%22WARNING%22;
                duration=P14D?project=niftyminds-client-reporting"""


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
    base_logs_url = "https://console.cloud.google.com/logs/query"
    start_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    inputs_dict = dict()
    component_name_map = dict()
    for key in FUNCTION_ARGS:
        if key != "global":
            if key == "keboola_trigger":
                component_name_map[key] = "Keboola orchestration trigger"
            elif key == "dataform_trigger":
                component_name_map[key] = "Dataform trigger"
            elif key == "slack_notification":
                component_name_map[key] = "Slack notification"

    # Create a timestamp (ms) since epoch = execution_id
    GLOBAL_LOG_FIELDS['execution_id'] = int(time.time() * 1000)
    # Insert executuion_id into inputs_dict under the "global" key
    inputs_dict['execution_id'] = GLOBAL_LOG_FIELDS['execution_id']

    gcp_log(
        "NOTICE",
        f"----- Function started -----",
        dict(job_phase="orchestrator", job_phase_detail="start",
             input_params=request.get_json(silent=True))
    )

    # check request arguments
    check_request_args_result = check_request_args(request)
    if (check_request_args_result[1] == 400):
        return check_request_args_result
    else:
        inputs_dict = dict(**inputs_dict, **check_request_args_result[0])

    project = inputs_dict.get('project', 'niftyminds-client-reporting')

    # Add execution_id to inputs_dict for each key
    for key in FUNCTION_ARGS:
        if key != "global":
            inputs_dict[key]['execution_id'] = GLOBAL_LOG_FIELDS['execution_id']
            inputs_dict[key]['client_name'] = inputs_dict['client_name']

    # ---------------------------------
    # Keboola trigger
    # ---------------------------------
    gcp_log(
        "INFO",
        f"Starting {component_name_map['keboola_trigger']}...",
        dict(
            job_phase=component_name_map['keboola_trigger'],
            job_phase_detail="start"
        )
    )
    # Call keboola orchestration trigger
    keboola_response = requests.post(
        CLOUD_FUNCTION_URLS["keboola_trigger"], json=inputs_dict['keboola_trigger'])

    # keboola_response_body = keboola_response.json()
    if keboola_response.status_code == 400:
        component_error_details = {
            **keboola_response.json()['details'],
            **{"error": keboola_response.json()['error']},
        }
        message = f"Error in {component_name_map['keboola_trigger']} for {inputs_dict['client_name']}; Execution ID: {GLOBAL_LOG_FIELDS['execution_id']}"
        slack_response = send_slack_notification(
            client_name=inputs_dict['client_name'],
            execution_id=GLOBAL_LOG_FIELDS['execution_id'],
            slack_oauth_token=inputs_dict['slack_notification']['slack_oauth_token'],
            error_params={
                "start_datetime": start_datetime,
                "pipeline_component": component_name_map['keboola_trigger'],
                "job_phase": component_error_details['job_phase'],
                "component_url": CLOUD_FUNCTION_URLS['keboola_trigger'],
                "error_type": "Component error",
                "message": message,
                "message_detail": keboola_response.json()['error'],
                "gcp_warnerr_logs_url": build_logs_url(
                    base_logs_url,
                    GLOBAL_LOG_FIELDS['execution_id'],
                    component_name_map['keboola_trigger'],
                    project,
                    False
                ),
                "gcp_full_logs_url": build_logs_url(
                    base_logs_url,
                    GLOBAL_LOG_FIELDS['execution_id'],
                    component_name_map['keboola_trigger'],
                    project,
                    True
                )

            }
        )

        if slack_response.status_code == 400:
            gcp_log(
                "WARNING",
                "Sending slack message failed",
                {
                    "component_error_details": {
                        **slack_response.json()['details'],
                        **{"error": slack_response.json()['error']},
                    },
                    "job_phase": component_name_map['keboola_trigger'],
                    "job_phase_detail": "send_slack_notification"
                }
            )

        return gcp_log(
            "ERROR",
            message,
            {
                "component_error_details": component_error_details,
                "job_phase": component_name_map['keboola_trigger'],
                "job_phase_detail": "run_component"
            }
        )
    else:
        gcp_log(
            "NOTICE",
            f"Finished {component_name_map['keboola_trigger']}; Execution ID: {GLOBAL_LOG_FIELDS['execution_id']}",
            dict(
                job_phase=component_name_map['keboola_trigger'],
                job_phase_detail="finish"
            )
        )

    # ---------------------------------
    # Dataform trigger
    # ---------------------------------
    gcp_log(
        "INFO",
        f"Starting {component_name_map['dataform_trigger']}...",
        dict(
            job_phase=component_name_map['dataform_trigger'],
            job_phase_detail="start"
        )
    )
    dataform_response = requests.post(
        CLOUD_FUNCTION_URLS['dataform_trigger'], json=inputs_dict['dataform_trigger']
    )

    if dataform_response.status_code == 400:
        component_error_details = {
            **dataform_response.json()['details'],
            **{"error": dataform_response.json()['error']},
        }
        message = f"Error in {component_name_map['dataform_trigger']} for {inputs_dict['client_name']}; Execution ID: {GLOBAL_LOG_FIELDS['execution_id']}"
        slack_response = send_slack_notification(
            client_name=inputs_dict['client_name'],
            execution_id=GLOBAL_LOG_FIELDS['execution_id'],
            slack_oauth_token=inputs_dict['slack_notification']['slack_oauth_token'],
            error_params={
                "start_datetime": start_datetime,
                "pipeline_component": component_name_map['dataform_trigger'],
                "job_phase": component_error_details['job_phase'],
                "component_url": CLOUD_FUNCTION_URLS['dataform_trigger'],
                "error_type": "Component error",
                "message": message,
                "message_detail": dataform_response.json()['error'],
                "gcp_warnerr_logs_url": build_logs_url(
                    base_logs_url,
                    GLOBAL_LOG_FIELDS['execution_id'],
                    component_name_map['dataform_trigger'],
                    project,
                    False
                ),
                "gcp_full_logs_url": build_logs_url(
                    base_logs_url,
                    GLOBAL_LOG_FIELDS['execution_id'],
                    component_name_map['dataform_trigger'],
                    project,
                    True
                )

            }
        )
        if slack_response.status_code == 400:
            gcp_log(
                "WARNING",
                "Sending slack message failed",
                {
                    "component_error_details": {
                        **slack_response.json()['details'],
                        **{"error": slack_response.json()['error']},
                    },
                    "job_phase": component_name_map['dataform_trigger'],
                    "job_phase_detail": "send_slack_notification"
                }
            )

            return gcp_log(
                "ERROR",
                message,
                {
                    "component_error_details": component_error_details,
                    "job_phase": component_name_map['dataform_trigger'],
                    "job_phase_detail": "run_component"
                }
            )
    else:
        gcp_log(
            "NOTICE",
            f"Finished {component_name_map['dataform_trigger']}; Execution ID: {GLOBAL_LOG_FIELDS['execution_id']}",
            dict(
                job_phase=component_name_map['dataform_trigger'],
                job_phase_detail="finish"
            )
        )

    # print(dataform.json())
    # print(dataform.status_code)
    # print(dataform.json()['error'])

    return gcp_log(
        "NOTICE",
        f"----- Orchestrator finished for client: '{inputs_dict['client_name']}'; Execution ID: {GLOBAL_LOG_FIELDS['execution_id']} -----",
        dict(
            job_phase="orchestrator",
            job_phase_detail="finish"
        )
    )

    # # check the result of the incovation
    # execution_result = check_workflow_execution_result(
    #     invocation_name, inputs_dict)
    # if execution_result[1] == 400:
    #     return execution_result[0]
    # else:
    #     message = f"----- Function finished for client: '{inputs_dict['client_name']}' and repostiory: '{inputs_dict['dataform_repo_name']}' -----",
    #     gcp_log(
    #         "NOTICE",
    #         message,
    #         dict(job_phase="end", status_code=execution_result[1], status=str(
    #             execution_result[0]))
    #     )

    #     return (message, execution_result[1])


def build_logs_url(base_logs_url, execution_id, pipeline_phase, project, complete=False):
    # query_params = {
    #     "query": f"jsonPayload.execution_id={execution_id}\njsonPayload.pipeline_phase=\"{pipeline_phase}\"",
    #     "duration": "P7D",
    #     "project": project,
    # }
    # if complete == False:
    #     query_params['query'] += f"\nseverity > \"WARNING\""

    # # logs_url = f"{base_logs_url};{query_params['query']};"
    # # logs_url += f"{query_params['duration']}"
    # # logs_url += f"?projects/{project}"
    #  # Construct the logs URL
    # logs_url = f"{base_logs_url};query={query_params['query'].replace(' ', '%20').replace('\n', '%0A')};"
    # logs_url += f"duration={query_params['duration']}?project={query_params['project']}"

    # Initialize the query components
    query = (
        f"jsonPayload.execution_id={execution_id}\n"
        f"jsonPayload.pipeline_phase=\"{pipeline_phase}\""
    )

    # Append severity condition if complete is False
    if not complete:
        query += "\nseverity >= \"WARNING\""

    # URL encode the query string
    encoded_query = urllib.parse.quote(query)

    # Construct the final logs URL
    logs_url = f"{base_logs_url};query={encoded_query};duration=P7D?project={project}"

    return logs_url


def send_slack_notification(client_name, execution_id, slack_oauth_token, error_params):
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"[{error_params['start_datetime']}] 🚨 {error_params['message']}"
            }
        },
        {
            "type": "divider"
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "⚙️ *Basic information*"
            }
        },
        {
            "type": "divider"
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Client name:* {client_name}"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Exection ID:* {execution_id}"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Pipeline component:* {error_params['pipeline_component']}"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Pipeline phase:* {error_params['pipeline_component']}"
            }
        },
        {
            "type": "divider"
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"❌ *Error details*"
            }
        },
        {
            "type": "divider"
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Error type:* {error_params['error_type']}"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Error message:* {error_params['message_detail']}\n"
            }
        },
        {
            "type": "divider"
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"🔗 *Links*"
            }
        },
        {
            "type": "divider"
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"<{error_params['component_url']}|*Link to failed component*>"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"<{error_params['gcp_warnerr_logs_url']}|*Link to logs in GCP (warning and higher)*>"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"<{error_params['gcp_full_logs_url']}|*Link to _all_ logs for the execution in GCP*>\n\n"
            }
        }
    ]

    response = requests.post(
        CLOUD_FUNCTION_URLS['slack_notification'],
        json={
            "client_name": client_name,
            "execution_id": execution_id,
            "slack_oauth_token": slack_oauth_token,
            "blocks": blocks
        },
        headers={
            "Content-Type": "application/json"
        }
    )

    return response


def check_request_args(request):
    request_json = request.get_json(silent=False)
    inputs_dict = dict()
    additional_log_fields = dict(
        job_phase="check_request_args", error_type="Orchestator error")

    # Log that the check of args started
    gcp_log(
        "Notice",
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

    for key in FUNCTION_ARGS:
        additional_log_fields['pipeline_component'] = key

        gcp_log(
            "INFO",
            f"Started: Checking request args for {key}...",
            # dict(job_phase="check_request_args", pipeline_component=key)
            additional_log_fields
        )
        if key == "global":
            if 'client_name' not in request_json:
                return gcp_log(
                    "ERROR",
                    "Request argument for 'client_name' is missing.",
                    additional_log_fields
                )
            else:
                inputs_dict['client_name'] = request_json['client_name']
        elif key not in request_json:
            return gcp_log(
                "ERROR",
                f"Request arguments for {key} are missing. Please enter a body with valid values for: {FUNCTION_ARGS[key]['required']}.",
                additional_log_fields
            )
        else:
            inputs_dict[key] = request_json[key]

        gcp_log(
            "INFO",
            f"Finished: Checking request args for {key}.",
            additional_log_fields
        )

    #     # Check optional args
    #     if 'optional' not in args:
    #         continue
    #     for arg in args['optional']:
    #         additional_log_fields['job_phase_detail'] = "optional_args"

    #         if arg in request_json and request_json[arg] is not None:
    #             inputs_dict[key][arg] = request_json[key][arg]
    #         else:
    #             # inputs_dict[key][arg] = DEFAULTS_ARG_VALUES[arg]
    #             inputs_dict[key][arg] = None
    #             gcp_log(
    #                 "WARNING",
    #                 f"The '{arg}' wasn't provided or the value is None. Setting value to = '{None}' instead.",
    #                 dict(**additional_log_fields, arg_details={
    #                     "name": arg,
    #                     "value": None,
    #                 })
    #                 # dict(job_phase=job_phase,
    #                 #      job_phase_detail="optional_args",
    #                 #      arg_details={
    #                 #          "name": arg,
    #                 #          "value": DEFAULTS_ARG_VALUES[arg]
    #                 #      })
    #             )

    #     # Check required args
    #     if 'required' not in args:
    #         continue
    #     for arg in args['required']:
    #         additional_log_fields['job_phase_detail'] = "optional_args"

    #         if arg in request_json and request_json[arg] is not None:
    #             inputs_dict[key][arg] = request_json[key][arg]
    #             if arg == 'client_name':
    #                 GLOBAL_LOG_FIELDS['client_name'] = request_json['client_name']
    #         else:
    #             return gcp_log(
    #                 "ERROR",
    #                 f"The '{arg}' wasn't provided or the value is None. Please enter a value for '{arg}'.",
    #                 dict(**additional_log_fields, arg_details={
    #                     "name": arg,
    #                     "value": request_json.get(arg),
    #                 })
    #                 # dict(job_phase=job_phase,
    #                 #      job_phase_detail="required_args",
    #                 #      arg_details={
    #                 #          "name": arg,
    #                 #          "value": request_json.get(arg),
    #                 #      })
    #             )

    #     gcp_log(
    #         "INFO",
    #         f"Finished: Checking request args for {key}...",
    #         # dict(job_phase="check_request_args", pipeline_component=key)
    #         additional_log_fields
    #     )

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
        # pipeline_component="Dataform orchestrator",
        **additional_log_fields
    )

    print(json.dumps(log_entry))

    if severity.upper() == "ERROR":
        return ({"error": message, "details": additional_log_fields, }, 400)

    return ({"message": message, "details": additional_log_fields}, 200)
