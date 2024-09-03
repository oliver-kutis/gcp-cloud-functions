import functions_framework
import json
import time
from google.cloud import logging
from google.cloud import dataform_v1beta1 as dataform

DEFAULTS_ARG_VALUES = {
    'project_id': 'niftyminds-client-reporting',
    'location': 'europe-west3',
    'git_commitish': "main",
}
GLOBAL_LOG_FIELDS = {
    'pipeline_phase': 'Dataform trigger'
}
FUNCTION_ARGS = {
    "required": ['execution_id', 'dataform_repo_name', 'client_name'],
    "optional": ['git_commitish',
                 'project_id', 'location']
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

    # check request arguments
    check_request_args_result = check_request_args(request)
    if (check_request_args_result[1] == 400):
        return check_request_args_result
    else:
        inputs_dict = check_request_args_result[0]

    # compile dataform code
    compilation_result = create_dataform_compilation(inputs_dict)
    # If errror in create_job, return the error message
    if compilation_result[1] == 400:
        return compilation_result
    else:
        compilation_name = compilation_result[0]

    # invoke dataform workflow
    invocation_result = create_workflow_invocation(
        compilation_name, inputs_dict)
    if invocation_result[1] == 400:
        return invocation_result[0]
    else:
        invocation_name = invocation_result[0]

    # check the result of the incovation
    execution_result = check_workflow_execution_result(
        invocation_name, inputs_dict)
    if execution_result[1] == 400:
        return execution_result[0]
    else:
        message = f"----- Function finished for client: '{inputs_dict['client_name']}' and repostiory: '{inputs_dict['dataform_repo_name']}' -----",
        gcp_log(
            "NOTICE",
            message,
            dict(job_phase="end", status_code=execution_result[1], status=str(
                execution_result[0]))
        )

        return (message, execution_result[1])


def create_dataform_compilation(inputs_dict):
    job_phase = "create_dataform_compilation"
    # Compilation result
    try:
        parent = f"projects/{inputs_dict['project_id']}/locations/{inputs_dict['location']}/repositories/{inputs_dict['dataform_repo_name']}"
        client = dataform.DataformClient()

        compilation_result = dataform.CompilationResult()
        compilation_result.git_commitish = inputs_dict["git_commitish"]

        request = dataform.CreateCompilationResultRequest(
            parent=parent, compilation_result=compilation_result)

        result = client.create_compilation_result(request)

        gcp_log(
            "INFO",
            f"Compilation result created: {compilation_result}",
            dict(job_phase=job_phase, job_phase_detail="compilation_result",
                 compilation_result=str(result))
        )

        return (result.name, 200)

    except Exception as e:
        return gcp_log(
            "ERROR",
            f"Failed to create dataform compilation result for repository: {inputs_dict['dataform_repo_name']}.",
            dict(job_phase=job_phase, job_phase_detail="compilation_result",
                 error_message=str(e))
        )

    return (str(workflow_invocation_request_result), 200)


def create_workflow_invocation(compilation_name, inputs_dict):
    job_phase = "create_workflow_invocation"
    try:
        parent = f"projects/{inputs_dict['project_id']}/locations/{inputs_dict['location']}/repositories/{inputs_dict['dataform_repo_name']}"
        client = dataform.DataformClient()

        workflow_invocation = dataform.WorkflowInvocation()
        workflow_invocation.compilation_result = compilation_name

        if 'tags' in inputs_dict:
            workflow_invocation.invocation_config = {
                "included_tags": inputs_dict['tags']
            }

        request = dataform.CreateWorkflowInvocationRequest(
            parent=parent,
            workflow_invocation=workflow_invocation
        )

        result = client.create_workflow_invocation(request)

        gcp_log(
            "INFO",
            f"Workflow invocation created: {result}",
            dict(job_phase=job_phase, job_phase_detail="workflow_invocation",
                 workflow_invocation_result=str(result))
        )

        return (result.name, 200)
    except Exception as e:
        return gcp_log(
            "ERROR",
            f"Failed to create dataform workflow invocation for repository: {inputs_dict['dataform_repo_name']}.",
            dict(job_phase=job_phase, job_phase_detail="workflow_invocation",
                 error_message=str(e)
                 )
        )


def check_workflow_execution_result(invocation_name, inputs_dict):
    job_phase = "check_workflow_execution_result"
    try:
        invocation_id = invocation_name.split("/")[-1]
        client = dataform.DataformClient()
        request = dataform.GetWorkflowInvocationRequest(name=invocation_name)

        execution_completed = False

        while execution_completed != True:
            result = client.get_workflow_invocation(request)
            state = dataform.WorkflowInvocation.State(result.state).name
            if state == "RUNNING" or state == "CANCELING":
                gcp_log(
                    "INFO",
                    f"Dataform execution is in state {state}, falling to sleep for 10 seconds",
                    dict(job_phase=job_phase,
                         job_phase_detail="running_or_canceling")
                )

                time.sleep(10)

            else:
                execution_completed = True
                execution_time = result.invocation_timing.end_time.seconds - \
                    result.invocation_timing.start_time.seconds
                message = f"The dataform execution {state} in {execution_time} seconds for client: '{inputs_dict['client_name']}' in repository: '{inputs_dict['dataform_repo_name']}'",
                add_args_dict = dict(
                    job_phase=job_phase,
                    invocation_id=invocation_id
                )
                if state == "SUCCEEDED":
                    gcp_log("NOTICE", message, add_args_dict)
                elif state == "CANCELLED":
                    gcp_log("WARNING", message, add_args_dict)
                elif state == "FAILED":
                    return gcp_log("ERROR", message, add_args_dict)
                break

        return (result, 200)
    except Exception as e:
        return gcp_log(
            "ERROR",
            f"Failed to check the dataform execution due to an error: {e}",
            dict(
                job_phase=job_phase,
                error_message=str(e)
            )
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
            if arg == 'client_name':
                GLOBAL_LOG_FIELDS['client_name'] = request_json['client_name']
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
