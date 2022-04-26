"""
  Connector for Beast Workload Manager (Spark AKS)
"""
import json
from http.client import HTTPException
from typing import Optional

from proteus.connectors.beast._auth import BeastAuth
from proteus.connectors.beast._models import JobRequest, BeastJobParams
from proteus.utils import doze, session_with_retries


class BeastConnector:
    """
      Beast API connector
    """

    __SUPPORTED_BEAST_RELEASE__ = "1.2.*"

    def __init__(self, *, base_url, code_root="/ecco/dist", lifecycle_check_interval: int = 60,
                 failure_type: Optional[Exception] = None):
        """
          Creates a Beast connector, capable of submitting/status tracking etc.

        :param base_url: Base URL for Beast Workload Manager.
        :param code_root: Root folder for code deployments.
        :param lifecycle_check_interval: Time to wait between lifecycle checks for submissions/cancellations etc.
        """
        self.base_url = base_url
        self.code_root = code_root
        self.lifecycle_check_interval = lifecycle_check_interval
        self.failed_stages = ["FAILED", "SCHEDULING_FAILED", "RETRIES_EXCEEDED", "SUBMISSION_FAILED", "STALE"]
        self.success_stages = ["COMPLETED"]
        self.http = session_with_retries()
        self.http.auth = BeastAuth()
        self._failure_type = failure_type or Exception

    @staticmethod
    def redact_sensitive(json_str: str) -> str:
        """
          Redacts sensitive info when preparing a request to be printed

        :param json_str: Serialized JobRequest to print
        :return:
        """
        request_json = json.loads(json_str)

        for arg_key, _ in request_json['extraArgs'].items():
            if 'password' in arg_key or 'secret' in arg_key:
                request_json['extraArgs'][arg_key] = '***'
        return json.dumps(request_json)

    def _submit(self, request: JobRequest) -> (str, str):
        request_json = request.to_json()

        print(f"Submitting request: {self.redact_sensitive(json.dumps(request_json))}")

        submission_result = self.http.post(f"{self.base_url}/job/submit", json=request_json)
        submission_json = submission_result.json()

        if submission_result.status_code == 202 and submission_json:
            print(
                f"Beast has accepted the request, stage: {submission_json['lifeCycleStage']}, id: {submission_json['id']}")
        else:
            raise HTTPException(
                f"Error {submission_result.status_code} when submitting a request: {submission_result.text}")

        return submission_json['id'], submission_json['lifeCycleStage']

    def _existing_submission(self, submitted_tag: str, project: str) -> (Optional[str], Optional[str]):
        print(f"Looking for existing submissions of {submitted_tag}")

        existing_submissions = self.http.get(f"{self.base_url}/job/requests/{project}/tags/{submitted_tag}").json()

        if len(existing_submissions) == 0:
            print(f"No previous submissions found for {submitted_tag}")
            return None, None

        running_submissions = []
        for submission_request_id in existing_submissions:
            submission_lifecycle = self.http.get(
                f"{self.base_url}/job/requests/{submission_request_id}").json()['lifeCycleStage']
            if submission_lifecycle not in self.success_stages and submission_lifecycle not in self.failed_stages:
                print(f"Found a running submission of {submitted_tag}: {submission_request_id}.")
                running_submissions.append((submission_request_id, submission_lifecycle))

        if len(running_submissions) == 0:
            print("None of found submissions are active")
            return None, None

        if len(running_submissions) == 1:
            return running_submissions[0][0], running_submissions[0][1]

        raise self._failure_type(
            f"Fatal: more than one submission of {submitted_tag} is running: {running_submissions}. Please review their status restart/terminate the task accordingly")

    def run_job(self, job_params: BeastJobParams, **context):
        """
          Runs a job through Beast

        :param job_params: Parameters for Beast Job body.
        :return: A JobRequest for Beast.
        """

        (request_id, request_lifecycle) = self._existing_submission(submitted_tag=job_params.client_tag,
                                                                    project=job_params.project_name)

        if request_id:
            print(f"Resuming watch for {request_id}")

        if not request_id:
            prepared_arguments = {key: str(value) for (key, value) in job_params.extra_arguments.items()}

            submit_request = JobRequest(
                root_path=self.code_root,
                project_name=job_params.project_name,
                runnable=job_params.project_runnable,
                version=job_params.project_version,
                inputs=job_params.project_inputs,
                outputs=job_params.project_outputs,
                overwrite=job_params.overwrite_outputs,
                extra_args=prepared_arguments,
                client_tag=job_params.client_tag,
                cost_optimized=job_params.cost_optimized,
                job_size=job_params.size_hint,
                flexible_driver=job_params.flexible_driver,
                max_runtime_hours=job_params.max_runtime_hours,
                runtime_tags=job_params.runtime_tags,
                execution_group=job_params.execution_group
            )

            (request_id, request_lifecycle) = self._submit(submit_request)

        while request_lifecycle not in self.success_stages and request_lifecycle not in self.failed_stages:
            doze(self.lifecycle_check_interval)
            request_lifecycle = self.http.get(f"{self.base_url}/job/requests/{request_id}").json()['lifeCycleStage']
            print(f"Request: {request_id}, current state: {request_lifecycle}")

        if request_lifecycle in self.failed_stages:
            raise self._failure_type(
                f"Execution failed, please find request's log at: {self.base_url}/job/logs/{request_id}")

    def start_job(self, job_params: BeastJobParams, **context) -> Optional[str]:
        """
          Starts a job through Beast.

        :param job_params: Parameters for Beast Job body.
        :return: A JobRequest for Beast.
        """

        (request_id, _) = self._existing_submission(submitted_tag=job_params.client_tag,
                                                    project=job_params.project_name)

        if not request_id:
            prepared_arguments = {key: str(value) for (key, value) in job_params.extra_arguments.items()}

            submit_request = JobRequest(
                root_path=self.code_root,
                project_name=job_params.project_name,
                runnable=job_params.project_runnable,
                version=job_params.project_version,
                inputs=job_params.project_inputs,
                outputs=job_params.project_outputs,
                overwrite=job_params.overwrite_outputs,
                extra_args=prepared_arguments,
                client_tag=job_params.client_tag,
                cost_optimized=job_params.cost_optimized,
                job_size=job_params.size_hint,
                flexible_driver=job_params.flexible_driver,
                max_runtime_hours=job_params.max_runtime_hours,
                runtime_tags=job_params.runtime_tags,
                execution_group=job_params.execution_group
            )

            request_id, _ = self._submit(submit_request)

        return request_id
