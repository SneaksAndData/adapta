"""
  Thin wrapper for Mlflow operations.
"""
#  Copyright (c) 2023-2024. ECCO Sneaks & Data
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import os
from typing import List, Optional

import mlflow
from mlflow.entities.model_registry import ModelVersion
from mlflow.pyfunc import PyFuncModel
from mlflow.store.artifact.artifact_repository_registry import get_artifact_repository
from mlflow.store.entities import PagedList
from mlflow.tracking import MlflowClient

MLFLOW_ARTIFACT_STORE_SCHEME = "mlflow-artifacts"


class MlflowBasicClient:
    """
    Mlflow operations scoped to MlflowClient API.
    """

    def __init__(self, tracking_server_uri: str):
        assert os.environ.get("MLFLOW_TRACKING_USERNAME") and os.environ.get(
            "MLFLOW_TRACKING_PASSWORD"
        ), "Both MLFLOW_TRACKING_USERNAME and MLFLOW_TRACKING_PASSWORD must be set to access MLFlow Tracking Server"

        mlflow.set_tracking_uri(tracking_server_uri)
        self._tracking_server_uri = tracking_server_uri
        self._client = MlflowClient()

    @property
    def tracking_server_uri(self) -> str:
        """Returns tracking server URI"""
        return self._tracking_server_uri

    def _get_latest_model_versions(self, model_name: str) -> List[mlflow.entities.model_registry.ModelVersion]:
        """Gets latest model versions, one for each stage

        :param model_name: Model name
        """
        return self._client.get_registered_model(model_name).latest_versions

    def get_latest_model_version(self, model_name: str, model_stage: Optional[str] = None) -> ModelVersion:
        """
          Get model version using mlflow client

        :param model_name: Name of a model.
        :param model_stage: Stage of a model.
        """
        if model_stage:
            return [m for m in self._get_latest_model_versions(model_name) if m.current_stage == model_stage][0]

        return sorted(
            self._get_latest_model_versions(model_name),
            key=lambda m: int(m.version),
            reverse=True,
        )[0]

    def get_model_version_by_alias(self, model_name: str, alias: str) -> ModelVersion:
        """
          Get model version by alias using mlflow client

        :param model_name: Name of a model.
        :param alias: Alias of a model.
        """
        return self._client.get_model_version_by_alias(model_name, alias)

    def _get_artifact_repo_backported(self, run_id: str) -> mlflow.store.artifact_repo.ArtifactRepository:
        run = self._client.get_run(run_id)

        artifact_uri = (
            run.info.artifact_uri
            if run.info.artifact_uri.startswith(MLFLOW_ARTIFACT_STORE_SCHEME)
            else f"{MLFLOW_ARTIFACT_STORE_SCHEME}:/{run.info.experiment_id}/{run.info.run_id}"
        )

        return get_artifact_repository(artifact_uri)

    def download_artifact(self, model_name: str, model_version: str, artifact_path: str):
        """
          Download an artifact from mlflow model registry for the latest version of this model

        :param model_name: Name of a model.
        :param model_version: Version of a model.
        :param artifact_path: Path to a desired artifact.
        """
        run_id = self._client.get_model_version(model_name, model_version).run_id
        repository = self._get_artifact_repo_backported(run_id)
        return repository.download_artifacts(artifact_path)

    def search_model_versions(self, model_name: str) -> PagedList[ModelVersion]:
        """
          Search model versions with Mlflow client.

        :param model_name: Name of a model.
        """
        return self._client.search_model_versions(f"name='{model_name}'")

    def set_model_stage(self, model_name: str, model_version: str, stage: str) -> ModelVersion:
        """
        inherited the transitioning model version stage in Mlflow
        :param model_name: model name
        :param stage:['Staging', 'Production', 'None']
        :param model_version: version of model
        """
        return self._client.transition_model_version_stage(
            name=model_name,
            version=model_version,
            stage=stage,
        )

    def set_model_alias(self, model_name: str, alias: str, model_version: Optional[str]) -> None:
        """
        inherited the setting model version alias in Mlflow
        :param model_name: model name
        :param alias: alias name
        :param model_version: version of model
        """
        self._client.set_registered_model_alias(
            name=model_name,
            alias=alias,
            version=model_version,
        )

    def log_dict(self, artifact: dict, artifact_path: str, run_id: str):
        """
        inherited the logging dictionary in Mlflow

        :param artifact: dictionary to log
        :param artifact_path: artifact path
        :param run_id: run id
        """
        self._client.log_dict(run_id=run_id, dictionary=artifact, artifact_file=artifact_path)

    def log_metric(self, run_id: str, metric_name: str, metric_value: float):
        """
        inherited the logging metric in Mlflow

        :param run_id: run id
        :param metric_name: metric name
        :param metric_value: metric value
        """
        self._client.log_metric(run_id=run_id, key=metric_name, value=metric_value)

    def create_run(self, experiment_name: str, run_name: str) -> str:
        """
        inherited the creating run in Mlflow

        :param experiment_name: experiment name
        :param run_name: run name
        :return: run id
        """
        experiment = self._client.get_experiment_by_name(experiment_name)
        experiment_id = experiment.experiment_id if experiment else self._client.create_experiment(experiment_name)
        return self._client.create_run(experiment_id=experiment_id, run_name=run_name).info.run_id

    def terminate_run(self, run_id: str):
        """
        inherited the stopping run in Mlflow

        :param run_id: run id
        """
        self._client.set_terminated(run_id)

    def set_run_tag(self, key: str, value: any, run_id: str):
        """
        inherited the setting run tag in Mlflow

        :param key: tag key
        :param value: tag value
        :param run_id: run id
        """
        self._client.set_tag(run_id=run_id, key=key, value=value)

    @staticmethod
    def load_model_by_name(model_name: str, stage_or_version: str) -> PyFuncModel:
        """
        Load model as pyfunc using models:/ api
        """
        return mlflow.pyfunc.load_model(f"models:/{model_name}/{stage_or_version}")

    @staticmethod
    def load_model_by_uri(model_uri: str) -> PyFuncModel:
        """
        Load model as pyfunc using one of the following:

         - ``/Users/me/path/to/local/model``
         - ``relative/path/to/local/model``
         - ``s3://my_bucket/path/to/model``
         - ``runs:/<mlflow_run_id>/run-relative/path/to/model``
         - ``models:/<model_name>/<model_version>``
         - ``models:/<model_name>/<stage>``
         - ``mlflow-artifacts:/path/to/model``

         For more information about supported URI schemes, see
         `Referencing Artifacts <https://www.mlflow.org/docs/latest/concepts.html#
         artifact-locations>`_.
        """
        return mlflow.pyfunc.load_model(model_uri)
