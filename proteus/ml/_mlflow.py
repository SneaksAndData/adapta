"""
  Thin wrapper for Mlflow operations.
"""
import os
from typing import List

import mlflow
from mlflow.entities.model_registry import RegisteredModel, ModelVersion
from mlflow.pyfunc import PyFuncModel
from mlflow.store.entities import PagedList
from mlflow.tracking import MlflowClient


class MlflowBasicClient:
    """
      Mlflow operations scoped to MlflowClient API.
    """

    def __init__(self, tracking_server_uri: str):
        assert os.environ.get('MLFLOW_TRACKING_USERNAME') and os.environ.get(
            'MLFLOW_TRACKING_PASSWORD'), 'Both MLFLOW_TRACKING_USERNAME and MLFLOW_TRACKING_PASSWORD must be set to access MLFlow Tracking Server'

        mlflow.set_tracking_uri(tracking_server_uri)
        self._client = MlflowClient()

    def _get_model_versions(self, model_name: str) -> List[mlflow.entities.model_registry.ModelVersion]:
        return self._client.get_registered_model(model_name).latest_versions

    def get_latest_model_version(self, model_name: str) -> ModelVersion:
        """
          Get model version using mlflow client

        :param model_name: Name of a model.
        """
        return sorted(self._get_model_versions(model_name), key=lambda m: m.version, reverse=True)[0]

    def download_artifact(self,
                          model_name: str,
                          model_version: str,
                          artifact_path: str):
        """
          Download an artifact from mlflow model registry for the latest version of this model

        :param model_name: Name of a model.
        :param model_version: Version of a model.
        :param artifact_path: Path to the desired artifact.
        """
        run_id = self._client.get_model_version(model_name, model_version).run_id
        return self._client.download_artifacts(run_id, artifact_path)

    def search_model_versions(self, model_name: str) -> PagedList[ModelVersion]:
        """
          Search model versions with Mlflow client.

        :param model_name: Name of a model.
        """
        return self._client.search_model_versions(f"name='{model_name}'")

    def set_model_stage(self,
                        model_name: str,
                        model_version: str,
                        stage: str) -> ModelVersion:
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

    @staticmethod
    def load_model_by_name(model_name: str, stage: str) -> PyFuncModel:
        """
         Load model as pyfunc using models:/ api

        """
        return mlflow.pyfunc.load_model(f"models:/{model_name}/{stage}")

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
