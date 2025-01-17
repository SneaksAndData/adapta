"""Mlflow python model module"""
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

import configparser
import importlib
import pathlib
import tempfile
from typing import Optional, Dict, Any, Literal

import mlflow
from mlflow.pyfunc import PythonModel

from adapta.ml.mlflow._client import MlflowBasicClient
from adapta.ml._model import MachineLearningModel


class _MlflowMachineLearningModel(PythonModel):
    """Machine learning model wrapper used for logging MachineLearning models
    as mlflow pyfunc models
    """

    def load_context(self, context):
        config = configparser.ConfigParser()
        config.read(context.artifacts["config"])
        module = importlib.import_module(config["model"]["module_name"])
        class_ = getattr(module, config["model"]["class_name"])
        self.model = class_.load_model(context.artifacts["model"])  # pylint: disable=W0201

    def predict(self, context, model_input, params: Optional[Dict[str, Any]] = None):
        return self.model.predict(**model_input)

    def predict_stream(self, context, model_input, params: Optional[Dict[str, Any]] = None):
        raise NotImplementedError("Predict stream is not currently supported")


def register_mlflow_model(
    model: MachineLearningModel,
    mlflow_client: MlflowBasicClient,
    model_name: str,
    experiment: str,
    run_name: Optional[str] = None,
    run_id: Optional[str] = None,
    transition_to_stage: Optional[Literal["staging", "production"]] = None,
    parent_run_id: Optional[str] = None,
    version_alias: Optional[str] = None,
    metrics: Optional[Dict[str, float]] = None,
    model_params: Optional[Dict[str, Any]] = None,
    artifacts_to_log: Dict[str, str] = None,
) -> str:
    """Registers mlflow model

    :param model: Machine learning model to register
    :param mlflow_client: Mlflow client
    :param model_name: Name of Mlflow model
    :param experiment: Name of Mlflow experiment
    :param run_name: Name of Mlflow run (only used if run_id is None)
    :param run_id: Run id
    :param parent_run_id: Parent run id
    :param transition_to_stage: Whether to transition to stage
    :param version_alias: Alias to assign to model
    :param metrics: Metrics to log
    :param model_params: Model hyperparameters to log
    :param artifacts_to_log: Additional artifacts to log

    :return: Run id of the newly created run for registering the model.
    If run_id is provided, it will be the same as run_id
    """
    assert transition_to_stage in [None, "staging", "production"]

    mlflow.set_experiment(experiment)

    path_model = pathlib.PurePath(tempfile.gettempdir(), "model")
    path_config = pathlib.PurePath(tempfile.gettempdir(), "config.ini")
    path_model = path_model.as_posix().replace(path_model.drive, "")
    model.save_model(path_model)

    config = configparser.ConfigParser()
    config["model"] = {
        "module_name": model.__module__,
        "class_name": model.__class__.__qualname__,
    }
    with open(path_config, "w", encoding="utf8") as file_stream:
        config.write(file_stream)

    artifacts = {
        "model": path_model,
        "config": str(path_config),
    }

    if artifacts_to_log is not None:
        if not any(list(artifacts_to_log.keys())) not in ["model", "config"]:
            raise ValueError('Artifact names "model" and "config" are reserved for internal usage')
        artifacts.update(artifacts_to_log)

    with mlflow.start_run(nested=True, run_name=run_name, run_id=run_id, parent_run_id=parent_run_id) as run:
        mlflow.pyfunc.log_model(
            artifact_path="mlflow_model",
            python_model=_MlflowMachineLearningModel(),
            registered_model_name=model_name,
            artifacts=artifacts,
        )

        if metrics is not None:
            mlflow.log_metrics(metrics)

        if model_params is not None:
            mlflow.log_params(model_params)

        version = mlflow_client.get_latest_model_version(model_name).version

        if version_alias is not None:
            mlflow_client.set_model_alias(model_name=model_name, alias=version_alias, model_version=version)

        if transition_to_stage is not None:
            mlflow_client.set_model_stage(
                model_name=model_name,
                stage=transition_to_stage,
                model_version=version,
            )

        return run.info.run_id
