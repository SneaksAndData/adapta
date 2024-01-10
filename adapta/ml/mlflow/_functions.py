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
from typing import Optional, Dict, Any

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

    def predict(self, context, model_input):
        return self.model.predict(**model_input)


def register_mlflow_model(
    model: MachineLearningModel,
    mlflow_client: MlflowBasicClient,
    model_name: str,
    experiment: str,
    run_name: str = None,
    transition_to_stage: str = None,
    metrics: Optional[Dict[str, float]] = None,
    model_params: Optional[Dict[str, Any]] = None,
    artifacts_to_log: Dict[str, str] = None,
):
    """Registers mlflow model

    :param model: Machine learning model to register
    :param mlflow_client: Mlflow client
    :param model_name: Name of Mlflow model
    :param experiment: Name of Mlflow experiment
    :param run_name: Name of Mlflow run
    :param transition_to_stage: Whether to transition to stage
    :param metrics: Metrics to log
    :param model_params: Model hyperparameters to log
    :param artifacts_to_log: Additional artifacts to log
    """
    assert transition_to_stage in [None, "Staging", "Production"]

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

    with mlflow.start_run(nested=True, run_name=run_name):
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

        if transition_to_stage is not None:
            mlflow_client.set_model_stage(
                model_name=model_name,
                model_version=mlflow_client.get_latest_model_version(model_name).version,
                stage=transition_to_stage,
            )
