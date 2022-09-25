"""Mlflow python model module"""
import configparser
import importlib
import pathlib
import tempfile
from abc import abstractmethod, ABC
from typing import Optional, Dict

import mlflow
from mlflow.pyfunc import PythonModel

from proteus.ml import MlflowBasicClient


class MachineLearningModel(ABC):
    """"""

    @abstractmethod
    def load_model(self):
        pass

    @abstractmethod
    def save_model(self):
        pass

    @abstractmethod
    def fit(self, **kwargs):
        pass

    @abstractmethod
    def predict(self, **kwargs):
        pass


class _MlflowMachineLearningModel(PythonModel):
    def load_context(self, context):
        config = configparser.ConfigParser()
        config.read(context.artifacts['config'])
        module = importlib.import_module(config['model']['module_name'])
        class_ = getattr(module, config['model']['class_name'])
        self.model = class_.load_model(context.artifacts['model'])  # pylint: disable=W0201

    def predict(self, context, model_input):
        return self.model.predict(**model_input)


def register_mlflow_model(
    model: MachineLearningModel,
    mlflow_client: MlflowBasicClient,
    model_name: str,
    experiment: str,
    run_name: str = None,
    transition_to_stage: str = None,
    metrics: Optional[Dict[str, float]] = None
):
    """Registers mlflow model"""
    assert transition_to_stage in [None, 'Staging', 'Production']

    mlflow.set_experiment(experiment)

    path_model = pathlib.PurePath(tempfile.gettempdir(), 'model')
    path_config = pathlib.PurePath(tempfile.gettempdir(), 'config.ini')
    path_model = path_model.as_posix().replace(path_model.drive, '')
    model.save_model(path_model)

    config = configparser.ConfigParser()
    config['model'] = {
        'module_name': model.__module__,
        'class_name': model.__class__.__qualname__,
    }
    with open(path_config, 'w') as file_stream:
        config.write(file_stream)

    artifacts = {
        'model': path_model,
        'config': str(path_config),
    }

    with mlflow.start_run(nested=True, run_name=run_name):

        mlflow.pyfunc.log_model(
            artifact_path='mlflow_model',
            python_model=_MlflowMachineLearningModel(),
            registered_model_name=model_name,
            artifacts=artifacts,
        )

        if metrics is not None:
            mlflow.log_metrics(metrics)

        if transition_to_stage is not None:
            mlflow_client.set_model_stage(
                model_name=model_name,
                model_version=mlflow_client.get_latest_model_version(model_name).version,
                stage=transition_to_stage,
            )
