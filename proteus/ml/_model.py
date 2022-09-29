"""Machine learning model module"""
from abc import ABC, abstractmethod


class MachineLearningModel(ABC):
    """Machine Learning base class"""

    @abstractmethod
    def load_model(self, path: str):
        """Loads model from path

        :param path: Path to model
        """

    @abstractmethod
    def save_model(self, path: str):
        """Saves model to path

        :param path: Path to store model
        """

    @abstractmethod
    def fit(self, **kwargs):
        """Fits machine learning model"""

    @abstractmethod
    def predict(self, **kwargs):
        """Predicts with machine learning model"""
