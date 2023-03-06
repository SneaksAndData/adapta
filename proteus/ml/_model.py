"""Machine learning model module"""
#  Copyright (c) 2023. ECCO Sneaks & Data
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
