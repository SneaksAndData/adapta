#  Copyright (c) 2023-2026. ECCO Data & AI and other project contributors.
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
import datetime

import pytest

from adapta.storage.models.expression_dsl.trino_filter_expression import TrinoFilterExpression


@pytest.mark.parametrize(
    "value, expected_value",
    [
        ("test", "'test'"),
        (123, "123"),
        (45.67, "45.67"),
        (True, "True"),
        (False, "False"),
        (None, "NULL"),
        (datetime.date(2023, 10, 1), "DATE '2023-10-01'"),
        (datetime.datetime(2023, 10, 1, 12, 30, 45), "TIMESTAMP '2023-10-01 12:30:45'"),
    ],
)
def test_format_value_expected(value: any, expected_value: str):
    """
    Test the _format_value method of TrinoFilterExpression.
    """
    assert TrinoFilterExpression._format_value(value) == expected_value
