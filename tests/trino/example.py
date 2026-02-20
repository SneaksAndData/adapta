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
import os
from datetime import date, timedelta

import polars as pl
from adapta.process_communication import DataSocket
from adapta.security.clients import HashicorpVaultOidcClient
from adapta.storage.models.filter_expression import FilterField
from adapta.storage.query_enabled_store import QueryEnabledStore
from adapta.storage.secrets.hashicorp_vault_secret_storage_client import HashicorpSecretStorageClient

pl.Config.set_tbl_rows(50)
pl.Config.set_tbl_cols(500)
pl.Config.set_tbl_width_chars(10000)

secrets_client = HashicorpSecretStorageClient(
    base_client=HashicorpVaultOidcClient("https://hashicorp-vault.production.sneaksanddata.com/")
)
secrets = {
    **secrets_client.read_secret(
        storage_name="secret",
        secret_name="applications/trino/production-0/password_users",
    ),
}

os.environ["ADAPTA__TRINO_USERNAME"] = secrets["OR_STREAMLIT_USERNAME"]
os.environ["ADAPTA__TRINO_PASSWORD"] = secrets["OR_STREAMLIT_PASSWORD"]
os.environ[
    "NEXUS__QES_CONNECTION_STRING"
] = 'qes://engine=TRINO;plaintext_credentials={};settings={"host": "trino.awsp.sneaksanddata.com", "port": "443"}'


query = """
    WITH ranked_forecasts AS (
      SELECT
        SPLIT_PART(forecast_granularity, '::', 1) AS sku_key,
        SPLIT_PART(forecast_granularity, '::', 2) AS location_key,
        parameter_value,
        forecast_date,
        period_date_from,
        period_date_to,
        model_name,
        ROW_NUMBER() OVER (
          PARTITION BY
            SPLIT_PART(forecast_granularity, '::', 1),
            SPLIT_PART(forecast_granularity, '::', 2),
            period_date_from,
            period_date_to
          ORDER BY forecast_date DESC
        ) AS rn
      FROM lakehouse.forecasting.parametric
      WHERE
        forecast_granularity_type = 'sku_key::location_key'
        AND distribution = 'poisson'
        AND period_date_to >= CURRENT_DATE
    )
    SELECT
      rf.sku_key,
      rf.location_key,
      rf.parameter_value,
      rf.forecast_date AS latest_forecast_date,
      rf.period_date_from,
      rf.period_date_to,
      rf.model_name,
      loc.country_code
    FROM ranked_forecasts rf
    LEFT JOIN lakehouse.advanced_analytics.location loc
      ON rf.location_key = loc.location_key
    WHERE rf.rn = 1
    ORDER BY rf.period_date_from
"""

store = QueryEnabledStore.from_string(connection_string=os.environ["NEXUS__QES_CONNECTION_STRING"])
socket = DataSocket(
    alias="?",
    data_path=f"trino://{query}",
    data_format="?",
)

today = date.today()
simulation_length = 14
simulation_end = today + timedelta(days=simulation_length)

filters = (
    (FilterField("country_code").isin(["DK"]))
    & (FilterField("period_date_from") >= today - timedelta(weeks=1))
    & (FilterField("period_date_to") <= simulation_end)
    & (FilterField("period_date_to") >= today)
)

result = (
    store.open(socket.parse_data_path())
    .select(*["sku_key", "location_key", "parameter_value"])
    .filter(filters)
    .limit(5)
    .read()
    .to_polars()
)

print(result)
# print(result["location_key"].unique().to_list())
