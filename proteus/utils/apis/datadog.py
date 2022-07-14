"""
 Utilities for Datadog providers.
"""
from typing import Optional

from datadog_api_client import Configuration, ApiClient
from datadog_api_client.v2.api.key_management_api import KeyManagementApi


def get_key_name(conf: Configuration) -> Optional[str]:
    """
     Reads a key name from Datadog Key Management
    :param conf: Datadog configuration object
    :return:
    """
    app_key = conf.api_key['appKeyAuth']
    app_key_last4 = app_key[len(app_key) - 4:]
    with ApiClient(conf) as api_client:
        keys = KeyManagementApi(api_client).list_current_user_application_keys()

        matching_keys = [key for key in keys.to_dict()['data'] if key['attributes']['last4'] == app_key_last4]

        if len(matching_keys) != 1:
            return None

        return matching_keys[0]['attributes']['name']
