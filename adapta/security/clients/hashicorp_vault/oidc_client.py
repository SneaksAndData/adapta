"""
 Hashicorp Vault implementation of AuthenticationClient.
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

import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional
from urllib import parse

import hvac

from adapta.security.clients._base import AuthenticationClient
from adapta.security.clients.hashicorp_vault.hashicorp_vault_client import (
    HashicorpVaultClient,
)


def _get_vault_credentials():
    class HttpServ(HTTPServer):
        """Http server for handling login responses"""

        def __init__(self, *args, **kwargs):
            super(HTTPServer, self).__init__(*args, **kwargs)
            self.token = None

    class AuthHandler(BaseHTTPRequestHandler):
        """Authentication handler"""

        token = ""

        def do_GET(self):  # pylint: disable=C0103
            """Handles GET request and collects token"""

            params = parse.parse_qs(self.path.split("?")[1])
            self.server.token = params["code"][0]
            self.send_response(200)
            self.end_headers()
            self.wfile.write(
                str.encode(
                    "<script>window.close()</script><div>Authentication successful,"
                    " you can close the browser now.</div>"
                )
            )

    server_address = ("127.0.0.1", 8250)
    httpd = HttpServ(server_address, AuthHandler)
    httpd.handle_request()
    return httpd.token


class HashicorpVaultOidcClient(HashicorpVaultClient):
    """
    Credentials provider for OIDC.
    """

    @staticmethod
    def from_base_client(
        client: AuthenticationClient,
    ) -> Optional["HashicorpVaultClient"]:
        """
         Safe casts AuthenticationClient to HashicorpVaultClient if type checks out.

        :param client: AuthenticationClient
        :return: HashicorpVaultClient or None if type does not check out
        """
        if isinstance(client, HashicorpVaultClient):
            return client

        return None

    def get_credentials(self):
        client = hvac.Client(url=self._vault_address)
        auth_url_response = client.auth.oidc.oidc_authorization_url_request(
            role=None, redirect_uri="http://localhost:8250/oidc/callback", path="oidc"
        )
        auth_url = auth_url_response["data"]["auth_url"]
        if auth_url == "":
            return None

        params = parse.parse_qs(auth_url.split("?")[1])
        auth_url_nonce = params["nonce"][0]
        auth_url_state = params["state"][0]

        webbrowser.open(auth_url)
        token = _get_vault_credentials()
        auth_result = client.auth.oidc.oidc_callback(
            code=token, path="oidc", nonce=auth_url_nonce, state=auth_url_state
        )
        return auth_result

    def get_access_token(self, scope: Optional[str] = None) -> str:
        return self.get_credentials()["auth"]["client_token"]
