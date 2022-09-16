import webbrowser
from typing import Optional, Dict
from urllib import parse

import hvac
from pyarrow._fs import PyFileSystem

from proteus.security.clients import ProteusClient
from proteus.storage.models.base import DataPath
from http.server import BaseHTTPRequestHandler, HTTPServer


def _get_vault_credentials():
    class HttpServ(HTTPServer):
        def __init__(self, *args, **kwargs):
            HTTPServer.__init__(self, *args, **kwargs)
            self.token = None

    class AuthHandler(BaseHTTPRequestHandler):
        token = ''

        def do_GET(self):
            params = parse.parse_qs(self.path.split('?')[1])
            self.server.token = params['code'][0]
            self.send_response(200)
            self.end_headers()
            self.wfile.write(str.encode('<div>Authentication successful, you can close the browser now.</div>'))

    server_address = ('127.0.0.1', 8250)
    httpd = HttpServ(server_address, AuthHandler)
    httpd.handle_request()
    return httpd.token


class HashicorpVaultClient(ProteusClient):
    TEST_VAULT_ADDRESS = "https://hashicorp-vault.test.sneaksanddata.com/"
    PRODUCTION_VAULT_ADDRESS = "https://hashicorp-vault.test.sneaksanddata.com/"

    def __init__(self, vault_address):
        self._vault_address = vault_address

    @classmethod
    def from_base_client(cls, client: ProteusClient) -> Optional['HashicorpVaultClient']:
        """
         Safe casts ProteusClient to HashicorpVaultClient if type checks out.

        :param client: ProteusClient
        :return: HashicorpVaultClient or None if type does not check out
        """
        if isinstance(client, HashicorpVaultClient):
            return client

        return None

    def get_credentials(self):
        client = hvac.Client(url=self._vault_address)
        auth_url_response = client.auth.oidc.oidc_authorization_url_request(
            role=None,
            redirect_uri='http://localhost:8250/oidc/callback',
            path='oidc'
        )
        auth_url = auth_url_response['data']['auth_url']
        if auth_url == '':
            return None

        params = parse.parse_qs(auth_url.split('?')[1])
        auth_url_nonce = params['nonce'][0]
        auth_url_state = params['state'][0]

        webbrowser.open(auth_url)
        token = _get_vault_credentials()
        auth_result = client.auth.oidc.oidc_callback(
            code=token, path='oidc', nonce=auth_url_nonce, state=auth_url_state
        )
        return auth_result

    def get_access_token(self, scope: Optional[str] = None) -> str:
        return self.get_credentials()["auth"]["token"]

    def connect_storage(self, path: DataPath, set_env: bool = False) -> Optional[Dict]:
        """
         Not supported  in HashicorpVaultClient
        :return:
        """

    def connect_account(self):
        """
         Not supported  in HashicorpVaultClient
        :return:
        """

    def get_pyarrow_filesystem(self, path: DataPath) -> PyFileSystem:
        """
         Not supported  in HashicorpVaultClient
        :return:
        """

