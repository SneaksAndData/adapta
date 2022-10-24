"""
 Hashicorp Vault implementation of Proteus Client.
"""
import webbrowser
from typing import Optional, Dict
from urllib import parse
from http.server import BaseHTTPRequestHandler, HTTPServer
from pyarrow.fs import FileSystem

import hvac

from proteus.security.clients import HashicorpVaultClient
from proteus.security.clients._base import ProteusClient
from proteus.storage.models.base import DataPath


def _get_vault_credentials():
    class HttpServ(HTTPServer):
        """Http server for handling login responses"""

        def __init__(self, *args, **kwargs):
            super(HTTPServer, self).__init__(*args, **kwargs)
            self.token = None

    class AuthHandler(BaseHTTPRequestHandler):
        """Authentication handler"""
        token = ''

        def do_GET(self):  # pylint: disable=C0103
            """Handles GET request and collects token"""

            params = parse.parse_qs(self.path.split('?')[1])
            self.server.token = params['code'][0]
            self.send_response(200)
            self.end_headers()
            self.wfile.write(str.encode('<div>Authentication successful, you can close the browser now.</div>'))

    server_address = ('127.0.0.1', 8250)
    httpd = HttpServ(server_address, AuthHandler)
    httpd.handle_request()
    return httpd.token


class HashicorpVaultOidcClient(HashicorpVaultClient):
    """
     Hashicorp vault Credentials provider.
    """
    def __init__(self, vault_address):
        super().__init__(vault_address)

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
        return self.get_credentials()["auth"]["client_token"]
