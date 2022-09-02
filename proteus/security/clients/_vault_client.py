import webbrowser
from typing import Optional, Dict
from urllib import parse

import hvac
from proteus.security.clients import ProteusClient
from proteus.storage.models.base import DataPath


class VaultClient(ProteusClient):
    TEST = "hashicorp-vault.test.sneaksanddata.com"

    def get_credentials(self):
        client = hvac.Client(url=f"https://{self.TEST}/")
        auth_url_response = client.auth.oidc.oidc_authorization_url_request(
            role=None,
            redirect_uri=f'https://{self.TEST}/ui/vault/auth/oidc/oidc/callback',
            path='oidc'
        )
        auth_url = auth_url_response['data']['auth_url']
        if auth_url == '':
            return None

        params = parse.parse_qs(auth_url.split('?')[1])
        auth_url_nonce = params['nonce'][0]
        auth_url_state = params['state'][0]

        webbrowser.open(auth_url)
        #token = login_odic_get_token()
        return auth_url_response

    def get_access_token(self, scope: Optional[str] = None) -> str:
        pass

    def connect_storage(self, path: DataPath, set_env: bool = False) -> Optional[Dict]:
        pass

    def connect_account(self):
        pass