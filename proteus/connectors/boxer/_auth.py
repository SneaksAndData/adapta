"""
 Boxer Auth class
 Based on https://docs.python-requests.org/en/master/user/advanced/#custom-authentication
"""
import base64

from requests.auth import AuthBase
from Crypto.PublicKey import RSA
from Crypto.Signature.PKCS1_v1_5 import new as signature_factory
from Crypto.Hash.SHA256 import new as sha256_get_instance


class BoxerAuth(AuthBase):
    """Attaches HTTP Bearer Authentication to the given Request object sent to Boxer"""

    def __init__(self, *, private_key_base64: str, consumer_id: str):
        # setup any auth-related data here
        self._sign_key = private_key_base64
        self._consumer_id = consumer_id

    def _sign_string(self, input_string: str) -> str:
        """
          Signs input for Boxer

        :param input_string: input to generate signature for
        :return:
        """
        msg_bytes = input_string.encode('utf-8')
        digest = sha256_get_instance()

        private_key_bytes = base64.b64decode(self._sign_key)
        rsa_key = RSA.importKey(private_key_bytes, '')
        signer = signature_factory(rsa_key)

        digest.update(msg_bytes)
        signed = signer.sign(digest)
        return base64.b64encode(signed).decode('utf-8')

    def __call__(self, r):
        """
          Auth entrypoint

        :param r: Request to authorize
        :return: Request with Auth header set
        """
        payload = r.url.replace('https://', '').split('?')[0]
        signature_base64 = self._sign_string(payload)
        r.headers['Authorization'] = f"Signature {signature_base64}"
        r.headers['X-Boxer-ConsumerId'] = self._consumer_id
        r.headers['X-Boxer-Payload'] = payload

        return r
