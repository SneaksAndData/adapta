"""
  Connector for Boxer Auth API.
"""
import os
import jwt

from typing import Iterator

from proteus.connectors.boxer._auth import BoxerAuth
from proteus.connectors.boxer._helpers import (_iterate_user_claims_response,
                                               _iterate_boxer_claims_response)
from proteus.connectors.boxer._models import BoxerClaim, UserClaim
from proteus.utils import session_with_retries


class BoxerConnector:
    """
      Boxer Auth API connector
    """

    def __init__(self, *, base_url, retry_attempts=10):
        """ Creates Boxer Auth connector, capable of managing claims/consumers
        :param base_url: Base URL for Boxer Auth endpoint
        :param retry_attempts: Number of retries for Boxer-specific error messages
        """
        self.base_url = base_url
        self.http = session_with_retries()
        assert os.environ.get('BOXER_CONSUMER_ID'), "Environment BOXER_CONSUMER_ID not set"
        assert os.environ.get('BOXER_PRIVATE_KEY'), "Environment BOXER_PRIVATE_KEY not set"
        self.http.auth = BoxerAuth(private_key_base64=os.environ.get('BOXER_PRIVATE_KEY'),
                                   consumer_id=os.environ.get('BOXER_CONSUMER_ID'))
        self.retry_attempts = retry_attempts

    def push_user_claim(self, claim: BoxerClaim, user_id: str):
        """ Adds/Overwrites a new Boxer Claim to a user
        :param claim: Boxer Claim
        :param user_id: User's UPN
        :return:
        """
        target_url = f"{self.base_url}/claims/user/{user_id}"
        claim_json = claim.to_dict()
        response = self.http.post(target_url, json=claim_json)
        response.raise_for_status()
        print(f"Successfully pushed user claim for user {user_id}")

    def push_group_claim(self, claim: BoxerClaim, group_name: str):
        """ Adds/Overwrites a new Boxer Claim to a user
        :param claim: Boxer Claim
        :param group_name: Group Name
        :return:
        """
        target_url = f"{self.base_url}/claims/group/{group_name}"
        claim_json = claim.to_dict()
        response = self.http.post(target_url, json=claim_json)
        response.raise_for_status()
        print(f"Successfully pushed user claim for group {group_name}")

    def get_claims_by_type(self, claims_type: str) -> Iterator[UserClaim]:
        """Reads claims of specified type from Boxer.
        :param claims_type: claim type to filter claims by.
        :return: Iterator[UserClaim]
        """
        target_url = f"{self.base_url}/claims/type/{claims_type}"
        response = self.http.get(target_url)
        response.raise_for_status()
        return _iterate_user_claims_response(response)

    def get_claims_by_user(self, user_id: str) -> Iterator[BoxerClaim]:
        """ Reads user claims from Boxer
        :param user_id: user upn to load claims for
        :return: Iterator[UserClaim]
        """
        empty_user_token = jwt.encode({"upn": user_id}, "_", algorithm="HS256")
        target_url = f"{self.base_url}/claims/user/{empty_user_token}"
        response = self.http.get(target_url)
        response.raise_for_status()
        return _iterate_boxer_claims_response(response)

    def get_claims_by_group(self, group_name: str) -> Iterator[BoxerClaim]:
        """ Reads user claims from Boxer
        :param group_name: group name to load claims for
        :return: Iterator[UserClaim]
        """
        target_url = f"{self.base_url}/claims/group/{group_name}"
        response = self.http.get(target_url)
        response.raise_for_status()
        return _iterate_boxer_claims_response(response)

    def get_claims_for_token(self, jwt_token: str) -> Iterator[BoxerClaim]:
        """ Reads user claims from Boxer
        :param jwt_token: jwt token with UPN set
        :return: Iterator[UserClaim]
        """
        target_url = f"{self.base_url}/claims/user/{jwt_token}"
        response = self.http.get(target_url)
        response.raise_for_status()
        return _iterate_boxer_claims_response(response)
