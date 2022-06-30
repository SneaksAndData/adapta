""" Helper functions to parse responses
"""
from requests import Response

from proteus.connectors.boxer._models import UserClaim, BoxerClaim


def _iterate_user_claims_response(user_claim_response: Response):
    """ Creates an iterator to iterate user claims from Json Response
    :param user_claim_response: HTTP Response containing json array of type UserClaim
    """
    response_json = user_claim_response.json()

    if response_json:
        for api_response_item in response_json:
            yield UserClaim.from_dict(api_response_item)
    else:
        raise ValueError('Expected response body of type application/json')

def _iterate_boxer_claims_response(boxer_claim_response: Response):
    """ Creates an iterator to iterate user claims from Json Response
    :param boxer_claim_response: HTTP Response containing json array of type BoxerClaim
    """
    response_json = boxer_claim_response.json()

    if response_json:
        for api_response_item in response_json:
            yield BoxerClaim.from_dict(api_response_item)
    else:
        raise ValueError('Expected response body of type application/json')
