"""
 Models for Boxer
"""
from dataclasses import dataclass
from typing import Dict


@dataclass
class BoxerClaim:
    """
     Boxer Claim
    """
    claim_type: str
    claim_value: str
    issuer: str

    def to_dict(self) -> Dict:
        return {
            "claimType": self.claim_type,
            "claimValue": self.claim_value,
            "issuer": self.issuer
        }

    @classmethod
    def from_dict(cls, json_data: Dict):
        return BoxerClaim(
            claim_type=json_data['claimType'],
            claim_value=json_data['claimValue'],
            issuer=json_data['issuer']
        )

@dataclass
class UserClaim:
    """
     Boxer User Claim
    """
    user_id: str
    user_claim_id: str
    claim: BoxerClaim

    def to_dict(self) -> Dict:
        return {
            "userId": self.user_id,
            "userClaimId": self.user_claim_id,
            "claim": self.claim.to_dict()
        }

    @classmethod
    def from_dict(cls, json_data: Dict):
        return UserClaim(
            user_id=json_data['userId'],
            user_claim_id=json_data['userClaimId'],
            claim=BoxerClaim.from_dict(json_data['claim'])
        )

@dataclass
class GroupClaim:
    """
     Boxer Group Claim
    """
    group_name: str
    group_claim_id: str
    claim: BoxerClaim

    def to_dict(self) -> Dict:
        return {
            "groupName": self.group_name,
            "groupClaimId": self.group_claim_id,
            "claim": self.claim.to_dict()
        }

    @classmethod
    def from_dict(cls, json_data: Dict):
        return GroupClaim(
            group_name=json_data['groupName'],
            group_claim_id=json_data['groupClaimId'],
            claim=BoxerClaim.from_dict(json_data['claim'])
        )
