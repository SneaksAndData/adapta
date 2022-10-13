"""
 Import index.
"""

from proteus.security.clients._local_client import LocalClient
from proteus.security.clients._base import ProteusClient
try:
    from proteus.security.clients._hashicorp_vault_client import HashicorpVaultClient
except ImportError:
    pass

try:
    from proteus.security.clients._azure_client import AzureClient
except ImportError:
    pass
