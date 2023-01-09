"""
 Import index.
"""

from proteus.security.clients._local_client import LocalClient
from proteus.security.clients._base import ProteusClient

try:
    from proteus.security.clients.hashicorp_vault.oidc_client import HashicorpVaultOidcClient
except ImportError:
    pass

try:
    from proteus.security.clients.hashicorp_vault.kubernetes_client import HashicorpVaultKubernetesClient
except ImportError:
    pass

try:
    from proteus.security.clients.hashicorp_vault.hashicorp_vault_client import HashicorpVaultClient
except ImportError:
    pass

try:
    from proteus.security.clients._azure_client import AzureClient
except ImportError:
    pass
