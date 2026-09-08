"""
Import index.
"""

#  Copyright (c) 2023-2026. ECCO Data & AI and other project contributors.
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

from adapta.security.clients._base import AuthenticationClient as AuthenticationClient
from adapta.security.clients._local_client import LocalClient

try:
    from adapta.security.clients.hashicorp_vault.oidc_client import (
        HashicorpVaultOidcClient as HashicorpVaultOidcClient,
    )
except ImportError:
    pass

try:
    from adapta.security.clients.hashicorp_vault.kubernetes_client import (
        HashicorpVaultKubernetesClient as HashicorpVaultKubernetesClient,
    )
except ImportError:
    pass

try:
    from adapta.security.clients.hashicorp_vault.hashicorp_vault_client import (
        HashicorpVaultClient as HashicorpVaultClient,
    )
except ImportError:
    pass

try:
    from adapta.security.clients.hashicorp_vault.token_client import (
        HashicorpVaultTokenClient as HashicorpVaultTokenClient,
    )
except ImportError:
    pass

try:
    from adapta.security.clients._azure_client import AzureClient as AzureClient
except ImportError:
    pass

try:
    from adapta.security.clients.aws import AwsClient as AwsClient
except ImportError:
    pass
