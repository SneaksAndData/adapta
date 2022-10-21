from dataclasses import dataclass
from typing import Optional, Dict


@dataclass
class ProteusLogMetadata:
    template: str
    diagnostics: Optional[str] = None
    tags: Optional[Dict[str, str]] = None
    fields: Dict[str, str] = None

