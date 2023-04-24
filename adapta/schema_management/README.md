# Usage

Creating a data model for a table "Order":
```python
from dataclasses import field, dataclass
from datetime import datetime
from typing import Optional

from adapta.schema_management.schema_entity import PythonSchemaEntity

@dataclass
class Order:
	"""Data model for entity Order
	"""
	created_on: Optional[datetime] = field(metadata={"DisplayName": "Created On", "Description": "Date and time when the record was created."})
ORDER_PYTHON_SCHEMA: Order = PythonSchemaEntity(Order)
```

This allows you to reference field names directly from model class like this: `Order.created_on` will return `created_on`, which is very useful when working with dataframe libraries - removes the need to hardcode field names everywhere.
