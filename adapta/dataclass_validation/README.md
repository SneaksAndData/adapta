# Dataclass Validation framework

Framework for validation dataframe(s) against dataclass schema(s). Currently supported dataframe types are:
- Polars

If a new dataframe type is needed, one simply needs to implement the `AbstractValidationClass` class for the new dataframe type and add support for it in the `AbstractDataClass`. 

Also, if new field attribute or checks are needed, the framework is designed to be easily extensible.  

## Example usage

### Setup dataclass schema 
```python
from adapta.dataclass_validation import AbstractDataClass, Field, Checks


# Setup an example dataclass
class ExampleDataClass(AbstractDataClass):
    column_1 = Field(display_name="Column 1", description="Description for column 1.", dtype=str, primary_key=True)
    column_2 = Field(
        display_name="Column 2",
        description="Description for column 2.",
        dtype=float,
        required=False,
        required_by_settings=["setting_1"],
        coerce=True,
        checks=Checks(ge_value=0.0),
        allow_missing_values=True,
    )
    column_3 = Field(
        display_name="Column 3",
        description="Description for column 3.",
        dtype=float,
        required=False,
        required_by_settings=["setting_2"],
        checks=Checks(ge_value=0.0, le_value=0.0),
        allow_missing_values=True,
    )
    column_4 = Field(
        display_name="Column 4",
        description="Description for column 4.",
        required=False,
        dtype=bool,
        add_field_if_missing=True,
        allow_missing_values=True,
    )
    column_5 = Field(
        display_name="Column 5",
        description="Description for column 5.",
        dtype=str,
        required=True,
        allow_missing_values=False,
    )


# Make the schema
EXAMPLE_SCHEMA = ExampleDataClass()
```

### Validate a dataframe against the dataclass schema
```python
import polars as pl
# Create an example Polars DataFrame that matches the schema
example_dataframe = pl.DataFrame(
    {
        EXAMPLE_SCHEMA.column_1: ["A", "B", "C"],
        EXAMPLE_SCHEMA.column_2: ["test1", "test2", "test3"],
        EXAMPLE_SCHEMA.column_3: [1.0, 2.0, 3.0],
        EXAMPLE_SCHEMA.column_4: [True, False, True],
        EXAMPLE_SCHEMA.column_5: ["test1", "test2", None],
    }
)

# Validate - this intentionally fails due to:
# 1) column_2 can't be coerced since it's string and expects a float value
# 2) column_2 will have incorrect datatype, since it's string and didn't cast to float
# 3) column_3 does not satisfy the less than or equal to constraint. Should be less than 0.0.
# 4) column_5 does not allow missing values but contains missing values.
EXAMPLE_SCHEMA.validate_data(
    data=example_dataframe,
    settings=["setting_1"],
)
```

### Validate multiple dataframes against the dataclass schema
```python
import polars as pl
from adapta.dataclass_validation import AbstractDataClass, Field, ValidationClass, Checks


# Setup an example dataclass
class Example1DataClass(AbstractDataClass):
    column_1 = Field(display_name="Column 1", description="Description for column 1.", dtype=str, primary_key=True)
    column_2 = Field(
        display_name="Column 2",
        description="Description for column 2.",
        dtype=float,
        required=False,
        required_by_settings=["setting_1"],
        coerce=True,
        checks=Checks(ge_value=0.0),
        allow_missing_values=True,
    )
    column_3 = Field(
        display_name="Column 3",
        description="Description for column 3.",
        dtype=float,
        required=False,
        required_by_settings=["setting_2"],
        checks=Checks(ge_value=0.0, le_value=0.0),
        allow_missing_values=True,
    )
    column_4 = Field(
        display_name="Column 4",
        description="Description for column 4.",
        required=False,
        dtype=bool,
        add_field_if_missing=True,
        allow_missing_values=True,
    )
    column_5 = Field(
        display_name="Column 5",
        description="Description for column 5.",
        dtype=str,
        required=False,
        allow_missing_values=False,
    )


class Example2DataClass(AbstractDataClass):
    column_1 = Field(display_name="Column 1", description="Description for column 1.", dtype=str, primary_key=True)
    column_2 = Field(
        display_name="Column 2",
        description="Description for column 2.",
        dtype=list[list[float]],
        allow_missing_values=True,
    )
    column_3 = Field(
        display_name="Column 3", description="Description for column 3.", dtype=str, allow_missing_values=False
    )


class InheritExampleDataclass(Example2DataClass):
    """
    Example of inheriting from another class
    """


# Make the schema
EXAMPLE_SCHEMA_1 = Example1DataClass()
EXAMPLE_SCHEMA_2 = Example2DataClass()
EXAMPLE_SCHEMA_3 = InheritExampleDataclass()

# Create an example Polars DataFrame that matches the schema
example_dataframe_1 = pl.DataFrame(
    {
        EXAMPLE_SCHEMA_1.column_1: ["A", "B", "C"],
        EXAMPLE_SCHEMA_1.column_2: ["test1", "test2", "test3"],
        EXAMPLE_SCHEMA_1.column_3: [1.0, 2.0, 3.0],
        EXAMPLE_SCHEMA_1.column_4: [True, False, True],
        EXAMPLE_SCHEMA_1.column_5: ["test1", "test2", None],  # No validation failure despite None because not required
    }
)
example_dataframe_2 = pl.DataFrame(
    {
        EXAMPLE_SCHEMA_2.column_1: ["X", "Y"],
        EXAMPLE_SCHEMA_2.column_2: [[[1, 2], [3, 4]], [[5, 6], [7, 8]]],
        EXAMPLE_SCHEMA_2.column_3: ["A", None],
    },
    strict=False,
)
example_dataframe_3 = pl.DataFrame(
    {
        EXAMPLE_SCHEMA_3.column_1: ["X", "Y"],
        EXAMPLE_SCHEMA_3.column_2: [[[1, 2.0], [3, 4.0]], [[5, 6.0], [7, 8.0]]],
        EXAMPLE_SCHEMA_3.column_3: ["A", None],
    },
    strict=False,
)

### This is how to validate ###
# Setup match between the DataFrame and the schema
validations = [
    (example_dataframe_1, EXAMPLE_SCHEMA_1),
    (example_dataframe_2, EXAMPLE_SCHEMA_2),
    (example_dataframe_3, EXAMPLE_SCHEMA_3),
]

# Validate - this intentionally fails due to:
# For Example1DataClass:
# 1) column_2 can't be coerced since it's string and expects a float value
# 2) column_2 will have incorrect datatype, since it's string and didn't cast to float
# 3) If "setting_2" is on: column_3 does not satisfy the less than or equal to constraint. Should be less than 0.0.

# For Example2DataClass:
# 1) column_2 has incorrect datatype. Should be List[List[Float]], but is List[List[Int]].
# 2) 'column_3' does not allow missing values but contains missing values.

# For InheritExampleDataclass:
# 1) 'column_3' does not allow missing values but contains missing values.
ValidationClass().validate(validations=validations, settings=["setting_1"])
```

### Other functionalities
The dataclass framework also support multiple other functionalities such as:
- Get primary keys: `EXAMPLE_SCHEMA.get_primary_keys()`
- Get required fields based on settings: `EXAMPLE_SCHEMA.get_required_fields(settings=["setting_1"])`
- Create empty Polars dataframe with correct schema: `EXAMPLE_SCHEMA.create_empty_polars_dataframe()`
