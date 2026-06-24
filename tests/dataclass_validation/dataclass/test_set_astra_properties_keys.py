import pytest

from adapta.dataclass_validation import Field, AbstractDataClass
from adapta.dataclass_validation.dataclass.dataclass_core import AstraProperties


class DataClassWithField(AbstractDataClass):
    field_1: Field = Field(
        display_name="v",
        description="d",
        dtype=str,
        primary_key=True,
        astra_properties=AstraProperties(partition_key=True),
    )
    field_2: Field = Field(
        display_name="v", description="d", dtype=str, astra_properties=AstraProperties(partition_key=True)
    )
    field_3: Field = Field(
        display_name="v", description="d", dtype=str, astra_properties=AstraProperties(custom_index=True)
    )
    field_4: Field = Field(
        display_name="v",
        description="d",
        dtype=str,
        astra_properties=AstraProperties(vector_enabled=True, custom_index=True),
    )


def test__set_astra_properties_keys__expected_astra_properties():
    """
    Test that the _set_astra_properties_keys method works as expected.
    """

    # Arrange and Act
    TEST_SCHEMA = DataClassWithField()

    # Assert
    assert TEST_SCHEMA.get_astra_partition_keys() == ["field_1", "field_2"]
    assert TEST_SCHEMA.get_astra_custom_index_keys() == ["field_3", "field_4"]
    assert TEST_SCHEMA.get_astra_vector_enabled_keys() == ["field_4"]


class DataClassWithFieldWithoutPrimaryKey(AbstractDataClass):
    field_1: Field = Field(
        display_name="v", description="d", dtype=str, astra_properties=AstraProperties(partition_key=True)
    )


def test__set_astra_properties_keys__expected_raise():
    """
    Test that the _set_astra_properties_keys method raises an error when properties are defined without a primary key.
    """

    # Arrange and Act
    with pytest.raises(
        ValueError,
        match="Data class DataClassWithFieldWithoutPrimaryKey must have at least one primary key defined to use Astra properties.",
    ):
        TEST_SCHEMA = DataClassWithFieldWithoutPrimaryKey()
