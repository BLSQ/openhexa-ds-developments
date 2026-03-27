import polars as pl

from d2d_development.data_models import DataPointModel
from tests.mock_dhis2_get import MockDHIS2Client


def test_data_point_model_to_str():
    """Test conversion of a Polars DataFrame to JSON using the DataPointModel."""
    single_point = DataPointModel(
        dataElement="de1",
        period="202601",
        orgUnit="OU1",
        categoryOptionCombo="coc1",
        attributeOptionCombo="aoc1",
        value="100.2",
    )

    assert "dataElement=de1" in str(single_point)
    assert "period=202601" in str(single_point)
    assert "orgUnit=OU1" in str(single_point)
    assert "categoryOptionCombo=coc1" in str(single_point)
    assert "attributeOptionCombo=aoc1" in str(single_point)
    assert "value=100.2" in str(single_point)


def test_data_point_model_to_json():
    """Test conversion of a Polars DataFrame to JSON using the DataPointModel."""
    data_elements = pl.DataFrame(MockDHIS2Client().data_value_sets.get())
    single_point = DataPointModel(
        dataElement=data_elements[0]["dataElement"].item(),
        period=data_elements[0]["period"].item(),
        orgUnit=data_elements[0]["orgUnit"].item(),
        categoryOptionCombo=data_elements[0]["categoryOptionCombo"].item(),
        attributeOptionCombo=data_elements[0]["attributeOptionCombo"].item(),
        value=data_elements[0]["value"].item(),
    )

    payload = single_point.to_json()
    assert payload["dataElement"] == data_elements[0]["dataElement"].item()
    assert payload["period"] == data_elements[0]["period"].item()
    assert payload["orgUnit"] == data_elements[0]["orgUnit"].item()
    assert payload["categoryOptionCombo"] == data_elements[0]["categoryOptionCombo"].item()
    assert payload["attributeOptionCombo"] == data_elements[0]["attributeOptionCombo"].item()
    assert payload["value"] == data_elements[0]["value"].item()


def test_data_point_model_to_json_delete():
    """Test conversion of a Polars DataFrame to JSON using the DataPointModel."""
    data_elements = pl.DataFrame(MockDHIS2Client().data_value_sets.get()).slice(2, 2)

    # Set third datapoint to value None to simulate a deleted value
    data_elements = data_elements.with_columns(
        pl.when(pl.arange(0, data_elements.height) == 2).then(None).otherwise(pl.col("value")).alias("value")
    )
    points_list = [
        DataPointModel(
            dataElement=row["dataElement"],
            period=row["period"],
            orgUnit=row["orgUnit"],
            categoryOptionCombo=row["categoryOptionCombo"],
            attributeOptionCombo=row["attributeOptionCombo"],
            value=row["value"],
        ).to_json()
        for row in data_elements.to_dicts()
    ]

    assert len(points_list) == 2
    assert points_list[0]["dataElement"] == "CCC333"
    assert points_list[0]["period"] == "202501"
    assert points_list[0]["orgUnit"] == "ORG003"
    assert points_list[0]["categoryOptionCombo"] == "CAT003"
    assert points_list[0]["attributeOptionCombo"] == "ATTR003"
    assert points_list[0]["value"] == "25"
    assert points_list[0].get("comment") is None
    assert points_list[1]["dataElement"] == "DELETE1"
    assert not points_list[1]["value"]
    assert points_list[1]["comment"] == "deleted value"
