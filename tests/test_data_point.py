import polars as pl

from d2d_development.data_models import DataPointModel


def test_data_point_model_to_str():
    """Test conversion of a Polars DataFrame to JSON using the DataPointModel."""
    single_point = DataPointModel(
        dataElement="de1",
        period="202601",
        orgUnit="OU1",
        categoryOptionCombo="coc1",
        attributeOptionCombo="aoc1",
        value=100.2,
    )

    assert "dataElement=de1" in str(single_point)
    assert "period=202601" in str(single_point)
    assert "orgUnit=OU1" in str(single_point)
    assert "categoryOptionCombo=coc1" in str(single_point)
    assert "attributeOptionCombo=aoc1" in str(single_point)
    assert "value=100.2" in str(single_point)


def test_data_point_model_to_json():
    """Test conversion of a Polars DataFrame to JSON using the DataPointModel."""
    df = pl.DataFrame(
        {
            "dataElement": ["de1", "de2", "de3"],
            "period": ["202601", "202602", "202603"],
            "orgUnit": ["OU1", "OU2", "OU3"],
            "categoryOptionCombo": ["COC1", "COC2", "COC3"],
            "attributeOptionCombo": ["AOC1", "AOC2", "AOC3"],
            "value": [100.0, None, 200.0],
        }
    )

    rows = df.to_dicts()
    json_list = [
        DataPointModel(
            dataElement=row["dataElement"],
            period=row["period"],
            orgUnit=row["orgUnit"],
            categoryOptionCombo=row["categoryOptionCombo"],
            attributeOptionCombo=row["attributeOptionCombo"],
            value=row.get("value"),
        ).to_json()
        for row in rows
    ]

    assert json_list[0]["value"] == 100.0
    assert json_list[1]["value"] == ""  # noqa: PLC1901
    assert json_list[1]["comment"] == "deleted value"
    assert json_list[2]["value"] == 200.0


def test_data_point_model_to_json_delete():
    """Test conversion of a Polars DataFrame to JSON using the DataPointModel."""
    df = pl.DataFrame(
        {
            "dataElement": ["de1", "de2"],
            "period": ["202601", "202602"],
            "orgUnit": ["OU1", "OU2"],
            "categoryOptionCombo": ["COC1", "COC2"],
            "attributeOptionCombo": ["AOC1", "AOC2"],
            "value": [100.0, None],
        }
    )

    rows = df.to_dicts()
    json_list = [
        DataPointModel(
            dataElement=row["dataElement"],
            period=row["period"],
            orgUnit=row["orgUnit"],
            categoryOptionCombo=row["categoryOptionCombo"],
            attributeOptionCombo=row["attributeOptionCombo"],
            value=row.get("value"),
        ).to_json()
        for row in rows
    ]

    assert json_list[0]["value"] == 100.0
    assert json_list[0].get("comment") is None
    assert json_list[1]["value"] == ""  # noqa: PLC1901
    assert json_list[1]["comment"] == "deleted value"
