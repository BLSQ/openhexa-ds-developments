from d2d_development.extract import DHIS2Extractor
from tests.mock_dhis2 import MockDHIS2Client


def test_extract_map_data_elements():
    """Test the mapping of data elements."""
    result = DHIS2Extractor(dhis2_client=MockDHIS2Client()).data_elements._retrieve_data(
        data_elements=[], org_units=[], period="202501"
    )
    assert result.shape == (3, 9)
    assert result.columns == [
        "dataType",
        "dx",
        "period",
        "orgUnit",
        "categoryOptionCombo",
        "attributeOptionCombo",
        "rateMetric",
        "domainType",
        "value",
    ]
    assert result["dataType"].unique().to_list() == ["DATA_ELEMENT"]
    assert result["dx"].to_list() == ["AAA111", "BBB222", "CCC333"]
    assert result["period"].to_list() == ["202501", "202501", "202501"]
    assert result["orgUnit"].to_list() == ["ORG001", "ORG002", "ORG003"]
    assert result["categoryOptionCombo"].to_list() == ["CAT001", "CAT002", "CAT003"]
    assert result["attributeOptionCombo"].to_list() == ["ATTR001", "ATTR002", "ATTR003"]
    assert result["rateMetric"].to_list() == [None, None, None]
    assert result["domainType"].to_list() == ["AGGREGATED", "AGGREGATED", "AGGREGATED"]
    assert result["value"].to_list() == ["12", "18", "25"]


def test_extract_map_reporting_rates():
    """Test the mapping of reporting rates."""
    result = DHIS2Extractor(dhis2_client=MockDHIS2Client()).reporting_rates._retrieve_data(
        reporting_rates=["AAA111.REPORTING_RATE", "BBB222.EXPECTED_REPORTS"], org_units=[], period="202409"
    )
    assert result.shape == (3, 9)
    assert result.columns == [
        "dataType",
        "dx",
        "period",
        "orgUnit",
        "categoryOptionCombo",
        "attributeOptionCombo",
        "rateMetric",
        "domainType",
        "value",
    ]
    assert result["dataType"].unique().to_list() == ["REPORTING_RATE"]
    assert result["dx"].to_list() == ["AAA111", "BBB222", "CCC333"]
    assert result["period"].to_list() == ["202409", "202409", "202409"]
    assert result["orgUnit"].to_list() == ["OU001", "OU002", "OU003"]
    assert result["categoryOptionCombo"].to_list() == [None, None, None]
    assert result["attributeOptionCombo"].to_list() == [None, None, None]
    assert result["rateMetric"].to_list() == ["REPORTING_RATE", "EXPECTED_REPORTS", "REPORTING_RATE"]
    assert result["domainType"].to_list() == ["AGGREGATED", "AGGREGATED", "AGGREGATED"]
    assert result["value"].to_list() == ["100", "0", "100"]


def test_extract_map_indicator():
    """Test the mapping of indicators."""
    result = DHIS2Extractor(dhis2_client=MockDHIS2Client()).indicators._retrieve_data(
        indicators=["AAA999"], org_units=[], period="202501"
    )
    assert result.shape == (3, 9)
    assert result.columns == [
        "dataType",
        "dx",
        "period",
        "orgUnit",
        "categoryOptionCombo",
        "attributeOptionCombo",
        "rateMetric",
        "domainType",
        "value",
    ]
    assert result["dataType"].unique().to_list() == ["INDICATOR"]
    assert result["dx"].to_list() == ["INDICATOR1", "INDICATOR2", "INDICATOR3"]
    assert result["period"].to_list() == ["202501", "202501", "202501"]
    assert result["orgUnit"].to_list() == ["ORG001", "ORG002", "ORG003"]
    assert result["categoryOptionCombo"].to_list() == [None, None, None]
    assert result["attributeOptionCombo"].to_list() == [None, None, None]
    assert result["rateMetric"].to_list() == [None, None, None]
    assert result["domainType"].to_list() == ["AGGREGATED", "AGGREGATED", "AGGREGATED"]
    assert result["value"].to_list() == ["5.0", "7.0", "9.0"]
