import polars as pl

from d2d_development.data_models import DataType
from d2d_development.extract import DHIS2Extractor


def test_extract_map_data_elements():
    """Test the mapping of data elements."""
    df = pl.DataFrame(
        {
            "dataType": [DataType.DATA_ELEMENT] * 3,
            "dx": ["de1", "de2", "de3"],
            "period": ["202201", "202202", "202203"],
            "orgUnit": ["ou1", "ou2", "ou3"],
            "categoryOptionCombo": ["coc1", "coc2", "coc3"],
            "attributeOptionCombo": ["aoc1", "aoc2", "aoc3"],
            "rateType": ["rt1", "rt2", "rt3"],
            "domainType": ["domain1", "domain2", "domain3"],
            "value": [10, 20, 30],
        }
    )
    extractor = DHIS2Extractor(dhis2_client=None)
    result = extractor._map_to_dhis2_format(df, data_type=DataType.DATA_ELEMENT)
    # extractor_de = DataElementsExtractor(extractor)
    # mapped_de = extractor_de._map_data_elements(raw_data, data_type=DataType.DATA_ELEMENT)
    assert result.shape == (3, 9)
