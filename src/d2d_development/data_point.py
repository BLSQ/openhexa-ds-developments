import pandas as pd


class DataPoint:
    """Helper class definition to store/create the correct DataElement JSON format."""

    def __init__(self, series_row: pd.Series):
        """Create a new org unit instance.

        Parameters
        ----------
        series_row : pandas series
            Expects columns with names :
                ['DATA_TYPE',
                'DX_UID',
                'PERIOD',
                'ORG_UNIT',
                'CATEGORY_OPTION_COMBO',
                'ATTRIBUTE_OPTION_COMBO',
                'RATE_TYPE',
                'DOMAIN_TYPE',
                'VALUE']
        """
        row = series_row.squeeze()
        self.dataType = row.get("DATA_TYPE")
        self.dataElement = row.get("DX_UID")
        self.period = row.get("PERIOD")
        self.orgUnit = row.get("ORG_UNIT")
        self.categoryOptionCombo = row.get("CATEGORY_OPTION_COMBO")
        self.attributeOptionCombo = row.get("ATTRIBUTE_OPTION_COMBO")
        self.value = row.get("VALUE")

    def to_json(self) -> dict:
        """Return a dictionary representation of the data point suitable for DHIS2 JSON format.

        Returns
        -------
        dict
            A dictionary with keys corresponding to DHIS2 data value fields.
        """
        return {
            "dataElement": self.dataElement,
            "period": self.period,
            "orgUnit": self.orgUnit,
            "categoryOptionCombo": self.categoryOptionCombo,
            "attributeOptionCombo": self.attributeOptionCombo,
            "value": self.value,
        }

    def to_delete_json(self) -> dict:
        """Return a dictionary representation of the data point for deletion in DHIS2 JSON format.

        Returns
        -------
        dict
            A dictionary with keys corresponding to DHIS2 data value fields.
            The attr 'value' is set to an empty string and 'comment' attr is added to indicate deletion.
        """
        return {
            "dataElement": self.dataElement,
            "period": self.period,
            "orgUnit": self.orgUnit,
            "categoryOptionCombo": self.categoryOptionCombo,
            "attributeOptionCombo": self.attributeOptionCombo,
            "value": "",
            "comment": "deleted value",
        }

    def _check_attributes(self, exclude_value: bool = False) -> bool:
        """Check if all attributes are not None.

        Parameters
        ----------
        exclude_value : bool, optional
            If True, the 'value' attribute is excluded from the check, by default False.

        Returns
        -------
        bool
            True if all relevant attributes are not None, False otherwise.
        """
        # List of attributes to check, optionally excluding the 'value' attribute
        attributes = [self.dataElement, self.period, self.orgUnit, self.categoryOptionCombo, self.attributeOptionCombo]
        if not exclude_value:
            attributes.append(self.value)

        # Return True if all attributes are not None
        return all(attr is not None for attr in attributes)

    def is_valid(self) -> bool:
        """Check if mandatory attributes are valid (not None).

        Returns
        -------
        bool
            True if all attributes are not None, False otherwise.
        """
        # Check if all attributes are valid (None check)
        return self._check_attributes(exclude_value=False)

    def is_to_delete(self) -> bool:
        """Check if the data point is marked for deletion.

        Returns
        -------
        bool
            True if all attributes except 'value' are not None and 'value' is None, False otherwise.
        """
        # Check if all attributes except 'value' are not None and 'value' is None
        return self._check_attributes(exclude_value=True) and self.value is None

    def __str__(self) -> str:
        return f"DataPoint({self.dataType} id:{self.dataElement} pe:{self.period} ou:{self.orgUnit} value:{self.value})"
