import json
from typing import NamedTuple

import pandas as pd


class OrgUnitRow(NamedTuple):
    """Helper object definition to represent an organizational unit."""

    id: str
    name: str
    shortName: str  # noqa: N815
    openingDate: str  # noqa: N815
    closedDate: str | None  # noqa: N815
    parent: dict | None
    level: int
    path: str
    geometry: str | dict | None


class OrgUnitObj:  # noqa: PLW1641 (no hashing)
    """Helper class definition to store/create the correct OrgUnit JSON format."""

    def __init__(self, org_unit: OrgUnitRow | pd.Series | tuple):
        """Create a new org unit instance.

        Parameters
        ----------
        org_unit : OrgUnitRow | pd.Series
            The organizational unit data.
            Expects columns with names :
                ['id', 'name', 'shortName', 'openingDate', 'closedDate', 'parent','level', 'path', 'geometry']
        """
        if isinstance(org_unit, pd.Series):
            # Convert Series to OrgUnitRow
            org_unit = OrgUnitRow(
                id=org_unit["id"],
                name=org_unit["name"],
                shortName=org_unit["shortName"],
                openingDate=org_unit["openingDate"],
                closedDate=org_unit["closedDate"],
                parent=org_unit["parent"],
                level=org_unit["level"],
                path=org_unit["path"],
                geometry=org_unit["geometry"],
            )
        elif isinstance(org_unit, tuple) and hasattr(org_unit, "_fields"):
            org_unit = OrgUnitRow(**org_unit._asdict())
        elif not isinstance(org_unit, OrgUnitRow):
            raise TypeError(f"Expected OrgUnitRow, pd.Series, or tuple, got {type(org_unit)}")

        self.initialize_from(org_unit_tuple=org_unit)

    def initialize_from(self, org_unit_tuple: OrgUnitRow):
        """Initialize the OrgUnitObj instance from an OrgUnitRow tuple.

        This object should represent a DHIS2 organizational unit with the same attribute naming.

        Parameters
        ----------
        org_unit_tuple : tuple
            A tuple containing organizational unit attributes.
        """
        # Keep names consistent
        self.id = org_unit_tuple.id
        self.name = org_unit_tuple.name
        self.shortName = org_unit_tuple.shortName
        self.openingDate = org_unit_tuple.openingDate
        self.closedDate = org_unit_tuple.closedDate
        self.parent = org_unit_tuple.parent
        geometry = org_unit_tuple.geometry
        self.geometry = json.loads(geometry) if isinstance(geometry, str) else geometry

    def to_json(self) -> dict:
        """Return a dictionary representation of the organizational unit suitable for DHIS2 API.

        Returns
        -------
        dict
            Dictionary containing the organizational unit's attributes formatted for DHIS2.
        """
        json_dict = {
            "id": self.id,
            "name": self.name,
            "shortName": self.shortName,
            "openingDate": self.openingDate,
            "closedDate": self.closedDate,
            "parent": {"id": self.parent.get("id")} if self.parent else None,
        }
        if self.geometry:
            json_dict["geometry"] = {
                "type": self.geometry["type"],
                "coordinates": self.geometry["coordinates"],
            }
        return {k: v for k, v in json_dict.items() if v is not None}

    def is_valid(self) -> bool:
        """Check if the OrgUnitObj instance has all required attributes set.

        Returns
        -------
        bool
            True if all required attributes are not None, False otherwise.
        """
        if self.id is None:
            return False
        if self.name is None:
            return False
        if self.shortName is None:
            return False
        if self.openingDate is None:
            return False
        return self.parent is not None  # otherwise you are a country

    def __str__(self) -> str:
        return f"OrgUnitObj({self.id}, {self.name})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, OrgUnitObj):
            return NotImplemented
        return (
            self.id == other.id
            and self.name == other.name
            and self.shortName == other.shortName
            and self.openingDate == other.openingDate
            and self.closedDate == other.closedDate
            and self.parent == other.parent
            and self.geometry == other.geometry
        )
