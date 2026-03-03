import tempfile
from pathlib import Path

import polars as pl
from openhexa.sdk import current_run
from openhexa.toolbox.dhis2 import DHIS2

from .data_models import DataType


class DataElementsExtractor:
    """Handles downloading and formatting of data elements from DHIS2."""

    def __init__(self, extractor: "DHIS2Extractor"):
        self.extractor = extractor

    def download_period(
        self,
        data_elements: list[str],
        org_units: list[str],
        period: str,
        output_dir: Path,
        filename: str | None = None,
        **kwargs,  # noqa: ANN003
    ) -> Path | None:
        """Download and handle data extracts for the specified period, saving them to the output directory.

        Parameters
        ----------
        data_elements : list[str]
            List of DHIS2 data element UIDs to extract.
        org_units : list[str]
            List of DHIS2 organization unit UIDs to extract data for.
        period : str
            DHIS2 period (valid format) to extract data for.
        output_dir : Path
            Directory where extracted data files will be saved.
        filename : str | None
            Optional filename for the extracted data file. If None, a default name will be used.
        kwargs : dict
            Additional keyword arguments for data retrieval, such as `last_updated` for filtering data.

        Returns
        -------
        Path | None
            The path to the extracted data file, or None if no data was extracted.

        Raises
        ------
        Exception
            If an error occurs during the extract process.
        """
        try:
            current_run.log_info(f"Retrieving data elements extract for period : {period}")
            return self.extractor._handle_extract_for_period(
                handler=self,
                data_products=data_elements,
                org_units=org_units,
                period=period,
                output_dir=output_dir,
                filename=filename,
                **kwargs,
            )
        except Exception as e:
            raise Exception(f"Extract data elements download error : {e}") from e

    def _retrieve_data(self, data_elements: list[str], org_units: list[str], period: str, **kwargs) -> pl.DataFrame:  # noqa: ANN003
        if not self.extractor._valid_dhis2_period_format(period):
            raise ValueError(f"Invalid DHIS2 period format: {period}")
        last_updated = kwargs.get("last_updated")
        try:
            response = self.extractor.dhis2_client.data_value_sets.get(
                data_elements=data_elements,
                periods=[period],
                org_units=org_units,
                last_updated=last_updated,  # not implemented yet
            )
        except Exception as e:
            raise Exception(f"Error retrieving data elements data: {e}") from e

        return self.extractor._map_to_dhis2_format(pl.DataFrame(response), data_type=DataType.DATA_ELEMENT)


class IndicatorsExtractor:
    """Handles downloading and formatting of indicators from DHIS2."""

    def __init__(self, extractor: "DHIS2Extractor"):
        self.extractor = extractor

    def download_period(
        self,
        indicators: list[str],
        org_units: list[str],
        period: str,
        output_dir: Path,
        filename: str | None = None,
        **kwargs,  # noqa: ANN003
    ) -> Path | None:
        """Download and handle data extracts for the specified periods, saving them to the output directory.

        Parameters
        ----------
        indicators : list[str]
            List of DHIS2 indicators UIDs to extract.
        org_units : list[str]
            List of DHIS2 organization unit UIDs to extract data for.
        period : str
            DHIS2 period (valid format) to extract data for.
        output_dir : Path
            Directory where extracted data files will be saved.
        filename : str | None
            Optional filename for the extracted data file. If None, a default name will be used.
        kwargs : dict
            Additional keyword arguments for data retrieval, such as `last_updated` for filtering data.

        Returns
        -------
        Path | None
            The path to the extracted data file, or None if no data was extracted.

        Raises
        ------
        Exception
            If an error occurs during the extract process.
        """
        try:
            current_run.log_info(f"Retrieving indicators extract for period : {period}")
            return self.extractor._handle_extract_for_period(
                handler=self,
                data_products=indicators,
                org_units=org_units,
                period=period,
                output_dir=output_dir,
                filename=filename,
                **kwargs,
            )
        except Exception as e:
            raise Exception(f"Extract indicators download error : {e}") from e

    def _retrieve_data(self, indicators: list[str], org_units: list[str], period: str, **kwargs) -> pl.DataFrame:  # noqa: ANN003
        if not self.extractor._valid_dhis2_period_format(period):
            raise ValueError(f"Invalid DHIS2 period format: {period}")

        include_cocs = kwargs.get("include_cocs", False)
        try:
            response = self.extractor.dhis2_client.analytics.get(
                indicators=indicators,
                periods=[period],
                org_units=org_units,
                include_cocs=False,  # avoid client error
            )
        except Exception as e:
            raise Exception(f"Error retrieving indicators data: {e}") from e

        raw_data_formatted = pl.DataFrame(response).rename({"pe": "period", "ou": "orgUnit"})
        return self.extractor._map_to_dhis2_format(
            raw_data_formatted, data_type=DataType.INDICATOR, map_cocs=include_cocs
        )


class ReportingRatesExtractor:
    """Handles downloading and formatting of reporting rates from DHIS2."""

    def __init__(self, extractor: "DHIS2Extractor"):
        self.extractor = extractor

    def download_period(
        self,
        reporting_rates: list[str],
        org_units: list[str],
        period: str,
        output_dir: Path,
        filename: str | None = None,
        **kwargs,  # noqa: ANN003
    ) -> Path | None:
        """Download and handle data extracts for the specified periods, saving them to the output directory.

        Parameters
        ----------
        reporting_rates : list[str]
            List of DHIS2 reporting rates UIDs.RATE to extract.
        org_units : list[str]
            List of DHIS2 organization unit UIDs to extract data for.
        period : str
            DHIS2 period (valid format) to extract data for.
        output_dir : Path
            Directory where extracted data files will be saved.
        filename : str | None
            Optional filename for the extracted data file. If None, a default name will be used.
        kwargs : dict
            Additional keyword arguments for data retrieval, such as `last_updated` for filtering data.

        Returns
        -------
        Path | None
            The path to the extracted data file, or None if no data was extracted.

        Raises
        ------
        Exception
            If an error occurs during the extract process.
        """
        try:
            current_run.log_info(f"Retrieving reporting rates extract for period : {period}")
            return self.extractor._handle_extract_for_period(
                handler=self,
                data_products=reporting_rates,
                org_units=org_units,
                period=period,
                output_dir=output_dir,
                filename=filename,
                **kwargs,
            )
        except Exception as e:
            raise Exception(f"Extract reporting rates download error : {e}") from e

    def _retrieve_data(self, reporting_rates: list[str], org_units: list[str], period: str, **kwargs) -> pl.DataFrame:  # noqa: ANN003
        if not self.extractor._valid_dhis2_period_format(period):
            raise ValueError(f"Invalid DHIS2 period format: {period}")

        try:
            response = self.extractor.dhis2_client.analytics.get(
                data_elements=reporting_rates,
                periods=[period],
                org_units=org_units,
                include_cocs=False,  # avoid client error
            )
        except Exception as e:
            raise Exception(f"Error retrieving reporting rates data: {e}") from e

        raw_data_formatted = pl.DataFrame(response).rename({"pe": "period", "ou": "orgUnit"})
        return self.extractor._map_to_dhis2_format(raw_data_formatted, data_type=DataType.REPORTING_RATE)


class DHIS2Extractor:
    """Extracts data from DHIS2 using various handlers for data elements, indicators, and reporting rates.

    Attributes
    ----------
    client : object
        The DHIS2 client used for data extraction.
    queue : object | None
        Optional queue for managing extracted files.
    download_mode : str
        Mode for downloading files ("DOWNLOAD_REPLACE" or "DOWNLOAD_NEW").
    last_updated : None
        Placeholder for future use.
    return_existing_file : bool
        When DOWNLOAD_NEW mode is used:
            True: returns the path to existing files.
            False: returns None if the file already exists.
        Default is False.

    Handlers
    --------
    data_elements : DataElementsExtractor
        Handler for extracting data elements.
    indicators : IndicatorsExtractor
        Handler for extracting indicators.
    reporting_rates : ReportingRatesExtractor
        Handler for extracting reporting rates.
    """

    def __init__(
        self, dhis2_client: DHIS2, download_mode: str = "DOWNLOAD_REPLACE", return_existing_file: bool = False
    ):
        self.dhis2_client = dhis2_client
        if download_mode not in {"DOWNLOAD_REPLACE", "DOWNLOAD_NEW"}:
            raise ValueError("Invalid 'download_mode', use 'DOWNLOAD_REPLACE' or 'DOWNLOAD_NEW'.")
        self.download_mode = download_mode
        self.last_updated = None  # NOTE: Placeholder for future use
        self.data_elements = DataElementsExtractor(self)
        self.indicators = IndicatorsExtractor(self)
        self.reporting_rates = ReportingRatesExtractor(self)
        self.return_existing_file = return_existing_file

    def _handle_extract_for_period(
        self,
        handler: DataElementsExtractor | IndicatorsExtractor | ReportingRatesExtractor,
        data_products: list[str],
        org_units: list[str],
        period: str,
        output_dir: Path,
        filename: str | None = None,
        **kwargs,  # noqa: ANN003
    ) -> Path | None:
        output_dir.mkdir(parents=True, exist_ok=True)
        if filename:
            extract_fname = output_dir / filename
        else:
            extract_fname = output_dir / f"data_{period}.parquet"

        # Skip if already exists and mode is DOWNLOAD_NEW
        if self.download_mode == "DOWNLOAD_NEW" and extract_fname.exists():
            current_run.log_info(f"Extract for period {period} already exists, download skipped.")
            return extract_fname if self.return_existing_file else None

        raw_data = handler._retrieve_data(data_products, org_units, period, **kwargs)

        if raw_data is None:
            current_run.log_info(f"Nothing to save for period {period}.")
            return None

        if extract_fname.exists():
            current_run.log_info(f"Replacing extract for period {period}.")

        self.save_to_parquet(raw_data, extract_fname)
        return extract_fname

    def _map_to_dhis2_format(
        self,
        dhis_data: pl.DataFrame,
        data_type: DataType = DataType.DATA_ELEMENT,
        domain_type: str = "AGGREGATED",
        map_cocs: bool = False,
    ) -> pl.DataFrame:
        """Maps DHIS2 data to a standardized data extraction table.

        Parameters
        ----------
        dhis_data : pd.DataFrame
            Input DataFrame containing DHIS2 data. Must include columns like `period`, `orgUnit`,
            `categoryOptionCombo(DATA_ELEMENT)`, `attributeOptionCombo(DATA_ELEMENT)`, `dataElement`
            and `value` based on the data type.
        data_type : str
            The type of data being mapped. Supported values are:
            - "DATA_ELEMENT": Includes `categoryOptionCombo` and maps `dataElement` to `dx_uid`.
            - "INDICATOR": Maps `dx` to `dx_uid`.
            - "REPORTING_RATE": Maps `dx` to `dx_uid` and `rate_type` by split the string by `.`.
            Default is "DATA_ELEMENT".
        domain_type : str, optional
            The domain of the data if its per period (Agg ex: monthly) or datapoint (Tracker ex: per day):
            - "AGGREGATED": For aggregated data (default).
            - "TRACKER": For tracker data.
            **NOTE: THIS IS WORK IN PROGRESS AND NOT USED YET**
        map_cocs : bool, optional
            Whether to include category option combo mapping for indicators.
            *Only applicable if `data_type` is "INDICATOR". Default is False.

        Returns
        -------
        pl.DataFrame
            A DataFrame formatted to SNIS standards, with the following columns (snake_case):
            - "data_type": The type of data (DATA_ELEMENT, REPORTING_RATE, or INDICATOR).
            - "dataElement": Data element, dataset, or indicator UID.
            - "period": Reporting period.
            - "orgUnit": Organization unit.
            - "categoryOptionCombo": (Only for DATA_ELEMENT) Category option combo UID.
            - "attributeOptionCombo": (Only for DATA_ELEMENT) Attribute option combo UID.
            - "rate_type": (Only for REPORTING_RATE) Rate type.
            - "domain_type": Data domain (AGGREGATED or TRACKER).
            - "value": Data value.
        """
        if dhis_data.height == 0:
            return None

        if data_type not in DataType:
            raise ValueError(
                "Incorrect 'data_type' configuration use: "
                "(DataType.DATA_ELEMENT, DataType.REPORTING_RATE, DataType.INDICATOR)."
            )

        try:
            n = dhis_data.height
            data = {
                "dataType": [data_type.value] * n,
                "dx": None,
                "period": dhis_data["period"] if "period" in dhis_data.columns else None,
                "orgUnit": dhis_data["orgUnit"] if "orgUnit" in dhis_data.columns else None,
                "categoryOptionCombo": None,
                "attributeOptionCombo": None,
                "rateMetric": None,
                "domainType": [domain_type] * n,
                "value": dhis_data["value"] if "value" in dhis_data.columns else None,
            }
            if data_type == DataType.DATA_ELEMENT:
                data["dx"] = dhis_data["dataElement"] if "dataElement" in dhis_data.columns else None
                data["categoryOptionCombo"] = (
                    dhis_data["categoryOptionCombo"] if "categoryOptionCombo" in dhis_data.columns else None
                )
                data["attributeOptionCombo"] = (
                    dhis_data["attributeOptionCombo"] if "attributeOptionCombo" in dhis_data.columns else None
                )
            elif data_type == DataType.REPORTING_RATE:
                if "dx" in dhis_data.columns:
                    split = dhis_data["dx"].str.split_exact(".", 1)
                    data["dx"] = split.struct.field("field_0")
                    data["rateMetric"] = split.struct.field("field_1")
            elif data_type == DataType.INDICATOR:
                data["dx"] = dhis_data["dx"] if "dx" in dhis_data.columns else None
                if map_cocs and "categoryOptionCombo" in dhis_data.columns:
                    data["categoryOptionCombo"] = dhis_data["categoryOptionCombo"]
            return pl.DataFrame(data)

        except AttributeError as e:
            raise AttributeError(f"missing extract data, required attribute for format: {e}") from e
        except Exception as e:
            raise Exception(f"Unexpected Error while creating extract format table: {e}") from e

    def _valid_dhis2_period_format(self, dhis2_period: str) -> bool:
        """Validate if the given period string is in a valid DHIS2 format.

        Returns
        -------
        bool
        True if valid, False otherwise.
        """
        # TODO: Expand this function to cover more DHIS2 period formats as needed
        return True

    @staticmethod
    def save_to_parquet(data: pl.DataFrame, filename: Path) -> None:
        """Safely saves a Polars DataFrame to a Parquet file using a temporary file and atomic replace.

        Args:
            data (pl.DataFrame): The DataFrame to save.
            filename (Path): The path where the Parquet file will be saved.
        """
        try:
            if not isinstance(data, pl.DataFrame):
                raise TypeError("The 'data' parameter must be a polars DataFrame.")

            # Write to a temporary file in the same directory
            with tempfile.NamedTemporaryFile(suffix=".parquet", dir=filename.parent, delete=False) as tmp_file:
                temp_filename = Path(tmp_file.name)
                data.write_parquet(temp_filename)

            # Atomically replace the old file with the new one
            temp_filename.replace(filename)

        except Exception as e:
            # Clean up the temp file if it exists
            if "temp_filename" in locals() and temp_filename.exists():
                temp_filename.unlink()
            raise RuntimeError(f"Failed to save parquet file to {filename}") from e
