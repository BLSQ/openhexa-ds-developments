import json
import logging

import pandas as pd
import requests
from openhexa.sdk import current_run
from openhexa.toolbox.dhis2 import DHIS2

from .data_models import DataPoint


class DHIS2Pusher:
    """Main class to handle pushing data to DHIS2."""

    def __init__(
        self,
        dhis2_client: DHIS2,
        import_strategy: str = "CREATE_AND_UPDATE",
        dry_run: bool = True,
        max_post: int = 500,
        logger: logging.Logger | None = None,
    ):
        self.dhis2_client = dhis2_client
        if import_strategy not in {"CREATE", "UPDATE", "CREATE_AND_UPDATE"}:
            raise ValueError("Invalid import strategy (use 'CREATE', 'UPDATE' or 'CREATE_AND_UPDATE')")
        self.import_strategy = import_strategy
        self.dry_run = dry_run
        self.max_post = max_post
        self.summary = {}
        self._reset_summary()
        self.logger = logger if logger else logging.getLogger(__name__)

    def push_data(
        self,
        df_data: pd.DataFrame,
    ) -> None:
        """Push formatted data to DHIS2."""
        valid, to_delete, to_ignore = self._classify_data_points(df_data)

        self._push_valid(valid)
        self._push_removals(to_delete)
        self._log_ignored_or_na(to_ignore)

    def _classify_data_points(self, data_values: pd.DataFrame) -> tuple[list, list, list]:
        if data_values.empty:
            current_run.log_warning("No data to push.")
            return [], [], []

        valid, to_delete, not_valid = [], [], []

        def classify_row(row: pd.Series):
            dpoint = DataPoint(row)
            if dpoint.is_valid():
                valid.append(dpoint.to_json())
            elif dpoint.is_to_delete():
                to_delete.append(dpoint.to_delete_json())
            else:
                not_valid.append(row)

        data_values.apply(classify_row, axis=1)
        return valid, to_delete, not_valid

    def _push_valid(self, data_points_valid: list) -> None:
        """Push valid values to DHIS2."""
        if len(data_points_valid) == 0:
            current_run.log_info("No data to push.")
            return

        msg = f"Pushing {len(data_points_valid)} data points."
        current_run.log_info(msg)
        self.logger.info(msg)

        self._push_data_points(data_point_list=data_points_valid, logging_interval=50000)
        msg = f"Data points push summary:  {self.summary['import_counts']}"
        current_run.log_info(msg)
        self.logger.info(msg)
        self._log_summary_errors()

    def _push_removals(self, data_points_to_remove: pd.DataFrame) -> None:
        if len(data_points_to_remove) == 0:
            # current_run.log_info("No NA data points to push.")
            return

        current_run.log_info(f"Pushing {len(data_points_to_remove)} data points with NA values.")
        self.logger.info(f"Pushing {len(data_points_to_remove)} data points with NA values.")
        self._log_ignored_or_na(data_points_to_remove, is_na=True)
        self._push_data_points(data_point_list=data_points_to_remove, logging_interval=50000)

        current_run.log_info(f"Data points delete summary: {self.summary['import_counts']}")
        self.logger.info(f"Data points delete summary: {self.summary['import_counts']}")
        self._log_summary_errors()

    def _log_ignored_or_na(self, data_point_list: list, is_na: bool = False):
        """Logs ignored or NA data points."""
        if len(data_point_list) > 0:
            current_run.log_info(
                f"{len(data_point_list)} data points will be  {'updated to NA' if is_na else 'ignored'}. "
                "Please check the last execution report for details."
            )
            self.logger.warning(f"{len(data_point_list)} data points to be {'updated to NA' if is_na else 'ignored'}: ")
            for i, ignored in enumerate(data_point_list, start=1):
                row_str = ", ".join(f"{k}={v}" for k, v in ignored.items())
                self.logger.warning(f"{i}. Data point {'NA' if is_na else 'ignored'}: {row_str}")

    def _log_summary_errors(self):
        """Logs all the errors in the summary dictionary using the configured logging."""
        errors = self.summary.get("ERRORS", [])
        if not errors:
            self.logger.info("No errors found in the summary.")
        else:
            self.logger.error(f"Logging {len(errors)} error(s) from export summary.")
            for i_e, error in enumerate(errors, start=1):
                self.logger.error(f"Error response {i_e}: {error}")

    def _push_data_points(
        self,
        data_point_list: list[dict],
        logging_interval: int = 50000,
    ) -> None:
        """dry_run: Set to true to get an import summary without actually importing data (DHIS2)."""
        self._reset_summary()
        total_data_points = len(data_point_list)
        processed_points = 0

        for chunk in self._split_list(data_point_list, self.max_post):
            r = None
            try:
                r = self.dhis2_client.api.session.post(
                    f"{self.dhis2_client.api.url}/dataValueSets",
                    json={"dataValues": chunk},
                    params={
                        "dryRun": self.dry_run,
                        "importStrategy": self.import_strategy,
                        "preheatCache": True,
                        "skipAudit": True,
                    },  # speed!
                )

                r.raise_for_status()
                response = self._safe_json(r)

                if response:
                    self._update_import_counts(response)

                # Capture conflicts/errorReports if present
                self._extract_conflicts(response)

            except requests.exceptions.RequestException as e:
                response = self._safe_json(r)
                if response:
                    self._update_import_counts(response)
                else:
                    # No response JSON, at least log the request error msg
                    self.summary["ERRORS"].append(
                        [f"Request error for period: {chunk[0].get('period', '')} with exception: {e}"]
                    )
                self._extract_conflicts(response)

            processed_points += len(chunk)
            # Log every logging_interval points
            if processed_points // logging_interval > (processed_points - len(chunk)) // logging_interval:
                current_run.log_info(
                    f"{processed_points} / {total_data_points} data points "
                    f"pushed summary: {self.summary['import_counts']}"
                )

        # Final summary
        current_run.log_info(
            f"{processed_points} / {total_data_points} data points processed."
            f" Final summary: {self.summary['import_counts']}"
        )

    def _reset_summary(self) -> None:
        self.summary = {
            "import_counts": {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0},
            "import_options": {},
            "ERRORS": [],
        }

    def _split_list(self, src_list: list, length: int):
        """Split list into chunks.

        Yields:
            list: A chunk of the source list of the specified length.
        """
        for i in range(0, len(src_list), length):
            yield src_list[i : i + length]

    def _safe_json(self, r: requests.Response) -> dict | None:
        if r is None:
            return None

        try:
            return r.json()
        except (ValueError, json.JSONDecodeError):
            return None

    def _update_import_counts(self, response: dict) -> None:
        if not response:
            return
        if "importCount" in response:
            import_counts = response.get("importCount", {})
        elif "response" in response and "importCount" in response["response"]:
            import_counts = response["response"].get("importCount", {})
        else:
            import_counts = {}
        for key in ["imported", "updated", "ignored", "deleted"]:
            self.summary["import_counts"][key] += import_counts.get(key, 0)

    def _extract_conflicts(self, response: dict) -> None:
        """Extract all conflicts and errorReports from a DHIS2 API response.

        Handles both top-level and nested 'response' nodes. Optionally updates the summary.

        Parameters
        ----------
        response : dict
            The JSON response from DHIS2 after an import.
        """
        if not response:
            return
        conflicts = response.get("conflicts", [])
        error_reports = response.get("errorReports", [])

        # Check if nested under "response"
        nested = response.get("response", {})
        conflicts += nested.get("conflicts", [])
        error_reports += nested.get("errorReports", [])
        all_errors = conflicts + error_reports

        if all_errors:
            self.summary.setdefault("ERRORS", []).extend(all_errors)
