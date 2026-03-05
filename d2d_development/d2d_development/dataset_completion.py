import json
import logging
from pathlib import Path

import pandas as pd
import requests
from openhexa.sdk import current_run
from openhexa.toolbox.dhis2 import DHIS2


class DHIS2ImportError(RuntimeError):
    """Custom exception for DHIS2 import errors."""

    pass


class DatasetCompletionSync:
    """Main class to handle pushing data to DHIS2.

    ATTENTION: This syncer assumes the source and target DHIS2 instances
     have the same organisation units configured.
    """

    def __init__(
        self,
        source_dhis2: DHIS2,
        target_dhis2: DHIS2,
        import_strategy: str = "CREATE_AND_UPDATE",
        dry_run: bool = True,
        logger: logging.Logger | None = None,
    ):
        self.source_dhis2 = source_dhis2
        self.target_dhis2 = target_dhis2
        if import_strategy not in {"CREATE", "UPDATE", "CREATE_AND_UPDATE"}:
            raise ValueError("Invalid import strategy (use 'CREATE', 'UPDATE' or 'CREATE_AND_UPDATE')")
        self.import_strategy = import_strategy
        self.dry_run = dry_run
        self.import_summary = {
            "import_counts": {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0},
            "errors": {"fetch_errors": 0, "no_completion": 0, "push_errors": 0},
        }
        self.completion_table = pd.DataFrame()
        self.logger = logger if logger else logging.getLogger(__name__)

    def _fetch_completion_status_from_source(
        self,
        dataset_id: str,
        period: str,
        org_unit: str,
        children: bool = True,
        timeout: int = 5,
    ) -> list[dict]:
        """Fetch completion status from source DHIS2.

        Args:
            dataset_id: The dataset ID to fetch completion status for.
            period: The period for which to fetch the completion status.
            org_unit: The organisation unit to fetch completion status for.
            children: Whether to include child org units in the fetch.
            timeout: Timeout for the request in seconds.

        Returns:
            list[dict]: A list of completion status dictionaries from the DHIS2 API.
                Returns an empty list if the request fails or no data is found.
        """
        endpoint = f"{self.source_dhis2.api.url}/completeDataSetRegistrations"
        params = {
            "period": period,
            "orgUnit": org_unit,
            "children": "true" if children else "false",
            "dataSet": dataset_id,
        }

        try:
            response = self.source_dhis2.api.session.get(endpoint, params=params, timeout=timeout)
            response.raise_for_status()  # raise exception for HTTP errors
            try:
                completion = response.json().get("completeDataSetRegistrations", [])
            except ValueError as e:
                self.import_summary["errors"]["fetch_errors"] += 1
                self.logger.error(f"Invalid JSON from {endpoint} for ds:{dataset_id} pe:{period} ou:{org_unit}: {e}")
                return []
            if not completion and not children:
                self.import_summary["errors"]["no_completion"] += 1
                self.logger.info(
                    f"No completion status found at source for ds: {dataset_id} pe: {period} ou: {org_unit}"
                )
            return completion if completion else []
        except requests.RequestException as e:
            self.import_summary["errors"]["fetch_errors"] += 1
            self.logger.error(
                f"GET request to {self.source_dhis2.api.url} failed to retrieve completion status for "
                f"ds: {dataset_id} pe: {period} ou: {org_unit} failed : {e}"
            )
        return []

    def _push_completion_status_to_target(
        self,
        dataset_id: str,
        period: str,
        org_unit: str,
        date: str,
        completed: bool,
        timeout: int = 5,
    ) -> None:
        """Perform a PUT request (or POST with importStrategy) to a DHIS2 API endpoint.

        Args:
        dataset_id: The dataset ID to push completion status for.
        period: The period for which to push the completion status.
        org_unit: The organisation unit to push completion status for.
        date: The date of completion.
        completed: Whether the dataset is marked as completed.
        timeout: Timeout for the request in seconds.

        Raises:
            requests.HTTPError if the request fails after retries.
        """
        endpoint = f"{self.target_dhis2.api.url}/completeDataSetRegistrations"
        payload = {
            "completeDataSetRegistrations": [
                {
                    "organisationUnit": org_unit,
                    "period": period,
                    "completed": completed,
                    "date": date,
                    "dataSet": dataset_id,
                }
            ]
        }
        params = {
            "dryRun": str(self.dry_run).lower(),
            "importStrategy": self.import_strategy,
            "preheatCache": True,
            "skipAudit": True,
            "reportMode": "FULL",
        }

        response = None
        try:
            response = self.target_dhis2.api.session.post(endpoint, json=payload, params=params, timeout=timeout)
            response.raise_for_status()
        except requests.RequestException:
            # avoid doube counting errors in summary
            # self.import_summary["errors"]["push_errors"] += 1
            raise
        finally:
            self._process_response(ds=dataset_id, pe=period, ou=org_unit, response=response)

    def _try_build_source_completion_table(self, org_units: list[str], dataset_id: str, period: str) -> None:
        """Build a completion status table for all organisation units provided.

        Args:
            org_units: List of organisation unit IDs to fetch completion status for (NOTE: use OU parents).
            dataset_id: The dataset ID to fetch completion status for.
            period: The period for which to fetch the completion status.
        """
        if not org_units:
            return

        completion_statuses = []
        for ou in org_units:
            completion = self._fetch_completion_status_from_source(
                dataset_id=dataset_id, period=period, org_unit=ou, children=True, timeout=30
            )
            if completion:
                completion_statuses.extend(completion)

        self.completion_table = pd.DataFrame(completion_statuses)

    def _get_source_completion_status_for_ou(self, dataset_id: str, period: str, org_unit: str) -> dict | None:
        """Handle fetching completion status for a specific org unit.

        Returns:
            list: The completion status as dictionaries for the specified org unit (children) if found, otherwise [].
        """
        if not self.completion_table.empty:
            completion_status = self.completion_table[self.completion_table["organisationUnit"] == org_unit]
            if not completion_status.empty:
                return completion_status.iloc[0].to_dict()

        results = self._fetch_completion_status_from_source(
            dataset_id=dataset_id, period=period, org_unit=org_unit, children=False
        )
        for item in results or []:
            if item.get("organisationUnit") == org_unit:
                return item

        return None

    def sync(
        self,
        source_dataset_id: str,
        target_dataset_id: str,
        org_units: list[str] | None,
        parent_ou: list[str] | None,
        period: list[str],
        logging_interval: int = 2000,
        ds_processed_path: Path | None = None,
        mark_uncompleted_as_processed: bool = False,
    ) -> None:
        """Sync completion status between datasets.

        source_dataset_id: The dataset ID in the source DHIS2 instance.
        target_dataset_id: The dataset ID in the target DHIS2 instance.
        org_units: List of organisation unit IDs to sync.
        parent_ou: List of parent organisation unit IDs to build completion table (if None, no table built).
        period: The period for which to sync the completion status.
        logging_interval: Interval for logging progress (defaults to 2000).
        ds_processed_path: Path to save processed org units (if None, no file saving nor comparison).
        mark_uncompleted_as_processed: If True, org units with no completion status will be marked as processed.
        """
        self.reset_import_summary()

        if not org_units:
            msg = f"No org units provided for period {period}. DS sync skipped."
            self.logger.warning(msg)
            current_run.log_warning(msg)
            return

        org_units_to_process = self._get_unprocessed_org_units(org_units, ds_processed_path, period)
        if not org_units_to_process:
            msg = f"All org units already processed for period {period}. DS sync skipped."
            self.logger.info(msg)
            current_run.log_info(msg)
            return

        msg = (
            f"Starting dataset '{target_dataset_id}' completion process for period: "
            f"{period} org units: {len(org_units_to_process)}."
        )
        current_run.log_info(msg)
        self.logger.info(msg)

        self._try_build_source_completion_table(org_units=parent_ou, dataset_id=source_dataset_id, period=period)

        try:
            processed = []
            for idx, ou in enumerate(org_units_to_process, start=1):
                completion_status = self._get_source_completion_status_for_ou(
                    dataset_id=source_dataset_id,
                    period=period,
                    org_unit=ou,
                )

                if not completion_status:
                    if mark_uncompleted_as_processed:
                        processed.append(ou)  # if True, empty completion -> mark as processed
                    continue

                if "date" not in completion_status or "completed" not in completion_status:
                    self.import_summary["errors"]["push_errors"] += 1
                    self.logger.error(
                        f"Missing keys in completion status for period {period}, org unit {ou}: {completion_status}"
                    )
                    continue

                try:
                    self._push_completion_status_to_target(
                        dataset_id=target_dataset_id,
                        period=period,
                        org_unit=ou,
                        date=completion_status.get("date"),
                        completed=completion_status.get("completed"),
                    )
                    processed.append(ou)
                except Exception as e:
                    self.logger.error(f"Error pushing completion status for period {period}, org unit {ou}: {e}")

                if idx % logging_interval == 0 or idx == len(org_units_to_process):
                    current_run.log_info(f"{idx} / {len(org_units_to_process)} OUs processed")
                    self._update_processed_ds_sync_file(
                        processed=processed,
                        period=period,
                        processed_path=ds_processed_path,
                    )
        except Exception as e:
            self.logger.error(f"Error setting dataset completion for dataset {target_dataset_id}, period {period}: {e}")
        finally:
            self._log_summary(org_units=org_units_to_process, period=period)

    def _get_unprocessed_org_units(self, org_units: list, processed_path: Path | None, period: str) -> list:
        if processed_path is None:
            return org_units
        ds_processed_fname = processed_path / f"ds_ou_processed_{period}.parquet"
        if not ds_processed_fname.exists():
            return org_units

        try:
            processed_df = pd.read_parquet(ds_processed_fname)
            if "ORG_UNIT" not in processed_df.columns:
                raise KeyError("Missing ORG_UNIT column")

            processed_set = set(processed_df["ORG_UNIT"].dropna().unique())
            remaining = [ou for ou in org_units if ou not in processed_set]

            msg = f"Loaded {len(processed_set)} processed org units, {len(remaining)} to process for period {period}."
            self.logger.info(msg)
            current_run.log_info(msg)
            return remaining
        except Exception as e:
            msg = f"Error loading processed org units file: {ds_processed_fname}. Returning all org units to process."
            self.logger.error(msg + f" Error: {e}")
            current_run.log_info(msg)
            return org_units

    def _update_processed_ds_sync_file(
        self,
        processed: list,
        period: str,
        processed_path: Path | None,
    ) -> None:
        """Save the processed org units to a parquet file."""
        if processed_path is None:
            current_run.log_warning("No processed path provided, skipping saving processed org units.")
            return

        processed_path.mkdir(parents=True, exist_ok=True)
        ds_processed_file = processed_path / f"ds_ou_processed_{period}.parquet"

        msg = None
        final_processed = processed

        if ds_processed_file.exists():
            existing_df = pd.read_parquet(ds_processed_file)
            existing_org_units = set(existing_df["ORG_UNIT"].unique())
            new_org_units = [ou for ou in processed if ou not in existing_org_units]
            final_processed = list(existing_org_units) + new_org_units
            msg = (
                f"Found {len(existing_org_units)} processed OUs, "
                f"updating file {ds_processed_file.name} with {len(new_org_units)} new OUs."
            )

        if final_processed:
            df_processed = pd.DataFrame({"ORG_UNIT": final_processed})
            df_processed.to_parquet(ds_processed_file, index=False)
            msg = f"Saved {len(final_processed)} processed org units in {ds_processed_file.name}."

        if msg:
            current_run.log_info(msg)
            self.logger.info(msg)

    def _log_summary(self, org_units: list, period: str) -> None:
        msg = (
            f"Dataset completion period {period} summary: {self.import_summary['import_counts']} "
            f"total org units: {len(org_units)} "
        )
        current_run.log_info(msg)
        self.logger.info(msg)

        if self.import_summary["errors"]["no_completion"] > 0:
            msg = (
                f"{self.import_summary['errors']['no_completion']} out of "
                f"{len(org_units)} completion statuses failed to be retrieved from source."
            )
            current_run.log_warning(msg)
            self.logger.warning(msg)

        if self.import_summary["errors"]["fetch_errors"] > 0:
            msg = (
                f"{self.import_summary['errors']['fetch_errors']} out of "
                f"{len(org_units)} completion statuses failed to fetch."
            )
            current_run.log_warning(msg)
            self.logger.warning(msg)

        if self.import_summary["errors"]["push_errors"] > 0:
            msg = (
                f"{self.import_summary['errors']['push_errors']} "
                f"out of {len(org_units)} completion statuses failed to push."
            )
            current_run.log_warning(msg)
            self.logger.warning(msg)

    def _process_response(self, ds: str, pe: str, ou: str, response: dict) -> None:
        """Log the response from the DHIS2 API after pushing completion status."""
        json_or_none = self._safe_json(response)
        if not json_or_none:
            self.import_summary["errors"]["push_errors"] += 1
            self.logger.error(
                f"No JSON response received for completion request ds: {ds} pe: {pe} ou: {ou} from DHIS2 API."
            )
            raise DHIS2ImportError("Empty or invalid JSON response from DHIS2")

        conflicts: list[str] = json_or_none.get("conflicts", {})
        status = json_or_none.get("status")
        if status in ["ERROR", "WARNING"] or conflicts:
            for conflict in conflicts:
                self.import_summary["errors"]["push_errors"] += 1
                self.logger.error(
                    f"Conflict pushing completion for ds: {ds} pe: {pe} ou: {ou} status: {status} - {conflict}"
                )
            self._update_import_summary(response=json_or_none)
            raise DHIS2ImportError(
                f"DHIS2 completion push failed with status={status} "
                f"and {len(conflicts)} conflict(s) for ds:{ds} pe:{pe} ou:{ou}"
            )

        if status == "SUCCESS":
            self.logger.info(f"Successfully pushed to target completion ds: {ds} pe:{pe} ou: {ou}")
            self._update_import_summary(response=json_or_none)

    def _safe_json(self, r: requests.Response) -> dict | None:
        if r is None:
            return None
        try:
            return r.json()
        except (ValueError, json.JSONDecodeError):
            return None

    def _update_import_summary(self, response: dict) -> None:
        if response:
            import_counts = response.get("importCount", {})
            for key in ["imported", "updated", "ignored", "deleted"]:
                self.import_summary["import_counts"][key] += import_counts.get(key, 0)

    def reset_import_summary(self) -> None:
        """Reset the import summary to its initial state."""
        self.import_summary = {
            "import_counts": {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0},
            "errors": {"fetch_errors": 0, "no_completion": 0, "push_errors": 0},
        }
