# d2d_development

DHIS2-to-DHIS2 development utility library maintained by Bluesquare Data Services team.

## Installation

Install this library on its own:

```bash
pip install git+https://github.com/BLSQ/openhexa-ds-developments.git#subdirectory=d2d_development
```

## Main Classes

### DHIS2Extractor

**Description:**  
Main class to extract data from DHIS2. It provides unified handlers for extracting data elements, indicators, and reporting rates, saving them to disk in a standardized format.


**Configuration Parameters:**
When initializing `DHIS2Extractor`, you can configure the following parameters:

- `dhis2_client` (required): The DHIS2 client instance.
- `download_mode`: Controls how files are saved when extracting data. Use `"DOWNLOAD_REPLACE"` (default) to always overwrite files, or `"DOWNLOAD_NEW"` to skip downloading if the file already exists.
- `return_existing_file`: If `True` and using `DOWNLOAD_NEW`, returns the path to the existing file instead of `None` when a file already exists (default: `False`).
- `logger`: Optional custom logger instance.

Example:
```python
extractor = DHIS2Extractor(dhis2_client, download_mode="DOWNLOAD_NEW", return_existing_file=True)
```

**Usage Example:**
```python
from d2d_development.extract import DHIS2Extractor
from openhexa.sdk import workspace
from openhexa.toolbox.dhis2 import DHIS2
from pathlib import Path

dhis2_client = DHIS2(workspace.get_connection("dhis2-connection"))
extractor = DHIS2Extractor(dhis2_client, download_mode="DOWNLOAD_REPLACE")

# Extract several periods of data elements
for period in ["202401", "202402", "202403"]:
    extractor.data_elements.download_period(
        data_elements=["de1", "de2"],
        org_units=["ou1", "ou2"],
        period=period,
        output_dir=Path("/output")
    )
# Extract one period of indicators
extractor.indicators.download_period(
	indicators=["ind1"],
	org_units=["ou1"],
	period="202401",
	output_dir=Path("/tmp")
)
# Extract one period of reporting rates
extractor.reporting_rates.download_period(
	reporting_rates=["rr1"],
	org_units=["ou1"],
	period="202401",
	output_dir=Path("/tmp")
)
```

### DHIS2Pusher

**Description:**  
Main class to handle pushing data to DHIS2. It validates and pushes formatted data (pandas or polars DataFrame) to a DHIS2 instance.

**Configuration Parameters:**
When initializing `DHIS2Pusher`, you can configure the following parameters:

- `dhis2_client` (required): The DHIS2 client instance.
- `import_strategy`: Strategy flag passed to the DHIS2 API for data import. Accepts "CREATE", "UPDATE", or "CREATE_AND_UPDATE" (default: "CREATE_AND_UPDATE"). This only controls how the DHIS2 server processes the data; it does not affect client-side logic.
- `dry_run`: If `True`, simulates the push without making changes on the server (default: `True`).
- `max_post`: Maximum number of data points per POST request (default: `500`).
- `logging_interval`: Log progress every N data points (default: `50000`).
- `logger`: Optional custom logger instance.

**Usage Example:**
```python
from d2d_development.push import DHIS2Pusher
from openhexa.sdk import workspace
from openhexa.toolbox.dhis2 import DHIS2
import polars as pl

dhis2_client = DHIS2(workspace.get_connection("dhis2-connection"))
pusher = DHIS2Pusher(
	dhis2_client,
	import_strategy="CREATE_AND_UPDATE",  # or "CREATE", "UPDATE"
	dry_run=False,
	max_post=1000,
	logging_interval=10000,
)

df = pl.DataFrame({
    "dx": ["de1"], 
    "period": ["202401"], 
    "orgUnit": ["ou1"], 
    "categoryOptionCombo": ["coc"], 
    "attributeOptionCombo": ["aoc"], 
    "value": [123]})
pusher.push_data(df)
```
