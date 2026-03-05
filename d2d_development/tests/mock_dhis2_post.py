class MockDHIS2Response:
    """Mock class to simulate a response from the DHIS2 API for testing purposes."""

    def __init__(self, json_data, status_code=200):  # noqa: ANN001
        self._json_data = json_data
        self.status_code = status_code

    def json(self) -> dict:  # noqa: D102
        return self._json_data

    def raise_for_status(self):  # noqa: D102
        if not (200 <= self.status_code < 300):
            raise Exception(f"HTTP {self.status_code}")


# Example OK import response for tests
MOCK_DHIS2_OK_RESPONSE = {"importCount": {"imported": 1, "updated": 0, "ignored": 0, "deleted": 0}, "status": "OK"}
