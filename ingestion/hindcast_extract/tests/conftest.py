"""config.py reads OWM_API_KEY/AZURE_STORAGE_CONNECTION_STRING from
os.environ at *import* time (module-level, not lazily) -- anything that
imports client.py/envelope.py transitively imports config.py first, so
these have to be set before pytest even collects the test modules, not
inside a fixture. Dummy values, never real credentials -- these tests never
make a real HTTP or Azure call (respx mocks httpx; nothing here calls
land_bronze against a real blob client).
"""

import os

os.environ.setdefault("OWM_API_KEY", "test-key-not-real")
os.environ.setdefault(
    "AZURE_STORAGE_CONNECTION_STRING",
    "DefaultEndpointsProtocol=https;AccountName=test;"
    "AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
)
