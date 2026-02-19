"""Built-in data connectors.

Importing this package registers all shipped connectors with the registry.
"""

from __future__ import annotations

from dp.connectors.postgres import PostgresConnector
from dp.connectors.mysql import MySQLConnector
from dp.connectors.rest_api import RESTAPIConnector
from dp.connectors.google_sheets import GoogleSheetsConnector
from dp.connectors.csv_file import CSVConnector
from dp.connectors.s3_gcs import S3GCSConnector
from dp.connectors.stripe import StripeConnector
from dp.connectors.hubspot import HubSpotConnector
from dp.connectors.shopify import ShopifyConnector
from dp.connectors.webhook import WebhookConnector

__all__ = [
    "PostgresConnector",
    "MySQLConnector",
    "RESTAPIConnector",
    "GoogleSheetsConnector",
    "CSVConnector",
    "S3GCSConnector",
    "StripeConnector",
    "HubSpotConnector",
    "ShopifyConnector",
    "WebhookConnector",
]
