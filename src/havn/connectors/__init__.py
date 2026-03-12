"""Built-in data connectors.

Importing this package registers all shipped connectors with the registry.
"""

from __future__ import annotations

from havn.connectors.postgres import PostgresConnector
from havn.connectors.mysql import MySQLConnector
from havn.connectors.rest_api import RESTAPIConnector
from havn.connectors.google_sheets import GoogleSheetsConnector
from havn.connectors.csv_file import CSVConnector
from havn.connectors.s3_gcs import S3GCSConnector
from havn.connectors.stripe import StripeConnector
from havn.connectors.hubspot import HubSpotConnector
from havn.connectors.shopify import ShopifyConnector
from havn.connectors.webhook import WebhookConnector

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
