#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from urllib.parse import unquote

import requests
import pendulum
import logging
import re

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.core import StreamData
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import Oauth2Authenticator


class SingleRefreshOauth2Authenticator(Oauth2Authenticator):
    def refresh_access_token(self) -> Tuple[str, int]:
        """
        Returns the refresh token and its lifespan in seconds

        :return: a tuple of (access_token, token_lifespan_in_seconds)
        """
        try:
            response = requests.post(
                url=self.get_token_refresh_endpoint(),
                data=self.build_refresh_request_body(),
            )
            response.raise_for_status()

            response_json = response.json()
            self._refresh_token = response_json["refresh_token"]

            return response_json[self.get_access_token_name()], response_json[self.get_expires_in_name()]
        except Exception as e:
            raise Exception(f"Error while refreshing access token: {e}") from e

    def get_auth_fields(self):
        return dict(
            access_token=self._access_token,
            refresh_token=self._refresh_token,
            token_expiry_date=self._token_expiry_date.isoformat(),
        )

    def set_auth_fields(self, token: dict):
        self._access_token = token["access_token"]
        self._refresh_token = token["refresh_token"]

        if "token_expiry_date" in token:
            self.token_expiry_date = pendulum.parser.parse(token["token_expiry_date"])


class ExactStream(HttpStream, IncrementalMixin):
    url_base = "https://start.exactonline.nl/api/v1/3361923/"
    cursor_field = "Timestamp"
    state_checkpoint_interval = 1000

    _cursor_value = None

    @property
    def auth(self) -> SingleRefreshOauth2Authenticator:
        """Helper property to return the Authenticator in the right type."""

        return self._session.auth

    @property
    def state(self) -> MutableMapping[str, Any]:
        return {
            self.cursor_field: self._cursor_value,
            "auth": self.auth.get_auth_fields(),
        }

    @state.setter
    def state(self, value: MutableMapping[str, Any]):
        if not value:
            return

        if self.cursor_field in value:
            self._cursor_value = value[self.cursor_field]
        if "auth" in value:
            self.auth.set_auth_fields(value["auth"])

    def read_records(self, *args, **kwargs) -> Iterable[StreamData]:
        for record in super().read_records(*args, **kwargs):
            # Track the largest timestamp value
            timestamp = record[self.cursor_field]
            self._cursor_value = max(timestamp, self._cursor_value) if self._cursor_value else timestamp

            yield record

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        If response contains the __next property, there are more pages. This property contains the full url to
        call next including endpoint and all query parameters.
        """

        response_json = response.json()
        next_url = response_json.get("d", {}).get("__next")

        return {"next_url": next_url} if next_url else None

    def request_headers(self, **kwargs) -> MutableMapping[str, Any]:
        """
        Default response type is XML, this is overriden to return JSON.
        """

        return {"Accept": "application/json"}

    def request_params(self, next_page_token: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        """
        The sync endpoints requires selection of fields to return. We use the configured catalog to make selection
        of fields we want to have.
        """

        # Contains the full next page, so don't append new query params
        if next_page_token:
            return {}

        configured_properties = list(self.get_json_schema()["properties"].keys())
        params = {
            "$select": ",".join(configured_properties),
        }

        if self._cursor_value:
            params["$filter"] = f"Timestamp gt {self._cursor_value}L"

        return params

    def should_retry(self, response: requests.Response) -> bool:
        # Test whether token is expired -> refresh and then retry
        if response.status_code == 401:
            error_reason = response.headers.get("WWW-Authenticate", "")
            error_reason = unquote(error_reason)

            if "message expired" in error_reason:
                self.auth.refresh_access_token()
                return True

            raise RuntimeError(f"Unexpected forbidden error: {error_reason}")

        # TODO: handle the rate limiting?
        return super().should_retry(response)

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        # Parse the results array from returned object
        response_json = response.json()
        results = response_json.get("d", {}).get("results")

        return [self._parse_timestamps(x) for x in results]

    def path(self, next_page_token: Mapping[str, Any] = None, **kwargs) -> str:
        """
        Returns the URL to call. On first call uses the property `endpoint` of subclass. For subsequent
        pages, `next_page_token` is used.
        """

        if not self.endpoint:
            raise RuntimeError("Subclass is missing endpoint")

        if next_page_token:
            return next_page_token["next_url"]

        return self.endpoint

    def _parse_timestamps(self, obj: dict):
        """
        Exact returns timestamps in following format: /Date(1672531200000)/ (OData date format).
        The value is in seconds since Epoch (UNIX time). Note, the time is in CET and not in GMT/UTC.
        https://support.exactonline.com/community/s/knowledge-base#All-All-DNO-Content-faq-rest-api
        """

        regex_timestamp = re.compile(r"^\/Date\((\d+)\)\/$")

        def parse_value(value):
            if isinstance(value, dict):
                return {k: parse_value(v) for k, v in value.items()}

            if isinstance(value, list):
                return [parse_value(v) for v in value]

            if isinstance(value, str):
                match = regex_timestamp.match(value)
                if match:
                    unix_seconds = int(match.group(1)) / 1000
                    timestamp = pendulum.from_timestamp(unix_seconds, "CET").set(tz="UTC")

                    return timestamp.isoformat()

            return value

        return {k: parse_value(v) for k, v in obj.items()}


class SyncCashflowPaymentTerms(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/Cashflow/PaymentTerms"


class SyncCRMAccounts(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/CRM/Accounts"


class SyncCRMAddresses(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/CRM/Addresses"


class SyncCRMContacts(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/CRM/Contacts"


class SyncCRMQuotationHeaders(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/CRM/QuotationHeaders"


class SyncCRMQuotationLines(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/CRM/QuotationLines"


class SyncCRMQuotations(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/CRM/Quotations"


class SyncDeleted(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/Deleted"


class SyncDocumentsDocumentAttachments(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/Documents/DocumentAttachments"


class SyncDocumentsDocuments(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/Documents/Documents"


class SyncFinancialGLAccounts(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/Financial/GLAccounts"


class SyncFinancialGLClassifications(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/Financial/GLClassifications"


class SyncFinancialTransactionLines(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/Financial/TransactionLines"


class SyncHRMLeaveAbsenceHoursByDay(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/HRM/LeaveAbsenceHoursByDay"


class SyncInventoryItemWarehouses(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/Inventory/ItemWarehouses"


class SyncInventorySerialBatchNumbers(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/Inventory/SerialBatchNumbers"


class SyncInventoryStockPositions(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/Inventory/StockPositions"


class SyncInventoryStockSerialBatchNumbers(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/Inventory/StockSerialBatchNumbers"


class SyncInventoryStorageLocationStockPositions(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/Inventory/StorageLocationStockPositions"


class SyncLogisticsItems(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/Logistics/Items"


class SyncLogisticsPurchaseItemPrices(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/Logistics/PurchaseItemPrices"


class SyncLogisticsSalesItemPrices(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/Logistics/SalesItemPrices"


class SyncLogisticsSupplierItem(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/Logistics/SupplierItem"


class SyncProjectProjectPlanning(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/Project/ProjectPlanning"


class SyncProjectProjects(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/Project/Projects"


class SyncProjectProjectWBS(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/Project/ProjectWBS"


class SyncProjectTimeCostTransactions(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/Project/TimeCostTransactions"


class SyncPurchaseOrderPurchaseOrders(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/PurchaseOrder/PurchaseOrders"


class SyncSalesSalesPriceListVolumeDiscounts(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/Sales/SalesPriceListVolumeDiscounts"


class SyncSalesInvoiceSalesInvoices(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/SalesInvoice/SalesInvoices"


class SyncSalesOrderGoodsDeliveries(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/SalesOrder/GoodsDeliveries"


class SyncSalesOrderGoodsDeliveryLines(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/SalesOrder/GoodsDeliveryLines"


class SyncSalesOrderSalesOrderHeaders(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/SalesOrder/SalesOrderHeaders"


class SyncSalesOrderSalesOrderLines(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/SalesOrder/SalesOrderLines"


class SyncSalesOrderSalesOrders(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/SalesOrder/SalesOrders"


class SyncSubscriptionSubscriptionLines(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/Subscription/SubscriptionLines"


class SyncSubscriptionSubscriptions(ExactStream):
    primary_key = "Timestamp"
    endpoint = "sync/Subscription/Subscriptions"


# Source
class SourceExact(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        access_token = config.get("access_token")
        refresh_token = config.get("refresh_token")

        if not access_token or not refresh_token:
            return False, "Missing access or refresh token"

        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        token_endpoint = "https://start.exactonline.nl/api/oauth2/token"

        auth = SingleRefreshOauth2Authenticator(
            token_refresh_endpoint=token_endpoint,
            client_id=config["client_id"],
            client_secret=config["client_secret"],
            refresh_token=config["refresh_token"],
            # We don't know when the token is expired in this context. We just set it to a future time,
            # upon 401 we will trigger refresh manually.
            token_expiry_date=pendulum.now().add(minutes=10),
        )

        auth._access_token = config["access_token"]

        return [
            SyncCashflowPaymentTerms(authenticator=auth),
            SyncCRMAccounts(authenticator=auth),
            SyncCRMAddresses(authenticator=auth),
            SyncCRMContacts(authenticator=auth),
            SyncCRMQuotationHeaders(authenticator=auth),
            SyncCRMQuotationLines(authenticator=auth),
            SyncCRMQuotations(authenticator=auth),
            SyncDeleted(authenticator=auth),
            SyncDocumentsDocumentAttachments(authenticator=auth),
            SyncDocumentsDocuments(authenticator=auth),
            SyncFinancialGLAccounts(authenticator=auth),
            SyncFinancialGLClassifications(authenticator=auth),
            SyncFinancialTransactionLines(authenticator=auth),
            SyncHRMLeaveAbsenceHoursByDay(authenticator=auth),
            SyncInventoryItemWarehouses(authenticator=auth),
            SyncInventorySerialBatchNumbers(authenticator=auth),
            SyncInventoryStockPositions(authenticator=auth),
            SyncInventoryStockSerialBatchNumbers(authenticator=auth),
            SyncInventoryStorageLocationStockPositions(authenticator=auth),
            SyncLogisticsItems(authenticator=auth),
            SyncLogisticsPurchaseItemPrices(authenticator=auth),
            SyncLogisticsSalesItemPrices(authenticator=auth),
            SyncLogisticsSupplierItem(authenticator=auth),
            SyncProjectProjectPlanning(authenticator=auth),
            SyncProjectProjects(authenticator=auth),
            SyncProjectProjectWBS(authenticator=auth),
            SyncProjectTimeCostTransactions(authenticator=auth),
            SyncPurchaseOrderPurchaseOrders(authenticator=auth),
            SyncSalesSalesPriceListVolumeDiscounts(authenticator=auth),
            SyncSalesInvoiceSalesInvoices(authenticator=auth),
            SyncSalesOrderGoodsDeliveries(authenticator=auth),
            SyncSalesOrderGoodsDeliveryLines(authenticator=auth),
            SyncSalesOrderSalesOrderHeaders(authenticator=auth),
            SyncSalesOrderSalesOrderLines(authenticator=auth),
            SyncSalesOrderSalesOrders(authenticator=auth),
            SyncSubscriptionSubscriptionLines(authenticator=auth),
            SyncSubscriptionSubscriptions(authenticator=auth),
        ]
