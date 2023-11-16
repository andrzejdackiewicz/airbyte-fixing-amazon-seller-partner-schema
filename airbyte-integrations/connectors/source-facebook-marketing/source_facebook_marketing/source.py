#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from typing import Any, List, Mapping, Optional, Tuple, Type

import facebook_business
import pendulum
from airbyte_cdk.models import (
    AdvancedAuth,
    AuthFlowType,
    ConnectorSpecification,
    DestinationSyncMode,
    FailureType,
    OAuthConfigSpecification,
    SyncMode,
)
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.utils import AirbyteTracedException
from source_facebook_marketing.api import API
from source_facebook_marketing.spec import ConnectorConfig
from source_facebook_marketing.streams import (
    Activities,
    AdAccounts,
    AdCreatives,
    Ads,
    AdSets,
    AdsInsights,
    AdsInsightsActionCarouselCard,
    AdsInsightsActionConversionDevice,
    AdsInsightsActionProductID,
    AdsInsightsActionReaction,
    AdsInsightsActionType,
    AdsInsightsActionVideoSound,
    AdsInsightsActionVideoType,
    AdsInsightsAgeAndGender,
    AdsInsightsCountry,
    AdsInsightsDeliveryDevice,
    AdsInsightsDeliveryPlatform,
    AdsInsightsDeliveryPlatformAndDevicePlatform,
    AdsInsightsDemographicsAge,
    AdsInsightsDemographicsCountry,
    AdsInsightsDemographicsDMARegion,
    AdsInsightsDemographicsGender,
    AdsInsightsDma,
    AdsInsightsPlatformAndDevice,
    AdsInsightsRegion,
    AdRuleLibraries,
    Campaigns,
    CustomAudiences,
    CustomConversions,
    Images,
    Videos,
)

from .utils import validate_end_date, validate_start_date

logger = logging.getLogger("airbyte")
UNSUPPORTED_FIELDS = {"unique_conversions", "unique_ctr", "unique_clicks"}


class SourceFacebookMarketing(AbstractSource):

    # Skip exceptions on missing streams
    raise_exception_on_missing_stream = False

    def _validate_and_transform(self, config: Mapping[str, Any]):
        config.setdefault("action_breakdowns_allow_empty", False)
        if config.get("end_date") == "":
            config.pop("end_date")

        config = ConnectorConfig.parse_obj(config)

        if config.start_date:
            config.start_date = pendulum.instance(config.start_date)

        if config.end_date:
            config.end_date = pendulum.instance(config.end_date)
        return config

    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Optional[Any]]:
        """Connection check to validate that the user-provided config can be used to connect to the underlying API

        :param logger: source logger
        :param config:  the user-input config object conforming to the connector's spec.json
        :return Tuple[bool, Any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        try:
            config = self._validate_and_transform(config)

            if config.end_date > pendulum.now():
                return False, "Date range can not be in the future."
            if config.start_date and config.end_date < config.start_date:
                return False, "End date must be equal or after start date."

            account_id_list = config.account_ids.split(',') if config.account_ids else []
            api = API(account_ids=account_id_list, access_token=config.access_token, page_size=config.page_size)

            record_iterator = AdAccounts(api=api).read_records(sync_mode=SyncMode.full_refresh, stream_state={})
            for account_info in list(record_iterator):
                if account_info.get("is_personal"):
                    message = (
                        "The personal ad account you're currently using is not eligible "
                        "for this operation. Please switch to a business ad account."
                    )
                    return False, message
        except AirbyteTracedException as e:
            return False, f"{e.message}. Full error: {e.internal_message}"

        except Exception as e:
            return False, f"Unexpected error: {repr(e)}"

        # make sure that we have valid combination of "action_breakdowns" and "breakdowns" parameters
        for stream in self.get_custom_insights_streams(api, config):
            try:
                stream.check_breakdowns()
            except facebook_business.exceptions.FacebookRequestError as e:
                return False, e._api_error_message
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Type[Stream]]:
        """Discovery method, returns available streams

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        :return: list of the stream instances
        """
        config = self._validate_and_transform(config)
        if config.start_date:
            config.start_date = validate_start_date(config.start_date)
            config.end_date = validate_end_date(config.start_date, config.end_date)

        account_id_list = config.account_ids.split(',') if config.account_ids else []
        api = API(account_ids=account_id_list, access_token=config.access_token, page_size=config.page_size)

        # if start_date not specified then set default start_date for report streams to 2 years ago
        report_start_date = config.start_date or pendulum.now().add(years=-2)

        insights_args = dict(
            api=api, start_date=report_start_date, end_date=config.end_date, insights_lookback_window=config.insights_lookback_window
        )
        streams = [
            AdAccounts(api=api),
            AdSets(
                api=api,
                start_date=config.start_date,
                end_date=config.end_date,
                include_deleted=config.include_deleted,
                page_size=config.page_size,
            ),
            Ads(
                api=api,
                start_date=config.start_date,
                end_date=config.end_date,
                include_deleted=config.include_deleted,
                page_size=config.page_size,
            ),
            AdCreatives(
                api=api,
                fetch_thumbnail_images=config.fetch_thumbnail_images,
                page_size=config.page_size,
            ),
            AdsInsights(page_size=config.page_size, **insights_args),
            AdsInsightsAgeAndGender(page_size=config.page_size, **insights_args),
            AdsInsightsCountry(page_size=config.page_size, **insights_args),
            AdsInsightsRegion(page_size=config.page_size, **insights_args),
            AdsInsightsDma(page_size=config.page_size, **insights_args),
            AdsInsightsPlatformAndDevice(page_size=config.page_size, **insights_args),
            AdsInsightsActionType(page_size=config.page_size, **insights_args),
            AdsInsightsActionCarouselCard(page_size=config.page_size, **insights_args),
            AdsInsightsActionConversionDevice(page_size=config.page_size, **insights_args),
            AdsInsightsActionProductID(page_size=config.page_size, **insights_args),
            AdsInsightsActionReaction(page_size=config.page_size, **insights_args),
            AdsInsightsActionVideoSound(page_size=config.page_size, **insights_args),
            AdsInsightsActionVideoType(page_size=config.page_size, **insights_args),
            AdsInsightsDeliveryDevice(page_size=config.page_size, **insights_args),
            AdsInsightsDeliveryPlatform(page_size=config.page_size, **insights_args),
            AdsInsightsDeliveryPlatformAndDevicePlatform(page_size=config.page_size, **insights_args),
            AdsInsightsDemographicsAge(page_size=config.page_size, **insights_args),
            AdsInsightsDemographicsCountry(page_size=config.page_size, **insights_args),
            AdsInsightsDemographicsDMARegion(page_size=config.page_size, **insights_args),
            AdsInsightsDemographicsGender(page_size=config.page_size, **insights_args),
            AdRuleLibraries(
                api=api,
                start_date=config.start_date,
                end_date=config.end_date, page_size=config.page_size),
            Campaigns(
                api=api,
                start_date=config.start_date,
                end_date=config.end_date,
                include_deleted=config.include_deleted,
                page_size=config.page_size,
            ),
            CustomConversions(
                api=api,
                include_deleted=config.include_deleted,
                page_size=config.page_size,
            ),
            CustomAudiences(
                api=api,
                include_deleted=config.include_deleted,
                page_size=config.page_size,
            ),
            Images(
                api=api,
                start_date=config.start_date,
                end_date=config.end_date,
                include_deleted=config.include_deleted,
                page_size=config.page_size,
            ),
            Videos(
                api=api,
                start_date=config.start_date,
                end_date=config.end_date,
                include_deleted=config.include_deleted,
                page_size=config.page_size,
            ),
            Activities(
                api=api,
                start_date=config.start_date,
                end_date=config.end_date,
                include_deleted=config.include_deleted,
                page_size=config.page_size,
            ),
        ]

        return streams + self.get_custom_insights_streams(api, config)

    def spec(self, *args, **kwargs) -> ConnectorSpecification:
        """Returns the spec for this integration.
        The spec is a JSON-Schema object describing the required configurations
        (e.g: username and password) required to run this integration.
        """
        return ConnectorSpecification(
            documentationUrl="https://docs.airbyte.com/integrations/sources/facebook-marketing",
            changelogUrl="https://docs.airbyte.com/integrations/sources/facebook-marketing",
            supportsIncremental=True,
            supported_destination_sync_modes=[DestinationSyncMode.append],
            connectionSpecification=ConnectorConfig.schema(),
            advanced_auth=AdvancedAuth(
                auth_flow_type=AuthFlowType.oauth2_0,
                oauth_config_specification=OAuthConfigSpecification(
                    complete_oauth_output_specification={
                        "type": "object",
                        "properties": {
                            "access_token": {
                                "type": "string",
                                "path_in_connector_config": ["access_token"],
                            }
                        },
                    },
                    complete_oauth_server_input_specification={
                        "type": "object",
                        "properties": {"client_id": {"type": "string"}, "client_secret": {"type": "string"}},
                    },
                    complete_oauth_server_output_specification={
                        "type": "object",
                        "additionalProperties": True,
                        "properties": {
                            "client_id": {"type": "string", "path_in_connector_config": ["client_id"]},
                            "client_secret": {"type": "string", "path_in_connector_config": ["client_secret"]},
                        },
                    },
                ),
            ),
        )

    def get_custom_insights_streams(self, api: API, config: ConnectorConfig) -> List[Type[Stream]]:
        """return custom insights streams"""
        streams = []
        for insight in config.custom_insights or []:
            insight_fields = set(insight.fields)
            if insight_fields.intersection(UNSUPPORTED_FIELDS):
                # https://github.com/airbytehq/oncall/issues/1137
                message = (
                    f"The custom fields `{insight_fields.intersection(UNSUPPORTED_FIELDS)}` are not a valid configuration for"
                    f" `{insight.name}'. Review Facebook Marketing's docs https://developers.facebook.com/docs/marketing-api/reference/ads-action-stats/ for valid breakdowns."
                )
                raise AirbyteTracedException(
                    message=message,
                    failure_type=FailureType.config_error,
                )
            stream = AdsInsights(
                api=api,
                name=f"Custom{insight.name}",
                fields=list(insight_fields),
                breakdowns=list(set(insight.breakdowns)),
                action_breakdowns=list(set(insight.action_breakdowns)),
                action_breakdowns_allow_empty=config.action_breakdowns_allow_empty,
                action_report_time=insight.action_report_time,
                time_increment=insight.time_increment,
                start_date=insight.start_date or config.start_date or pendulum.now().add(years=-2),
                end_date=insight.end_date or config.end_date,
                insights_lookback_window=insight.insights_lookback_window or config.insights_lookback_window,
                level=insight.level,
            )
            streams.append(stream)
        return streams
