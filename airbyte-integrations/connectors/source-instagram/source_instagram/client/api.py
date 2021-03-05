"""
MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import re
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, List, Sequence

import backoff
import pendulum
from base_python.entrypoint import logger
from facebook_business.adobjects.igmedia import IGMedia
from facebook_business.adobjects.iguser import IGUser
from facebook_business.exceptions import FacebookRequestError

from .common import retry_pattern

backoff_policy = retry_pattern(backoff.expo, FacebookRequestError, max_tries=5, factor=5)


# This function removes the _nc_rid parameter from the video url and ccb from profile_picture_url for users.
# _nc_rid is generated every time a new one and ccb can change its value, and tests fail when checking for identity.
# This does not spoil the link, it remains correct and by clicking on it you can view the video or see picture.
def clear_video_url(record_data: dict = None):
    if record_data.get("media_type") == "VIDEO":
        end_of_string = record_data["media_url"].find("&_nc_rid=")
        if end_of_string != -1:
            record_data["media_url"] = record_data["media_url"][:end_of_string]
    elif record_data.get("profile_picture_url"):
        record_data["profile_picture_url"] = "".join(re.split("ccb=.{1,5}&", record_data["profile_picture_url"]))
    return record_data


class StreamAPI(ABC):
    result_return_limit = 100
    non_object_fields = ["page_id", "business_account_id"]

    def __init__(self, api, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._api = api

    @abstractmethod
    def list(self, fields: Sequence[str] = None) -> Iterator[dict]:
        """Iterate over entities"""

    def filter_input_fields(self, fields: Sequence[str] = None):
        return list(set(fields) - set(self.non_object_fields))


class IncrementalStreamAPI(StreamAPI, ABC):
    @property
    @abstractmethod
    def state_pk(self):
        """Name of the field associated with the state"""

    @property
    def state(self):
        return {self.state_pk: str(self._state)}

    @state.setter
    def state(self, value):
        self._state = pendulum.parse(value[self.state_pk])

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._state = None

    def state_filter(self, records: Iterator[dict]) -> Iterator[Any]:
        """Apply state filter to set of records, update cursor(state) if necessary in the end"""
        latest_cursor = None
        for record in records:
            cursor = pendulum.parse(record[self.state_pk])
            if self._state and self._state >= cursor:
                continue
            latest_cursor = max(cursor, latest_cursor) if latest_cursor else cursor
            yield record

        if latest_cursor:
            stream_name = self.__class__.__name__
            if stream_name.endswith("API"):
                stream_name = stream_name[:-3]
            logger.info(f"Advancing bookmark for {stream_name} stream from {self._state} to {latest_cursor}")
            self._state = max(latest_cursor, self._state) if self._state else latest_cursor


class UsersAPI(StreamAPI):
    def list(self, fields: Sequence[str] = None) -> Iterator[dict]:
        for account in self._api.accounts:
            yield {
                **{"page_id": account["page_id"]},
                **clear_video_url(self._extend_record(account["instagram_business_account"], fields=self.filter_input_fields(fields))),
            }

    @backoff_policy
    def _extend_record(self, ig_user: IGUser, fields: Sequence[str] = None) -> Dict:
        return ig_user.api_get(fields=fields).export_all_data()


class UserLifetimeInsightsAPI(StreamAPI):
    LIFETIME_METRICS = ["audience_city", "audience_country", "audience_gender_age", "audience_locale"]
    period = "lifetime"

    def list(self, fields: Sequence[str] = None) -> Iterator[dict]:
        for account in self._api.accounts:
            for insight in self._get_insight_records(account["instagram_business_account"], params=self._params()):
                yield {
                    "page_id": account["page_id"],
                    "business_account_id": account["instagram_business_account"].get("id"),
                    "metric": insight.get("name"),
                    "date": insight.get("values")[0]["end_time"],
                    "value": insight.get("values")[0]["value"],
                }

    def _params(self) -> Dict:
        return {"metric": self.LIFETIME_METRICS, "period": self.period}

    @backoff_policy
    def _get_insight_records(self, instagram_user: IGUser, params: dict = None) -> Iterator[Any]:
        return instagram_user.get_insights(params=params)


class UserInsightsAPI(IncrementalStreamAPI):
    METRICS_BY_PERIOD = {
        "day": [
            "email_contacts",
            "follower_count",
            "get_directions_clicks",
            "impressions",
            "phone_call_clicks",
            "profile_views",
            "reach",
            "text_message_clicks",
            "website_clicks",
        ],
        "week": ["impressions", "reach"],
        "days_28": ["impressions", "reach"],
        "lifetime": ["online_followers"],
    }

    state_pk = "date"

    # We can only get User Insights data for today and the previous 29 days.
    # This is Facebook policy
    buffer_days = 29
    days_increment = 1

    def __init__(self, api):
        super().__init__(api=api)
        self._state = max(api._start_date, pendulum.now().subtract(days=self.buffer_days)).add(minutes=1)

    def list(self, fields: Sequence[str] = None) -> Iterator[dict]:
        for params_per_day in self._params():
            insights_per_day = []
            for account in self._api.accounts:
                insight_list = []
                for params in params_per_day:
                    insight_list += self._get_insight_records(account["instagram_business_account"], params=params)
                if not insight_list:
                    continue

                insight_record = {"page_id": account["page_id"], "business_account_id": account["instagram_business_account"].get("id")}
                for insight in insight_list:
                    key = (
                        f"{insight.get('name')}_{insight.get('period')}"
                        if insight.get("period") in ["week", "days_28"]
                        else insight.get("name")
                    )
                    insight_record[key] = insight.get("values")[0]["value"]
                    if not insight_record.get("date"):
                        insight_record["date"] = insight.get("values")[0]["end_time"]
                insights_per_day.append(insight_record)
            yield from self.state_filter(insights_per_day)

    def _params(self) -> Iterator[List]:
        buffered_start_date = self._state
        end_date = pendulum.now()

        while buffered_start_date <= end_date:
            params_list = []
            for period, metrics in self.METRICS_BY_PERIOD.items():
                params_list.append(
                    {
                        "metric": metrics,
                        "period": [period],
                        "since": buffered_start_date.to_datetime_string(),
                        "until": buffered_start_date.add(days=self.days_increment).to_datetime_string(),
                    }
                )
            yield params_list
            buffered_start_date = buffered_start_date.add(days=self.days_increment)

    @backoff_policy
    def _get_insight_records(self, instagram_user: IGUser, params: dict = None) -> List:
        return instagram_user.get_insights(params=params)._queue


class MediaAPI(StreamAPI):
    INVALID_CHILDREN_FIELDS = ["caption", "comments_count", "is_comment_enabled", "like_count", "children"]

    def list(self, fields: Sequence[str] = None) -> Iterator[dict]:
        children_fields = self.filter_input_fields(list(set(fields) - set(self.INVALID_CHILDREN_FIELDS)))
        for account in self._api.accounts:
            media = self._get_media(
                account["instagram_business_account"], {"limit": self.result_return_limit}, self.filter_input_fields(fields)
            )
            for record in media:
                record_data = record.export_all_data()
                if record_data.get("children"):
                    record_data["children"] = [
                        clear_video_url(self._get_single_record(child_record["id"], children_fields).export_all_data())
                        for child_record in record.get("children")["data"]
                    ]
                record_data.update(
                    {
                        "page_id": account["page_id"],
                        "business_account_id": account["instagram_business_account"].get("id"),
                    }
                )
                yield clear_video_url(record_data)

    @backoff_policy
    def _get_media(self, instagram_user: IGUser, params: dict = None, fields: Sequence[str] = None) -> Iterator[Any]:
        """
        This is necessary because the functions that call this endpoint return
        a generator, whose calls need decorated with a backoff.
        """
        return instagram_user.get_media(params=params, fields=fields)

    @backoff_policy
    def _get_single_record(self, media_id: str, fields: Sequence[str] = None) -> IGMedia:
        return IGMedia(media_id).api_get(fields=fields)


class StoriesAPI(StreamAPI):
    def list(self, fields: Sequence[str] = None) -> Iterator[dict]:
        for account in self._api.accounts:
            stories = self._get_stories(
                account["instagram_business_account"], {"limit": self.result_return_limit}, self.filter_input_fields(fields)
            )
            for record in stories:
                record_data = record.export_all_data()
                record_data.update(
                    {
                        "page_id": account["page_id"],
                        "business_account_id": account["instagram_business_account"].get("id"),
                    }
                )
                yield clear_video_url(record_data)

    @backoff_policy
    def _get_stories(self, instagram_user: IGUser, params: dict, fields: Sequence[str] = None) -> Iterator[Any]:
        """
        This is necessary because the functions that call this endpoint return
        a generator, whose calls need decorated with a backoff.
        """
        return instagram_user.get_stories(params=params, fields=fields)


class MediaInsightsAPI(MediaAPI):
    MEDIA_METRICS = ["engagement", "impressions", "reach", "saved"]
    CAROUSEL_ALBUM_METRICS = ["carousel_album_engagement", "carousel_album_impressions", "carousel_album_reach", "carousel_album_saved"]

    def list(self, fields: Sequence[str] = None) -> Iterator[dict]:
        for account in self._api.accounts:
            media = self._get_media(account["instagram_business_account"], {"limit": self.result_return_limit}, ["media_type"])
            for ig_media in media:
                yield {
                    **{
                        "id": ig_media.get("id"),
                        "page_id": account["page_id"],
                        "business_account_id": account["instagram_business_account"].get("id"),
                    },
                    **{record.get("name"): record.get("values")[0]["value"] for record in self._get_insights(ig_media)},
                }

    @backoff_policy
    def _get_insights(self, item) -> Iterator[Any]:
        """
        This is necessary because the functions that call this endpoint return
        a generator, whose calls need decorated with a backoff.
        """
        if item.get("media_type") == "VIDEO":
            metrics = self.MEDIA_METRICS + ["video_views"]
        elif item.get("media_type") == "CAROUSEL_ALBUM":
            metrics = self.CAROUSEL_ALBUM_METRICS
        else:
            metrics = self.MEDIA_METRICS

        # An error might occur if the media was posted before the most recent time that
        # the user's account was converted to a business account from a personal account
        try:
            return item.get_insights(params={"metric": metrics})
        except FacebookRequestError as error:
            logger.error(f"Insights error: {error.body()}")
            raise error


class StoriesInsightsAPI(StoriesAPI):
    STORY_METRICS = ["exits", "impressions", "reach", "replies", "taps_forward", "taps_back"]

    def list(self, fields: Sequence[str] = None) -> Iterator[dict]:
        for account in self._api.accounts:
            stories = self._get_stories(account["instagram_business_account"], {"limit": self.result_return_limit}, fields=[])
            for ig_story in stories:
                insights = self._get_insights(ig_story)
                if insights:
                    yield {
                        **{
                            "id": ig_story.get("id"),
                            "page_id": account["page_id"],
                            "business_account_id": account["instagram_business_account"].get("id"),
                        },
                        **{record.get("name"): record.get("values")[0]["value"] for record in insights},
                    }

    @backoff_policy
    def _get_insights(self, item) -> Iterator[Any]:
        """
        This is necessary because the functions that call this endpoint return
        a generator, whose calls need decorated with a backoff.
        """

        # Story IG Media object metrics with values less than 5 will return an error code 10 with the message (#10)
        # Not enough viewers for the media to show insights.
        try:
            return item.get_insights(params={"metric": self.STORY_METRICS})
        except FacebookRequestError as error:
            logger.error(f"Insights error: {error.api_error_message()}")
            if error.api_error_code() == 10:
                return []
            raise error
