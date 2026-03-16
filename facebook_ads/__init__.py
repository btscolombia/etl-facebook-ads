"""Loads campaigns, ad sets, ads, leads and insight data from Facebook Marketing API"""

import time
from typing import Iterator, Sequence

from facebook_business.api import FacebookResponse

import dlt
from dlt.common import pendulum
from dlt.common.typing import TDataItems, TDataItem, DictStrAny
from dlt.sources import DltResource

from .helpers import (
    get_data_chunked,
    enrich_ad_objects,
    get_start_date,
    process_report_item,
    execute_job,
    get_ads_account,
)
from .utils import (
    Ad,
    Campaign,
    AdSet,
    AdCreative,
    Lead,
)
from .utils import debug_access_token, get_long_lived_token
from .settings import (
    DEFAULT_AD_FIELDS,
    DEFAULT_ADCREATIVE_FIELDS,
    DEFAULT_ADSET_FIELDS,
    DEFAULT_CAMPAIGN_FIELDS,
    DEFAULT_LEAD_FIELDS,
    TFbMethod,
    TInsightsBreakdownOptions,
    FACEBOOK_INSIGHTS_RETENTION_PERIOD,
    ALL_ACTION_ATTRIBUTION_WINDOWS,
    DEFAULT_INSIGHT_FIELDS,
    INSIGHT_FIELDS_TYPES,
    INSIGHTS_PRIMARY_KEY,
    INSIGHTS_BREAKDOWNS_OPTIONS,
    INVALID_INSIGHTS_FIELDS,
    TInsightsLevels,
)


@dlt.source(name="facebook_ads")
def facebook_ads_source(
    account_id: str = dlt.config.value,
    access_token: str = dlt.secrets.value,
    chunk_size: int = 50,
    request_timeout: float = 300.0,
    app_api_version: str = None,
) -> Sequence[DltResource]:
    """Returns resources to load campaigns, ad sets, ads, creatives and leads from Facebook Marketing API."""

    account = get_ads_account(
        account_id, access_token, request_timeout, app_api_version
    )

    @dlt.resource(primary_key="id", write_disposition="replace")
    def campaigns(
        fields: Sequence[str] = DEFAULT_CAMPAIGN_FIELDS, states: Sequence[str] = None
    ) -> Iterator[TDataItems]:
        yield get_data_chunked(account.get_campaigns, fields, states, chunk_size)

    @dlt.resource(primary_key="id", write_disposition="replace")
    def ads(
        fields: Sequence[str] = DEFAULT_AD_FIELDS, states: Sequence[str] = None
    ) -> Iterator[TDataItems]:
        yield get_data_chunked(account.get_ads, fields, states, chunk_size)

    @dlt.resource(primary_key="id", write_disposition="replace")
    def ad_sets(
        fields: Sequence[str] = DEFAULT_ADSET_FIELDS, states: Sequence[str] = None
    ) -> Iterator[TDataItems]:
        yield get_data_chunked(account.get_ad_sets, fields, states, chunk_size)

    @dlt.transformer(primary_key="id", write_disposition="replace", selected=True)
    def leads(
        items: TDataItems,
        fields: Sequence[str] = DEFAULT_LEAD_FIELDS,
        states: Sequence[str] = None,
    ) -> Iterator[TDataItems]:
        for item in items:
            ad = Ad(item["id"])
            yield get_data_chunked(ad.get_leads, fields, states, chunk_size)

    @dlt.resource(primary_key="id", write_disposition="replace")
    def ad_creatives(
        fields: Sequence[str] = DEFAULT_ADCREATIVE_FIELDS, states: Sequence[str] = None
    ) -> Iterator[TDataItems]:
        yield get_data_chunked(account.get_ad_creatives, fields, states, chunk_size)

    return campaigns, ads, ad_sets, ad_creatives, ads | leads


@dlt.source(name="facebook_ads")
def facebook_insights_source(
    account_id: str = dlt.config.value,
    access_token: str = dlt.secrets.value,
    initial_load_past_days: int = 30,
    max_days_per_run: int = None,
    sleep_after_n_days: int = 0,
    sleep_seconds: int = 60,
    fields: Sequence[str] = DEFAULT_INSIGHT_FIELDS,
    attribution_window_days_lag: int = 7,
    time_increment_days: int = 1,
    breakdowns: TInsightsBreakdownOptions = None,
    action_breakdowns: Sequence[str] = None,
    level: TInsightsLevels = "ad",
    action_attribution_windows: Sequence[str] = ALL_ACTION_ATTRIBUTION_WINDOWS,
    batch_size: int = 50,
    request_timeout: int = 300,
    app_api_version: str = None,
) -> DltResource:
    """Incrementally loads insight reports with defined granularity level, fields, breakdowns."""

    account = get_ads_account(
        account_id, access_token, request_timeout, app_api_version
    )

    initial_load_start_date = pendulum.today().subtract(days=initial_load_past_days)
    initial_load_start_date_str = initial_load_start_date.isoformat()

    @dlt.resource(
        primary_key=INSIGHTS_PRIMARY_KEY,
        write_disposition="merge",
        columns=INSIGHT_FIELDS_TYPES,
    )
    def facebook_insights(
        date_start: dlt.sources.incremental[str] = dlt.sources.incremental(
            "date_start", initial_value=initial_load_start_date_str
        )
    ) -> Iterator[TDataItems]:
        start_date = get_start_date(date_start, attribution_window_days_lag)
        end_date = pendulum.now()
        days_processed = 0

        while start_date <= end_date:
            if max_days_per_run is not None and days_processed >= max_days_per_run:
                break
            query = {
                "level": level,
                "limit": batch_size,
                "time_increment": time_increment_days,
                "action_attribution_windows": list(action_attribution_windows),
                "time_ranges": [
                    {
                        "since": start_date.to_date_string(),
                        "until": start_date.add(
                            days=time_increment_days - 1
                        ).to_date_string(),
                    },
                ],
            }

            fields_to_use = set(fields)
            if breakdowns is not None:
                query["breakdowns"] = list(
                    INSIGHTS_BREAKDOWNS_OPTIONS[breakdowns]["breakdowns"]
                )
                fields_to_use = fields_to_use.union(
                    INSIGHTS_BREAKDOWNS_OPTIONS[breakdowns]["fields"]
                )
            query["fields"] = list(fields_to_use.difference(INVALID_INSIGHTS_FIELDS))

            if action_breakdowns is not None:
                query["action_breakdowns"] = list(action_breakdowns)

            job = execute_job(account.get_insights(params=query, is_async=True))
            yield list(map(process_report_item, job.get_result()))
            days_processed += time_increment_days
            start_date = start_date.add(days=time_increment_days)

            if (
                (sleep_after_n_days or 0) > 0
                and days_processed % (sleep_after_n_days or 1) == 0
                and days_processed > 0
                and start_date <= end_date
            ):
                time.sleep(sleep_seconds)

    return facebook_insights
