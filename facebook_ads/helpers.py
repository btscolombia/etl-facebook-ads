"""Facebook ads source helpers"""

import functools
import itertools
from typing import Any, Iterator, Sequence

import dlt
import humanize
import pendulum

from dlt.common import logger
from dlt.common.configuration.inject import with_config
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.typing import DictStrAny, TDataItem, TDataItems
from dlt.sources.helpers import requests
from dlt.sources.helpers.requests import Client

from facebook_business import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.user import User
from facebook_business.api import FacebookResponse

from .exceptions import InsightsJobTimeout
from .settings import (
    FACEBOOK_INSIGHTS_RETENTION_PERIOD,
    INSIGHTS_PRIMARY_KEY,
    TFbMethod,
)
from .utils import AbstractCrudObject, AbstractObject


def get_start_date(
    incremental_start_date: dlt.sources.incremental[str],
    attribution_window_days_lag: int = 7,
) -> pendulum.DateTime:
    """Get the start date for incremental loading of Facebook Insights data."""
    start_date: pendulum.DateTime = ensure_pendulum_datetime(
        incremental_start_date.start_value
    ).subtract(days=attribution_window_days_lag)

    min_start_date = pendulum.today().subtract(
        months=FACEBOOK_INSIGHTS_RETENTION_PERIOD
    )
    if start_date < min_start_date:
        logger.warning(
            "%s: Start date is earlier than %s months ago, using %s instead.",
            "facebook_insights",
            FACEBOOK_INSIGHTS_RETENTION_PERIOD,
            min_start_date,
        )
        start_date = min_start_date
        incremental_start_date.start_value = min_start_date

    incremental_start_date.start_value = start_date.isoformat()
    return start_date


def process_report_item(item: AbstractObject) -> DictStrAny:
    """Process a single insights report item."""
    d: DictStrAny = item.export_all_data()
    for pki in INSIGHTS_PRIMARY_KEY:
        if pki not in d:
            d[pki] = "no_" + pki
    return d


def get_data_chunked(
    method: TFbMethod, fields: Sequence[str], states: Sequence[str], chunk_size: int
) -> Iterator[TDataItems]:
    """Fetch data in chunks with pagination."""
    params: DictStrAny = {"limit": chunk_size}
    if states:
        params.update({"effective_status": states})
    it = map(
        lambda c: c.export_all_data(), method(fields=fields, params=params)
    )
    while True:
        chunk = list(itertools.islice(it, chunk_size))
        if not chunk:
            break
        yield chunk


def enrich_ad_objects(fb_obj_type: AbstractObject, fields: Sequence[str]) -> Any:
    """Returns a transformation to enrich resources with additional fields."""

    def _wrap(items: TDataItems, meta: Any = None) -> TDataItems:
        api_batch = FacebookAdsApi.get_default_api().new_batch()

        def update_item(resp: FacebookResponse, item: TDataItem) -> None:
            item.update(resp.json())

        def fail(resp: FacebookResponse) -> None:
            raise resp.error()

        for item in items:
            o: AbstractCrudObject = fb_obj_type(item["id"])
            o.api_get(
                fields=fields,
                batch=api_batch,
                success=functools.partial(update_item, item=item),
                failure=fail,
            )
        api_batch.execute()
        return items

    return _wrap


def execute_job(
    job: AbstractCrudObject,
    insights_max_wait_to_start_seconds: int = 5 * 60,
    insights_max_wait_to_finish_seconds: int = 30 * 60,
    insights_max_async_sleep_seconds: int = 5 * 60,
) -> AbstractCrudObject:
    """Execute an async Facebook insights job and wait for completion."""
    import time

    status = None
    time_start = time.time()
    sleep_time = 10
    while status != "Job Completed":
        duration = time.time() - time_start
        job = job.api_get()
        status = job["async_status"]
        percent_complete = job["async_percent_completion"]
        job_id = job["id"]
        logger.info("%s, %d%% done", status, percent_complete)

        if status == "Job Completed":
            return job

        if duration > insights_max_wait_to_start_seconds and percent_complete == 0:
            raise InsightsJobTimeout(
                "facebook_insights",
                f"Insights job {job_id} did not start after {insights_max_wait_to_start_seconds} seconds.",
            )
        if duration > insights_max_wait_to_finish_seconds and status != "Job Completed":
            raise InsightsJobTimeout(
                "facebook_insights",
                f"Insights job {job_id} did not complete after {insights_max_wait_to_finish_seconds // 60} seconds.",
            )

        logger.info("sleeping for %d seconds until job is done", sleep_time)
        time.sleep(sleep_time)
        if sleep_time < insights_max_async_sleep_seconds:
            sleep_time = 2 * sleep_time
    return job


def get_ads_account(
    account_id: str, access_token: str, request_timeout: float, app_api_version: str
) -> AdAccount:
    """Initialize Facebook Ads API and return the AdAccount."""
    notify_on_token_expiration()

    def retry_on_limit(response: requests.Response, exception: BaseException) -> bool:
        try:
            error = response.json()["error"]
            code = error["code"]
            message = error["message"]
            should_retry = code in (1, 2, 4, 17, 341, 32, 613, *range(80000, 80007), 800008, 800009, 80014)
            if should_retry:
                logger.warning("facebook_ads source will retry due to %s with error code %i", message, code)
            return should_retry
        except Exception:
            return False

    retry_session = Client(
        request_timeout=request_timeout,
        raise_for_status=False,
        retry_condition=retry_on_limit,
        # Limita los reintentos por petición para no quedarse intentando indefinidamente
        request_max_attempts=3,
        request_backoff_factor=2,
    ).session
    retry_session.params.update({"access_token": access_token})
    FacebookAdsApi.init(
        account_id="act_" + account_id,
        access_token=access_token,
        api_version=app_api_version,
    )
    FacebookAdsApi.get_default_api()._session.requests = retry_session

    user = User(fbid="me")
    accounts = user.get_ad_accounts()
    account = None
    for acc in accounts:
        acc_id = str(acc.get("account_id") or acc.get("id", "")).replace("act_", "")
        if acc_id == str(account_id).replace("act_", ""):
            account = acc
            break

    if not account:
        raise ValueError(f"Couldn't find account with id {account_id}")

    return account


@with_config(sections=("sources", "facebook_ads"))
def notify_on_token_expiration(access_token_expires_at: int = None) -> None:
    """Notifies if access token expires in less than 7 days."""
    if not access_token_expires_at:
        logger.warning("Token expiration time notification disabled.")
    else:
        expires_at = pendulum.from_timestamp(access_token_expires_at)
        if expires_at < pendulum.now().add(days=7):
            logger.error(
                "Access Token expires in %s. Replace the token now!",
                humanize.precisedelta(expires_at - pendulum.now()),
            )
