"""Small GitHub REST API client with pagination, bounded retries and rate-limit handling.

Retry policy i am working with:
- Network timeouts / connection errors and 5xx responses are transient: retry a
  fixed number of times with exponential backoff, then give up.
- Rate-limit responses (403/429 with no remaining quota, or Retry-After) are NOT
  retried here: waiting up to an hour inside a task is wasteful, so we raise
  RateLimitExceeded with the reset time and let the orchestrator decide.
- Other 4xx responses (404, 401, ...) are permanent: fail immediately.
"""
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterator

import requests

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.github.com"
RETRYABLE_EXCEPTIONS = (requests.exceptions.Timeout, requests.exceptions.ConnectionError)


class GitHubAPIError(Exception):
    """A request failed permanently or ran out of retries."""


class RateLimitExceeded(GitHubAPIError):
    def __init__(self, reset_at: datetime):
        self.reset_at = reset_at
        super().__init__(f"GitHub rate limit exhausted; resets at {reset_at.isoformat()}")


@dataclass(frozen=True)
class RateLimit:
    limit: int
    remaining: int
    reset_at: datetime


@dataclass(frozen=True)
class Page:
    """One API response page, kept unmodified so it can be stored as raw JSON."""
    org: str
    page_number: int
    url: str
    raw_text: str
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def records(self) -> list[dict]:
        return json.loads(self.raw_text)


class GitHubClient:
    def __init__(
        self,
        token: str | None = None,
        base_url: str = DEFAULT_BASE_URL,
        timeout: tuple[float, float] = (5, 30),
        max_attempts: int = 4,
        backoff_base: float = 1.0,
        max_pages: int = 100,
        sleep: Callable[[float], None] = time.sleep,
        session: requests.Session | None = None,
    ):
        token = token or os.environ.get("GITHUB_TOKEN")
        if not token:
            raise ValueError("GitHub token missing: set the GITHUB_TOKEN environment variable")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.backoff_base = backoff_base
        self.max_pages = max_pages
        self._sleep = sleep
        self._session = session or requests.Session()
        # The token lives only in the session headers; it is never stored on self or logged.
        self._session.headers.update({
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "github-airflow-ingestion",
        })

    def __repr__(self) -> str:
        return f"GitHubClient(base_url={self.base_url!r})"

    def get_rate_limit(self) -> RateLimit:
        core = self._get(f"{self.base_url}/rate_limit").json()["resources"]["core"]
        return RateLimit(
            limit=core["limit"],
            remaining=core["remaining"],
            reset_at=datetime.fromtimestamp(core["reset"], tz=timezone.utc),
        )

    def iter_org_repo_pages(self, org: str, per_page: int = 100) -> Iterator[Page]:         # requests 100 repos per page and follows GitHub's Link: rel="next" header until there are no pages left.
        """Yield every page of public repositories for an organisation.

        GitHub paginates with a `Link: <...>; rel="next"` header. We follow it
        until it disappears rather than guessing page numbers, and stop with an
        error after `max_pages` so a misbehaving API can't loop us forever.
        """
        url: str | None = f"{self.base_url}/orgs/{org}/repos"
        params: dict | None = {"type": "public", "per_page": per_page, "sort": "full_name"}
        page_number = 0
        while url:
            page_number += 1
            if page_number > self.max_pages:
                raise GitHubAPIError(f"Exceeded max_pages={self.max_pages} for org '{org}'")
            resp = self._get(url, params=params)
            yield Page(org=org, page_number=page_number, url=resp.url, raw_text=resp.text)
            url = resp.links.get("next", {}).get("url")
            params = None  # the "next" URL already contains the query string
        logger.info("Fetched %d page(s) for org '%s'", page_number, org)

    def _get(self, url: str, params: dict | None = None) -> requests.Response:
        for attempt in range(1, self.max_attempts + 1):
            is_last = attempt == self.max_attempts
            try:
                resp = self._session.get(url, params=params, timeout=self.timeout)
            except RETRYABLE_EXCEPTIONS as exc:
                if is_last:
                    raise GitHubAPIError(f"GET {url} failed after {attempt} attempts: {type(exc).__name__}") from exc
                self._backoff(url, attempt, type(exc).__name__)
                continue

            reset_at = _rate_limit_reset(resp)
            if reset_at is not None:
                raise RateLimitExceeded(reset_at)
            if resp.status_code >= 500:
                if is_last:
                    raise GitHubAPIError(f"GET {url} returned {resp.status_code} after {attempt} attempts")
                self._backoff(url, attempt, f"HTTP {resp.status_code}")
                continue
            if resp.status_code >= 400:
                raise GitHubAPIError(f"GET {url} returned {resp.status_code}: {resp.text[:200]}")
            return resp
        raise AssertionError("unreachable")

    def _backoff(self, url: str, attempt: int, reason: str) -> None:         #Retry timeouts and 5xx errors a fixed number of times, waiting longer each time (exponential backoff), so it can't loop forever.
        delay = self.backoff_base * 2 ** (attempt - 1)
        logger.warning("GET %s failed (%s), attempt %d/%d; retrying in %.1fs",
                       url, reason, attempt, self.max_attempts, delay)
        self._sleep(delay)


def _rate_limit_reset(resp: requests.Response) -> datetime | None:
    """Return the reset time if this response is a rate-limit rejection, else None."""
    if resp.status_code not in (403, 429):
        return None
    retry_after = resp.headers.get("Retry-After")       # Basically, when the rate limit is exhausted, stop and raise an error that includes the reset time, instead of retrying.
    if retry_after is not None:
        return datetime.now(timezone.utc) + timedelta(seconds=int(retry_after))
    if resp.headers.get("X-RateLimit-Remaining") == "0":
        reset = int(resp.headers.get("X-RateLimit-Reset", time.time()))
        return datetime.fromtimestamp(reset, tz=timezone.utc)
    return None
