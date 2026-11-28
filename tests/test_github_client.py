import logging
from datetime import timezone

import pytest
import requests
import responses

from github_ingestion.github_client import GitHubAPIError, GitHubClient, RateLimitExceeded

API = "https://api.github.com"
TOKEN = "ghp_secret_test_token"


@pytest.fixture
def sleeps() -> list[float]:
    return []


@pytest.fixture
def client(sleeps) -> GitHubClient:
    # Inject a fake sleep so retry tests run instantly and we can assert the delays.
    return GitHubClient(token=TOKEN, sleep=sleeps.append)


def repos_url(org: str = "stripe") -> str:
    return f"{API}/orgs/{org}/repos"


def test_missing_token_raises(monkeypatch):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    with pytest.raises(ValueError, match="GITHUB_TOKEN"):
        GitHubClient()


def test_token_read_from_env(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", TOKEN)
    assert GitHubClient()._session.headers["Authorization"] == f"Bearer {TOKEN}"


def test_token_not_in_repr(client):
    assert TOKEN not in repr(client)


@responses.activate
def test_pagination_follows_link_header(client):
    page2, page3 = f"{repos_url()}?page=2", f"{repos_url()}?page=3"
    responses.get(repos_url(), json=[{"id": 1}, {"id": 2}], headers={"Link": f'<{page2}>; rel="next", <{page3}>; rel="last"'})
    responses.get(page2, json=[{"id": 3}], headers={"Link": f'<{page3}>; rel="next"'})
    responses.get(page3, json=[{"id": 4}])

    pages = list(client.iter_org_repo_pages("stripe"))

    assert [p.page_number for p in pages] == [1, 2, 3]
    assert [r["id"] for p in pages for r in p.records] == [1, 2, 3, 4]
    assert "per_page=100" in responses.calls[0].request.url
    assert responses.calls[0].request.headers["Authorization"] == f"Bearer {TOKEN}"


@responses.activate
def test_page_keeps_raw_text_unmodified(client):
    body = '[{"id": 1,   "name": "spacing kept"}]'
    responses.get(repos_url(), body=body, content_type="application/json")
    (page,) = client.iter_org_repo_pages("stripe")
    assert page.raw_text == body


@responses.activate
def test_empty_org_yields_single_empty_page(client):
    responses.get(repos_url(), json=[])
    pages = list(client.iter_org_repo_pages("stripe"))
    assert len(pages) == 1 and pages[0].records == []


@responses.activate
def test_max_pages_guard_stops_infinite_pagination(sleeps):
    client = GitHubClient(token=TOKEN, sleep=sleeps.append, max_pages=2)
    responses.get(repos_url(), json=[], headers={"Link": f'<{repos_url()}>; rel="next"'})
    with pytest.raises(GitHubAPIError, match="max_pages"):
        list(client.iter_org_repo_pages("stripe"))


@responses.activate
def test_5xx_is_retried_with_exponential_backoff(client, sleeps):
    responses.get(repos_url(), status=502)
    responses.get(repos_url(), status=503)
    responses.get(repos_url(), json=[{"id": 1}])

    (page,) = client.iter_org_repo_pages("stripe")

    assert page.records == [{"id": 1}]
    assert sleeps == [1.0, 2.0]


@responses.activate
def test_5xx_gives_up_after_max_attempts(client, sleeps):
    responses.get(repos_url(), status=500)
    with pytest.raises(GitHubAPIError, match="500 after 4 attempts"):
        list(client.iter_org_repo_pages("stripe"))
    assert len(responses.calls) == 4
    assert sleeps == [1.0, 2.0, 4.0]


@responses.activate
def test_timeout_is_retried(client, sleeps):
    responses.get(repos_url(), body=requests.exceptions.ReadTimeout("slow"))
    responses.get(repos_url(), json=[{"id": 1}])
    (page,) = client.iter_org_repo_pages("stripe")
    assert page.records == [{"id": 1}] and sleeps == [1.0]


@responses.activate
def test_rate_limit_raises_immediately_without_retry(client, sleeps):
    responses.get(repos_url(), status=403, json={"message": "API rate limit exceeded"},
                  headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1800000000"})
    with pytest.raises(RateLimitExceeded) as exc_info:
        list(client.iter_org_repo_pages("stripe"))
    assert exc_info.value.reset_at.timestamp() == 1800000000
    assert len(responses.calls) == 1 and sleeps == []


@responses.activate
def test_secondary_rate_limit_retry_after(client):
    responses.get(repos_url(), status=429, headers={"Retry-After": "60"})
    with pytest.raises(RateLimitExceeded):
        list(client.iter_org_repo_pages("stripe"))


@responses.activate
def test_permission_403_is_not_rate_limit(client, sleeps):
    responses.get(repos_url(), status=403, json={"message": "Forbidden"}, headers={"X-RateLimit-Remaining": "4999"})
    with pytest.raises(GitHubAPIError) as exc_info:
        list(client.iter_org_repo_pages("stripe"))
    assert not isinstance(exc_info.value, RateLimitExceeded)
    assert sleeps == []


@responses.activate
def test_404_fails_fast(client, sleeps):
    responses.get(repos_url("no-such-org"), status=404, json={"message": "Not Found"})
    with pytest.raises(GitHubAPIError, match="404"):
        list(client.iter_org_repo_pages("no-such-org"))
    assert len(responses.calls) == 1 and sleeps == []


@responses.activate
def test_get_rate_limit_parses_core_quota(client):
    responses.get(f"{API}/rate_limit", json={"resources": {"core": {"limit": 5000, "remaining": 4321, "reset": 1800000000}}})
    rl = client.get_rate_limit()
    assert (rl.limit, rl.remaining) == (5000, 4321)
    assert rl.reset_at.tzinfo == timezone.utc


@responses.activate
def test_logs_never_contain_token(client, caplog):
    responses.get(repos_url(), status=500)
    responses.get(repos_url(), json=[])
    with caplog.at_level(logging.DEBUG):
        list(client.iter_org_repo_pages("stripe"))
    assert "retrying" in caplog.text
    assert TOKEN not in caplog.text
