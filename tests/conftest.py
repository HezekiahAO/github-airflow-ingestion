import pytest


@pytest.fixture
def raw_repo() -> dict:
    """A trimmed copy of a real GitHub /orgs/{org}/repos item."""
    return {
        "id": 123,
        "name": "stripe-python",
        "full_name": "stripe/stripe-python",
        "owner": {"login": "stripe", "id": 856813},
        "html_url": "https://github.com/stripe/stripe-python",
        "description": "Python library for the Stripe API.",
        "fork": False,
        "archived": False,
        "language": "Python",
        "license": {"key": "mit", "spdx_id": "MIT"},
        "default_branch": "master",
        "visibility": "public",
        "stargazers_count": 1700,
        "forks_count": 400,
        "open_issues_count": 20,
        "watchers_count": 1700,
        "created_at": "2011-04-14T00:00:00Z",
        "updated_at": "2026-11-20T10:00:00Z",
        "pushed_at": "2026-11-20T09:00:00Z",
        "node_id": "ignored-extra-field",
    }
