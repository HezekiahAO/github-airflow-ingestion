import pytest

from github_ingestion.schemas import GitHubRepo, validate_records


def test_valid_record_is_flattened(raw_repo):
    repo = GitHubRepo.model_validate(raw_repo)
    assert repo.repo_id == 123
    assert repo.org == "stripe"
    assert repo.license == "MIT"
    assert repo.is_fork is False
    assert not hasattr(repo, "node_id")


def test_null_optional_fields_are_allowed(raw_repo):
    raw_repo.update(description=None, language=None, license=None, pushed_at=None)
    assert GitHubRepo.model_validate(raw_repo).license is None


@pytest.mark.parametrize("missing", ["id", "name", "full_name", "html_url", "created_at", "updated_at"])
def test_missing_required_field_is_invalid(raw_repo, missing):
    del raw_repo[missing]
    valid, invalid = validate_records([raw_repo])
    assert valid == []
    assert invalid[0]["raw_payload"] == raw_repo


@pytest.mark.parametrize(
    "patch, expected",
    [
        ({"stargazers_count": -1}, "greater than or equal to 0"),
        ({"id": 0}, "greater than 0"),
        ({"html_url": "http://evil.example/x"}, "https://github.com/"),
        ({"full_name": "shopify/stripe-python"}, "does not match"),
        ({"updated_at": "2000-01-01T00:00:00Z"}, "earlier than created_at"),
        ({"created_at": "not-a-date"}, "datetime"),
    ],
)
def test_business_rules(raw_repo, patch, expected):
    raw_repo.update(patch)
    _, invalid = validate_records([raw_repo])
    assert len(invalid) == 1
    assert any(expected in e["message"] for e in invalid[0]["errors"])


def test_validate_records_splits_batch(raw_repo):
    bad = {**raw_repo, "id": 456, "forks_count": -5}
    valid, invalid = validate_records([raw_repo, bad, "not-a-dict"])
    assert [r.repo_id for r in valid] == [123]
    assert [i["repo_id"] for i in invalid] == [456, None]
