"""Schema and business rules for one GitHub repository record.

The GitHub API returns ~80 fields per repository. We keep only the ones we
store, flatten nested objects (owner, license) and enforce rules that the API
itself does not guarantee.
"""
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator


class GitHubRepo(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    repo_id: int = Field(gt=0, validation_alias="id")
    org: str = Field(min_length=1)
    name: str = Field(min_length=1)
    full_name: str = Field(min_length=3)
    html_url: str
    description: str | None = None
    language: str | None = None
    license: str | None = None
    default_branch: str = Field(min_length=1)
    visibility: str = "public"
    is_fork: bool = Field(validation_alias="fork")
    is_archived: bool = Field(validation_alias="archived")
    stargazers_count: int = Field(ge=0)
    forks_count: int = Field(ge=0)
    open_issues_count: int = Field(ge=0)
    watchers_count: int = Field(ge=0)
    created_at: datetime
    updated_at: datetime
    pushed_at: datetime | None = None

    @model_validator(mode="before")
    @classmethod
    def flatten_nested(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        data = dict(data)
        owner = data.get("owner")
        if isinstance(owner, dict) and "org" not in data:
            data["org"] = owner.get("login")
        lic = data.get("license")
        if isinstance(lic, dict):
            data["license"] = lic.get("spdx_id") or lic.get("key")
        return data

    @field_validator("html_url")
    @classmethod
    def must_be_github_url(cls, v: str) -> str:
        if not v.startswith("https://github.com/"):
            raise ValueError("html_url must start with https://github.com/")
        return v

    @model_validator(mode="after")
    def check_business_rules(self) -> "GitHubRepo":
        if self.full_name.lower() != f"{self.org}/{self.name}".lower():
            raise ValueError(f"full_name '{self.full_name}' does not match org/name '{self.org}/{self.name}'")
        if self.updated_at < self.created_at:
            raise ValueError("updated_at is earlier than created_at")
        return self


def validate_records(raw_records: list[dict]) -> tuple[list[GitHubRepo], list[dict]]:
    """Split raw API records into valid models and invalid entries.

    Each invalid entry keeps the untouched payload plus readable errors so it
    can be written to the quarantine table and debugged later.
    """
    valid: list[GitHubRepo] = []
    invalid: list[dict] = []
    for raw in raw_records:
        try:
            valid.append(GitHubRepo.model_validate(raw))
        except ValidationError as exc:
            invalid.append({
                "repo_id": raw.get("id") if isinstance(raw, dict) else None,
                "raw_payload": raw,
                "errors": [
                    {"field": ".".join(str(p) for p in e["loc"]) or "__root__", "message": e["msg"]}
                    for e in exc.errors()
                ],
            })
    return valid, invalid
