"""Generate 7 daily GitHub-like snapshots (2026-11-18 .. 2026-11-24) for stripe, shopify, microsoft.

Deterministic: running it twice produces identical files. The snapshots contain
the situations the pipeline must handle:
  * every day one repo per org changes (stars up, updated_at moves)  -> incremental pick-up
  * stripe/stripe-mcp appears on 2026-11-20                          -> insert
  * shopify/toxiproxy is archived on 2026-11-21                      -> update
  * microsoft/calculator disappears from 2026-11-21                  -> soft delete on the Monday full refresh
  * shopify/broken-fixture has forks_count = -1 every day            -> quarantine (deduplicated)
  * 2026-11-19: microsoft/vscode appears twice                       -> uniqueness < 100%
  * 2026-11-22: full_name does not match org/name                    -> quarantine
  * 2026-11-23: updated_at earlier than created_at                   -> quarantine
  * 2026-11-24: html_url missing                                     -> quarantine

Usage: python scripts/generate_fixtures.py [fixtures_dir]
"""
import copy
import json
import sys
from datetime import date, timedelta
from pathlib import Path

START = date(2026, 11, 18)
DAYS = 7
ORG_IDS = {"stripe": 856813, "shopify": 8085, "microsoft": 6154722}
REPOS = {
    "stripe": [("stripe-python", "Python", "MIT"), ("stripe-node", "TypeScript", "MIT"),
               ("stripe-go", "Go", "MIT"), ("stripe-java", "Java", "MIT"),
               ("stripe-cli", "Go", "Apache-2.0"), ("stripe-mock", "Go", "MIT"),
               ("stripe-ios", "Swift", "MIT")],
    "shopify": [("liquid", "Ruby", "MIT"), ("polaris", "TypeScript", None),
                ("hydrogen", "TypeScript", "MIT"), ("cli", "TypeScript", "MIT"),
                ("shopify-api-ruby", "Ruby", "MIT"), ("toxiproxy", "Go", "MIT"),
                ("broken-fixture", None, None)],
    "microsoft": [("vscode", "TypeScript", "MIT"), ("TypeScript", "TypeScript", "Apache-2.0"),
                  ("terminal", "C++", "MIT"), ("PowerToys", "C#", "MIT"),
                  ("playwright", "TypeScript", "Apache-2.0"), ("calculator", "C#", "MIT"),
                  ("fluentui", "TypeScript", None)],
}


def iso(d: date, hour: int = 12) -> str:
    return f"{d.isoformat()}T{hour:02d}:00:00Z"


def base_repo(org: str, idx: int, name: str, language, license_id) -> dict:
    repo_id = ORG_IDS[org] * 100 + idx
    return {
        "id": repo_id, "node_id": f"R_fixture_{repo_id}", "name": name, "full_name": f"{org}/{name}",
        "private": False, "owner": {"login": org, "id": ORG_IDS[org], "type": "Organization"},
        "html_url": f"https://github.com/{org}/{name}",
        "description": None if name == "polaris" else f"{name} (fixture snapshot)",
        "fork": False, "archived": False, "disabled": False, "visibility": "public",
        "language": language,
        "license": {"key": license_id.lower(), "spdx_id": license_id} if license_id else None,
        "default_branch": "main", "topics": [],
        "stargazers_count": 1000 * (idx + 1), "watchers_count": 1000 * (idx + 1),
        "forks_count": 100 * (idx + 1), "open_issues_count": 10 * (idx + 1),
        "created_at": f"201{idx}-0{idx + 1}-15T08:00:00Z",
        "updated_at": "2026-11-10T12:00:00Z", "pushed_at": "2026-11-10T11:00:00Z",
    }


def build_day(org: str, day: int, state: dict) -> list[dict]:
    d = START + timedelta(days=day)
    repos = state[org]
    changed = repos[day % len(repos)]
    if changed["name"] != "broken-fixture":
        changed["stargazers_count"] += 10 * (day + 1)
        changed["watchers_count"] = changed["stargazers_count"]
        changed["updated_at"], changed["pushed_at"] = iso(d), iso(d, 11)

    if org == "stripe" and day == 2:
        new = base_repo(org, 7, "stripe-mcp", "TypeScript", "MIT")
        new.update(created_at=iso(d, 9), updated_at=iso(d), pushed_at=iso(d, 11))
        repos.append(new)
    if org == "shopify" and day == 3:
        toxi = next(r for r in repos if r["name"] == "toxiproxy")
        toxi.update(archived=True, updated_at=iso(d))
    if org == "microsoft" and day == 3:
        repos[:] = [r for r in repos if r["name"] != "calculator"]

    snapshot = copy.deepcopy(repos)
    for r in snapshot:
        if r["name"] == "broken-fixture":
            r["forks_count"] = -1
    if org == "microsoft" and day == 1:
        snapshot.append(copy.deepcopy(snapshot[0]))
    if org == "microsoft" and day == 4:
        bad = base_repo(org, 8, "mismatch", "C", "MIT")
        bad.update(full_name="microsoft/something-else", created_at=iso(d, 1), updated_at=iso(d))
        snapshot.append(bad)
    if org == "stripe" and day == 5:
        bad = base_repo(org, 9, "time-travel", "Ruby", "MIT")
        bad.update(created_at="2027-01-01T00:00:00Z", updated_at=iso(d))
        snapshot.append(bad)
    if org == "stripe" and day == 6:
        bad = base_repo(org, 10, "no-url", "Python", "MIT")
        bad.update(created_at=iso(d, 1), updated_at=iso(d))
        del bad["html_url"]
        snapshot.append(bad)
    return snapshot


def main(fixtures_dir: Path) -> None:
    state = {org: [base_repo(org, i, *spec) for i, spec in enumerate(specs)] for org, specs in REPOS.items()}
    for day in range(DAYS):
        folder = fixtures_dir / "snapshots" / (START + timedelta(days=day)).isoformat()
        folder.mkdir(parents=True, exist_ok=True)
        for org in REPOS:
            (folder / f"{org}.json").write_text(json.dumps(build_day(org, day, state), indent=2) + "\n")
    print(f"Wrote {DAYS} days x {len(REPOS)} orgs to {fixtures_dir / 'snapshots'}")


if __name__ == "__main__":
    main(Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parents[1] / "fixtures")
