"""
PostHog Engineer Impact Data Fetcher
=====================================
Pre-fetches 90 days of PR, review, and label data from the PostHog GitHub
repository using the GitHub GraphQL API. Saves the result as posthog_data.json.

Usage:
    export GITHUB_TOKEN=ghp_your_token_here
    python fetch_posthog_data.py

Output:
    posthog_data.json  — static data file consumed by the Streamlit dashboard
"""

import os
import json
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Any

import requests

# ── Configuration ────────────────────────────────────────────────────────────

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
REPO_OWNER   = "PostHog"
REPO_NAME    = "posthog"
DAYS_BACK    = 90
OUTPUT_FILE  = "posthog_data.json"
GRAPHQL_URL  = "https://api.github.com/graphql"

# Bots to ignore
BOT_LOGINS = {
    "github-actions[bot]", "dependabot[bot]", "renovate[bot]",
    "posthog-bot", "codecov[bot]", "stale[bot]", "github-actions",
}

# Label weights — multiplied with base PR score
LABEL_WEIGHTS: dict[str, float] = {
    "bug":              2.0,
    "enterprise":       2.5,
    "clickhouse":       2.0,
    "performance":      1.8,
    "security":         2.5,
    "breaking change":  1.7,
    "data loss":        2.5,
    "critical":         2.5,
    "feature":          1.4,
    "ux":               1.3,
    "automerge":        0.7,   # auto-merged PRs are lower signal
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)

# ── GraphQL helpers ───────────────────────────────────────────────────────────

HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Content-Type":  "application/json",
}

PR_QUERY = """
query($owner: String!, $repo: String!, $cursor: String) {
  repository(owner: $owner, name: $repo) {
    pullRequests(
      first: 100
      after: $cursor
      states: [MERGED]
      orderBy: { field: UPDATED_AT, direction: DESC }
    ) {
      pageInfo { hasNextPage endCursor }
      nodes {
        number
        title
        mergedAt
        additions
        deletions
        changedFiles
        author        { login }
        labels(first: 20) { nodes { name } }
        reviews(first: 50) {
          nodes {
            author    { login }
            state
            submittedAt
            body
          }
        }
        reviewRequests(first: 20) {
          nodes {
            requestedReviewer {
              ... on User { login }
            }
          }
        }
        timelineItems(first: 5, itemTypes: [READY_FOR_REVIEW_EVENT]) {
          nodes {
            ... on ReadyForReviewEvent { createdAt }
          }
        }
        comments(first: 1) { totalCount }
      }
    }
  }
  rateLimit { remaining resetAt }
}
"""


# def gql(query: str, variables: dict) -> dict:
#     """Execute a single GraphQL request, respecting rate limits."""
#     resp = requests.post(
#         GRAPHQL_URL,
#         json={"query": query, "variables": variables},
#         headers=HEADERS,
#         timeout=30,
#     )
#     resp.raise_for_status()
#     data = resp.json()
#     if "errors" in data:
#         raise RuntimeError(f"GraphQL errors: {data['errors']}")
#     return data["data"]

import random

def gql(query: str, variables: dict) -> dict:
    """Execute a single GraphQL request, with retries on transient failures."""
    max_retries = 8
    base_sleep = 2

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(
                GRAPHQL_URL,
                json={"query": query, "variables": variables},
                headers=HEADERS,
                timeout=90,  # 30 -> 90
            )

            # transient upstream errors
            if resp.status_code in (502, 503, 504):
                raise requests.exceptions.HTTPError(
                    f"{resp.status_code} transient error", response=resp
                )

            resp.raise_for_status()
            data = resp.json()
            if "errors" in data:
                raise RuntimeError(f"GraphQL errors: {data['errors']}")
            return data["data"]

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError,
                requests.exceptions.HTTPError) as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status not in (None, 502, 503, 504) and not isinstance(e, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)):
                raise

            sleep = base_sleep * (2 ** (attempt - 1)) + random.random()
            log.warning(f"Transient error (attempt {attempt}/{max_retries}): {e}. Sleeping {sleep:.1f}s")
            time.sleep(min(sleep, 60))

    raise RuntimeError("GraphQL request failed after retries.")


def fetch_all_merged_prs(cutoff: datetime) -> list[dict]:
    """Page through merged PRs until we reach the cutoff date."""
    prs: list[dict] = []
    cursor: str | None = None
    page = 0

    while True:
        page += 1
        log.info(f"Fetching page {page} (collected {len(prs)} PRs so far)…")

        data = gql(PR_QUERY, {"owner": REPO_OWNER, "repo": REPO_NAME, "cursor": cursor})
        repo_data = data["repository"]["pullRequests"]
        rate      = data["rateLimit"]

        nodes = repo_data["nodes"]
        if not nodes:
            break

        for pr in nodes:
            merged_at_str = pr.get("mergedAt")
            if not merged_at_str:
                continue
            merged_at = datetime.fromisoformat(merged_at_str.replace("Z", "+00:00"))
            if merged_at < cutoff:
                log.info(f"Reached cutoff at PR #{pr['number']} merged {merged_at.date()}")
                return prs
            prs.append(pr)

        page_info = repo_data["pageInfo"]
        if not page_info["hasNextPage"]:
            break
        cursor = page_info["endCursor"]

        # Respect rate limit
        remaining = rate["remaining"]
        log.info(f"  Rate limit remaining: {remaining}")
        if remaining < 50:
            reset_at = datetime.fromisoformat(rate["resetAt"].replace("Z", "+00:00"))
            wait_sec = (reset_at - datetime.now(timezone.utc)).total_seconds() + 5
            log.warning(f"  Rate limit low — sleeping {wait_sec:.0f}s")
            time.sleep(max(wait_sec, 0))

    return prs


# ── Scoring engine ────────────────────────────────────────────────────────────

def label_multiplier(labels: list[str]) -> float:
    mult = 1.0
    for lbl in labels:
        lbl_lower = lbl.lower()
        for key, weight in LABEL_WEIGHTS.items():
            if key in lbl_lower:
                mult = max(mult, weight)
    return mult


def pr_size_score(additions: int, deletions: int, changed_files: int) -> float:
    """
    Non-linear score rewarding meaningful changes but penalising churn.
    Capped so that a single massive refactor doesn't dominate.
    """
    net   = additions + deletions
    score = min(net / 200, 5.0) + min(changed_files / 10, 3.0)
    return round(score, 2)


def review_response_hours(pr: dict) -> float | None:
    """
    Returns hours between PR being ready and first substantive review.
    Returns None if we can't determine it.
    """
    ready_events = pr.get("timelineItems", {}).get("nodes", [])
    ready_at_str = ready_events[0].get("createdAt") if ready_events else pr.get("mergedAt")
    if not ready_at_str:
        return None
    ready_at = datetime.fromisoformat(ready_at_str.replace("Z", "+00:00"))

    reviews = pr.get("reviews", {}).get("nodes", [])
    substantive = [
        r for r in reviews
        if r.get("state") in ("APPROVED", "CHANGES_REQUESTED")
        and r.get("author", {}).get("login") not in BOT_LOGINS
    ]
    if not substantive:
        return None

    first_review = min(
        datetime.fromisoformat(r["submittedAt"].replace("Z", "+00:00"))
        for r in substantive
    )
    delta = (first_review - ready_at).total_seconds() / 3600
    return max(delta, 0.0)


def velocity_penalty(response_hours: float | None) -> float:
    """Penalty factor 0-1 (1 = no penalty)."""
    if response_hours is None:
        return 1.0
    if response_hours <= 4:
        return 1.0
    if response_hours <= 24:
        return 0.9
    if response_hours <= 72:
        return 0.75
    return 0.6


# ── Main aggregation ──────────────────────────────────────────────────────────

def aggregate(prs: list[dict]) -> dict[str, Any]:
    """
    Returns per-engineer score breakdown and raw stats.
    """
    engineers: dict[str, dict] = {}

    def eng(login: str) -> dict:
        if login not in engineers:
            engineers[login] = {
                "login":                login,
                "merged_prs":           0,
                "pr_score":             0.0,
                "review_score":         0.0,
                "approvals_given":      0,
                "changes_requested":    0,
                "reviews_given":        0,
                "labels_touched":       set(),
                "total_additions":      0,
                "total_deletions":      0,
                "avg_response_hours":   [],
                "pr_details":           [],
            }
        return engineers[login]

    for pr in prs:
        author_login = (pr.get("author") or {}).get("login", "ghost")
        if author_login in BOT_LOGINS or not author_login:
            continue

        labels     = [l["name"] for l in pr.get("labels", {}).get("nodes", [])]
        mult       = label_multiplier(labels)
        size       = pr_size_score(pr["additions"], pr["deletions"], pr["changedFiles"])
        resp_hrs   = review_response_hours(pr)
        vel_pen    = velocity_penalty(resp_hrs)
        base_score = (3.0 + size) * mult * vel_pen

        a = eng(author_login)
        a["merged_prs"]      += 1
        a["pr_score"]        += round(base_score, 2)
        a["total_additions"] += pr["additions"]
        a["total_deletions"] += pr["deletions"]
        a["labels_touched"].update(labels)
        if resp_hrs is not None:
            a["avg_response_hours"].append(resp_hrs)
        a["pr_details"].append({
            "number":   pr["number"],
            "title":    pr["title"],
            "merged":   pr["mergedAt"][:10],
            "score":    round(base_score, 2),
            "labels":   labels,
            "mult":     mult,
            "size":     size,
            "vel_pen":  vel_pen,
        })

        # ── Reviews ──────────────────────────────────────────────────────
        seen_reviewers: set[str] = set()
        for review in pr.get("reviews", {}).get("nodes", []):
            reviewer = (review.get("author") or {}).get("login", "")
            if not reviewer or reviewer in BOT_LOGINS or reviewer == author_login:
                continue
            state = review.get("state", "")
            body  = review.get("body", "") or ""

            r = eng(reviewer)
            r["reviews_given"] += 1

            if state == "APPROVED":
                r["approvals_given"] += 1
                review_pts = 4.0 * mult  # meaningful approval on complex PR
            elif state == "CHANGES_REQUESTED":
                r["changes_requested"] += 1
                review_pts = 6.0 * mult  # blocking review = high signal
            else:
                # COMMENTED — partial credit based on body length
                review_pts = min(len(body) / 200, 2.0) * mult

            if reviewer not in seen_reviewers:
                r["review_score"] += round(review_pts, 2)
                seen_reviewers.add(reviewer)

    # Finalise
    result: dict[str, Any] = {}
    for login, d in engineers.items():
        hrs = d["avg_response_hours"]
        d["avg_response_hours"] = round(sum(hrs) / len(hrs), 1) if hrs else None
        d["labels_touched"]     = sorted(d["labels_touched"])
        d["total_score"]        = round(d["pr_score"] + d["review_score"], 2)
        result[login] = d

    return result


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    if not GITHUB_TOKEN:
        raise SystemExit("❌  Set the GITHUB_TOKEN environment variable first.")

    cutoff = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)
    log.info(f"Fetching merged PRs since {cutoff.date()} for {REPO_OWNER}/{REPO_NAME}")

    prs = fetch_all_merged_prs(cutoff)
    log.info(f"Fetched {len(prs)} merged PRs. Aggregating scores…")

    scores = aggregate(prs)
    log.info(f"Found {len(scores)} unique contributors.")

    output = {
        "generated_at":  datetime.now(timezone.utc).isoformat(),
        "repo":          f"{REPO_OWNER}/{REPO_NAME}",
        "days_back":     DAYS_BACK,
        "total_prs":     len(prs),
        "engineers":     scores,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2, default=str)

    log.info(f"✅  Saved → {OUTPUT_FILE}")

    # Print top 10 preview
    ranked = sorted(scores.values(), key=lambda x: x["total_score"], reverse=True)
    print("\n── Top 10 Engineers ──────────────────────────────")
    for i, eng in enumerate(ranked[:10], 1):
        print(f"  {i:2}. {eng['login']:<25}  score={eng['total_score']:7.1f}"
              f"  prs={eng['merged_prs']:4}  reviews={eng['reviews_given']:4}")


if __name__ == "__main__":
    main()