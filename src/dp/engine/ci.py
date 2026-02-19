"""GitHub Actions CI integration.

Generates workflow files and posts diff results as PR comments.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

WORKFLOW_TEMPLATE = """\
name: dp CI
on:
  pull_request:
    branches: [main]

jobs:
  diff:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dp
        run: pip install dp-platform

      - name: Build current branch
        run: dp stream full-refresh

      - name: Create baseline and diff
        run: |
          # Save current branch name
          CURRENT_BRANCH="${{ github.head_ref }}"

          # Build main baseline
          git checkout main
          dp stream full-refresh
          dp snapshot create main-baseline

          # Switch back and rebuild
          git checkout "$CURRENT_BRANCH"
          dp stream full-refresh

          # Generate diff
          dp diff --snapshot main-baseline --format json > diff-results.json

      - name: Post diff to PR
        if: always()
        uses: actions/github-script@v7
        with:
          script: |
            const fs = require('fs');
            let diff;
            try {
              diff = JSON.parse(fs.readFileSync('diff-results.json', 'utf8'));
            } catch (e) {
              console.log('No diff results found');
              return;
            }

            // Format as markdown
            let body = '## dp data diff\\n\\n';

            if (diff.table_changes && diff.table_changes.length > 0) {
              body += '| Table | Status | Snapshot Rows | Current Rows |\\n';
              body += '|-------|--------|--------------|-------------|\\n';
              for (const tc of diff.table_changes) {
                body += `| ${tc.table} | ${tc.status} | ${tc.snapshot_rows || '-'} | ${tc.current_rows || '-'} |\\n`;
              }
            } else {
              body += 'No data changes detected.\\n';
            }

            if (diff.file_changes) {
              const fc = diff.file_changes;
              if (fc.added?.length || fc.removed?.length || fc.modified?.length) {
                body += '\\n### File changes\\n';
                for (const f of (fc.added || [])) body += `- :heavy_plus_sign: ${f}\\n`;
                for (const f of (fc.removed || [])) body += `- :heavy_minus_sign: ${f}\\n`;
                for (const f of (fc.modified || [])) body += `- :pencil2: ${f}\\n`;
              }
            }

            await github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body: body,
            });
"""


def generate_workflow(project_dir: Path) -> dict:
    """Generate .github/workflows/dp-ci.yml in the project root."""
    workflows_dir = project_dir / ".github" / "workflows"
    workflows_dir.mkdir(parents=True, exist_ok=True)

    workflow_path = workflows_dir / "dp-ci.yml"
    workflow_path.write_text(WORKFLOW_TEMPLATE)

    return {"path": str(workflow_path.relative_to(project_dir))}


def _format_diff_comment(diff_data: list[dict] | dict) -> str:
    """Format diff results as a markdown PR comment."""
    lines = ["## dp data diff", ""]

    # Handle both list of DiffResults and snapshot diff format
    if isinstance(diff_data, dict):
        # Snapshot diff format
        table_changes = diff_data.get("table_changes", [])
        if table_changes:
            lines.append("| Table | Status | Snapshot Rows | Current Rows |")
            lines.append("|-------|--------|--------------|-------------|")
            for tc in table_changes:
                lines.append(
                    f"| {tc['table']} | {tc['status']} "
                    f"| {tc.get('snapshot_rows', '-')} "
                    f"| {tc.get('current_rows', '-')} |"
                )
        else:
            lines.append("No data changes detected.")

        file_changes = diff_data.get("file_changes", {})
        if any(file_changes.get(k) for k in ("added", "removed", "modified")):
            lines.extend(["", "### File changes"])
            for f in file_changes.get("added", []):
                lines.append(f"- :heavy_plus_sign: {f}")
            for f in file_changes.get("removed", []):
                lines.append(f"- :heavy_minus_sign: {f}")
            for f in file_changes.get("modified", []):
                lines.append(f"- :pencil2: {f}")
    elif isinstance(diff_data, list):
        # List of DiffResult dicts
        has_changes = any(
            r.get("added", 0) or r.get("removed", 0) or r.get("modified", 0)
            or r.get("schema_changes") or r.get("is_new") or r.get("error")
            for r in diff_data
        )
        if not has_changes:
            lines.append("No data changes detected.")
        else:
            lines.append("| Model | Before | After | Added | Removed | Modified | Schema |")
            lines.append("|-------|--------|-------|-------|---------|----------|--------|")
            for r in diff_data:
                if r.get("error"):
                    lines.append(f"| {r['model']} | | | | | | ERROR |")
                    continue
                before = "NEW" if r.get("is_new") else str(r.get("total_before", 0))
                after = str(r.get("total_after", 0))
                added = f"+{r.get('added', 0)}" if r.get("added") else "0"
                removed = str(r.get("removed", 0))
                modified = str(r.get("modified", 0))
                sc = r.get("schema_changes", [])
                schema_label = f"{len(sc)} change(s)" if sc else "\u2014"
                lines.append(
                    f"| {r['model']} | {before} | {after} "
                    f"| {added} | {removed} | {modified} | {schema_label} |"
                )

            # Sample rows in details
            for r in diff_data:
                if r.get("sample_added") or r.get("sample_removed") or r.get("sample_modified"):
                    lines.extend(["", f"<details><summary>Sample changed rows for {r['model']}</summary>", ""])
                    if r.get("sample_added"):
                        lines.append(f"**Added ({r.get('added', 0)} rows):**")
                        lines.append("")
                        lines.append(_dict_list_to_md_table(r["sample_added"]))
                    if r.get("sample_removed"):
                        lines.append(f"**Removed ({r.get('removed', 0)} rows):**")
                        lines.append("")
                        lines.append(_dict_list_to_md_table(r["sample_removed"]))
                    if r.get("sample_modified"):
                        lines.append(f"**Modified ({r.get('modified', 0)} rows):**")
                        lines.append("")
                        lines.append(_dict_list_to_md_table(r["sample_modified"]))
                    lines.append("</details>")

    return "\n".join(lines)


def _dict_list_to_md_table(rows: list[dict]) -> str:
    """Convert a list of dicts to a markdown table."""
    if not rows:
        return ""
    cols = list(rows[0].keys())
    lines = [
        "| " + " | ".join(cols) + " |",
        "| " + " | ".join("---" for _ in cols) + " |",
    ]
    for row in rows[:10]:  # Cap at 10 for readability
        lines.append("| " + " | ".join(str(row.get(c, "")) for c in cols) + " |")
    if len(rows) > 10:
        lines.append(f"| ... {len(rows) - 10} more rows ... |")
    return "\n".join(lines)


def post_diff_comment(
    json_path: str,
    repo: str | None = None,
    pr: int | None = None,
) -> dict:
    """Post a formatted diff comment to a GitHub PR.

    Requires GITHUB_TOKEN env var. Uses repo and pr from env if not provided.
    """
    from urllib.request import Request, urlopen

    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        return {"error": "GITHUB_TOKEN environment variable not set"}

    # Read diff data
    try:
        with open(json_path) as f:
            diff_data = json.load(f)
    except FileNotFoundError:
        return {"error": f"Diff results file not found: {json_path}"}
    except json.JSONDecodeError as e:
        return {"error": f"Invalid JSON in {json_path}: {e}"}

    # Resolve repo and PR from GitHub Actions environment
    if not repo:
        repo = os.environ.get("GITHUB_REPOSITORY")
    if not pr:
        # Try to get from GITHUB_REF (refs/pull/123/merge)
        ref = os.environ.get("GITHUB_REF", "")
        if "/pull/" in ref:
            try:
                pr = int(ref.split("/pull/")[1].split("/")[0])
            except (ValueError, IndexError):
                pass

    if not repo:
        return {"error": "Could not determine GitHub repository. Use --repo flag."}
    if not pr:
        return {"error": "Could not determine PR number. Use --pr flag."}

    # Format comment
    comment_body = _format_diff_comment(diff_data)

    # Post comment via GitHub API
    url = f"https://api.github.com/repos/{repo}/issues/{pr}/comments"
    payload = json.dumps({"body": comment_body}).encode()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/vnd.github.v3+json",
    }

    try:
        req = Request(url, data=payload, headers=headers)
        urlopen(req, timeout=30)
        return {"pr": pr, "repo": repo}
    except Exception as e:
        return {"error": f"Failed to post comment: {e}"}
