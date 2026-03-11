from __future__ import annotations

import os
import re
from pathlib import Path

from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/api/wiki", tags=["wiki"])

PAGES_DIR = Path(__file__).resolve().parent.parent.parent / "wiki" / "pages"


def _slug(filename: str) -> str:
    return filename.removesuffix(".md")


def _extract_title(content: str) -> str:
    for line in content.splitlines():
        line = line.strip()
        if line.startswith("# "):
            return line[2:].strip()
    return ""


def _extract_category(slug: str) -> str:
    categories = {
        "index": "Getting Started",
        "getting-started": "Getting Started",
        "transforms": "Core Concepts",
        "pipelines": "Core Concepts",
        "configuration": "Core Concepts",
        "environments": "Core Concepts",
        "connectors": "Data Integration",
        "cdc": "Data Integration",
        "seeds": "Data Integration",
        "sources": "Data Integration",
        "quality": "Data Quality",
        "contracts": "Data Quality",
        "lineage": "Data Quality",
        "auth": "Security",
        "masking": "Security",
        "scheduler": "Advanced",
        "notebooks": "Advanced",
        "versioning": "Advanced",
        "cli-reference": "Reference",
        "api-reference": "Reference",
    }
    return categories.get(slug, "Other")


@router.get("")
async def list_pages():
    if not PAGES_DIR.exists():
        return []
    pages = []
    for f in sorted(PAGES_DIR.iterdir()):
        if f.suffix == ".md":
            slug = _slug(f.name)
            content = f.read_text(encoding="utf-8")
            title = _extract_title(content)
            pages.append({
                "slug": slug,
                "title": title or slug.replace("-", " ").title(),
                "category": _extract_category(slug),
            })
    return pages


@router.get("/{slug}")
async def get_page(slug: str):
    safe = re.sub(r"[^a-zA-Z0-9_-]", "", slug)
    path = PAGES_DIR / f"{safe}.md"
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Page '{slug}' not found")
    content = path.read_text(encoding="utf-8")
    title = _extract_title(content)
    return {
        "slug": safe,
        "title": title or safe.replace("-", " ").title(),
        "content": content,
        "category": _extract_category(safe),
    }
