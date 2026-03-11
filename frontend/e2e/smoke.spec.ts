/**
 * Comprehensive E2E smoke test for dp web UI.
 *
 * Prerequisites – run once before the test suite:
 *   cd test-project && dp stream full-refresh --force && dp serve
 *
 * Then from frontend/:
 *   npx playwright test
 */
import { test, expect, type Page } from "@playwright/test";

/* ------------------------------------------------------------------ */
/* helpers                                                             */
/* ------------------------------------------------------------------ */

/** Click a primary tab (visible in the tab bar). */
async function goTab(page: Page, name: string) {
  await page.locator(`[data-dp-guide="tabs"] [data-dp-tab]`).getByText(name, { exact: true }).click();
  await page.waitForTimeout(400);
}

/** Open a secondary tab via the "More" dropdown (portaled to body). */
async function goSecondary(page: Page, name: string) {
  // The "More" button is inside a wrapper div, not a [data-dp-tab] element
  // It shows the arrow character ▾ and either "More" or the active secondary tab name
  const tabs = page.locator(`[data-dp-guide="tabs"]`);

  // Click the More/dropdown button (contains the ▾ arrow)
  const moreBtn = tabs.locator("button").filter({ hasText: "▾" });
  await moreBtn.click();
  await page.waitForTimeout(300);

  // Dropdown is portaled to document.body — find the menu item by role
  await page.getByRole("button", { name, exact: true }).last().click();
  await page.waitForTimeout(500);
}

/** Dismiss the welcome guide if it appears. */
async function dismissGuide(page: Page) {
  const skipBtn = page.getByText("Skip", { exact: true });
  if (await skipBtn.isVisible({ timeout: 2000 }).catch(() => false)) {
    await skipBtn.click();
    await page.waitForTimeout(300);
  }
}

/* ------------------------------------------------------------------ */
/* tests                                                               */
/* ------------------------------------------------------------------ */

test.describe("dp UI smoke tests", () => {

  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(`[data-dp-guide="tabs"]`)).toBeVisible({ timeout: 15000 });
    await dismissGuide(page);
  });

  /* ---- Overview ---- */
  test("Overview tab loads with data", async ({ page }) => {
    await expect(page.getByText("Pipeline Health")).toBeVisible({ timeout: 10000 });
    await expect(page.getByText("Warehouse")).toBeVisible();
    await expect(page.getByText("Quick Actions")).toBeVisible();
    // Stat cards — verify any stat number is visible (like "8" for tables count)
    await expect(page.getByText("CONNECTORS")).toBeVisible();
  });

  /* ---- Editor ---- */
  test("Editor tab shows file tree", async ({ page }) => {
    await goTab(page, "Editor");
    const sidebar = page.locator(`[data-dp-guide="sidebar"]`);
    await expect(sidebar).toBeVisible();
    await expect(sidebar.getByText("transform")).toBeVisible({ timeout: 5000 });
    await expect(sidebar.getByText("ingest")).toBeVisible();
  });

  test("Editor can open a SQL file", async ({ page }) => {
    await goTab(page, "Editor");
    const sidebar = page.locator(`[data-dp-guide="sidebar"]`);
    // Click directly on a SQL file visible in the tree
    await sidebar.getByText("earthquakes.sql").first().click({ timeout: 10000 });
    await page.waitForTimeout(1000);
    await expect(page.locator(".monaco-editor")).toBeVisible({ timeout: 10000 });
  });

  /* ---- Query ---- */
  test("Query tab has Run button and editor", async ({ page }) => {
    await goTab(page, "Query");
    await page.waitForTimeout(1000);
    // The panel area should have a Run button
    await expect(page.getByRole("button", { name: /Run/ }).first()).toBeVisible({ timeout: 10000 });
  });

  /* ---- Tables ---- */
  test("Tables tab shows schemas and tables", async ({ page }) => {
    await goTab(page, "Tables");
    await expect(page.getByText("bronze").first()).toBeVisible({ timeout: 5000 });
    await expect(page.getByText("gold").first()).toBeVisible();
    await expect(page.getByText("silver").first()).toBeVisible();
  });

  test("Tables tab can display table details", async ({ page }) => {
    await goTab(page, "Tables");
    await page.getByText("top_earthquakes").first().click();
    await page.waitForTimeout(1000);
    await expect(page.getByText("event_id").first()).toBeVisible({ timeout: 5000 });
  });

  /* ---- Data Sources ---- */
  test("Data Sources tab loads", async ({ page }) => {
    await goTab(page, "Data Sources");
    await page.waitForTimeout(500);
    // Should show one of the data source cards
    await expect(page.getByText("Upload File")).toBeVisible({ timeout: 5000 });
  });

  /* ---- Notebooks ---- */
  test("Notebooks tab lists available notebooks", async ({ page }) => {
    await goSecondary(page, "Notebooks");
    await expect(page.getByText("Earthquake Ingestion")).toBeVisible({ timeout: 5000 });
    await expect(page.getByText("Earthquake Explorer")).toBeVisible();
  });

  test("Notebooks can open and display cells", async ({ page }) => {
    await goSecondary(page, "Notebooks");
    await page.getByText("Earthquake Explorer").click();
    await page.waitForTimeout(1000);
    await expect(page.getByRole("button", { name: /Run All/ })).toBeVisible({ timeout: 5000 });
    await expect(page.getByText("Back")).toBeVisible();
  });

  /* ---- DAG ---- */
  test("DAG tab renders canvas with Basic/Full toggle", async ({ page }) => {
    await goSecondary(page, "DAG");
    await expect(page.getByText("Basic")).toBeVisible({ timeout: 5000 });
    await expect(page.getByText("Full")).toBeVisible();
    await expect(page.getByText("Rewind")).toBeVisible();
    await expect(page.locator("canvas")).toBeVisible();

    // Switch to Full DAG
    await page.getByText("Full").click();
    await page.waitForTimeout(500);
    await expect(page.locator("canvas")).toBeVisible();
  });

  test("DAG Rewind mode activates", async ({ page }) => {
    await goSecondary(page, "DAG");
    await page.getByText("Rewind").click();
    await page.waitForTimeout(500);
    await expect(page.getByText("Pipeline Rewind")).toBeVisible({ timeout: 5000 });
    await expect(page.locator("canvas")).toBeVisible();
  });

  /* ---- Sentinel ---- */
  test("Sentinel tab shows schema check UI", async ({ page }) => {
    await goSecondary(page, "Sentinel");
    await expect(page.getByText("Schema Sentinel")).toBeVisible({ timeout: 5000 });
    await expect(page.getByRole("button", { name: "Run Schema Check" })).toBeVisible();
  });

  test("Sentinel check executes", async ({ page }) => {
    await goSecondary(page, "Sentinel");
    await page.getByRole("button", { name: "Run Schema Check" }).click();
    await expect(page.getByText(/source\(s\) checked/)).toBeVisible({ timeout: 15000 });
  });

  /* ---- Diff ---- */
  test("Diff tab loads with Run Diff button", async ({ page }) => {
    await goSecondary(page, "Diff");
    await expect(page.getByText("Data Diff")).toBeVisible({ timeout: 5000 });
    await expect(page.getByRole("button", { name: "Run Diff" })).toBeVisible();
  });

  /* ---- Quality ---- */
  test("Quality tab shows sub-tabs and summary cards", async ({ page }) => {
    await goSecondary(page, "Quality");
    await expect(page.getByText("Freshness")).toBeVisible({ timeout: 5000 });
    await expect(page.getByText("Profiles")).toBeVisible();
    await expect(page.getByText("Assertions")).toBeVisible();
    await expect(page.getByText("Contracts", { exact: true })).toBeVisible();
    await expect(page.getByText("Total Models")).toBeVisible();
  });

  test("Quality Freshness shows model data", async ({ page }) => {
    await goSecondary(page, "Quality");
    await expect(page.getByText("bronze.earthquakes")).toBeVisible({ timeout: 10000 });
  });

  test("Quality Profiles shows data", async ({ page }) => {
    await goSecondary(page, "Quality");
    await page.getByText("Profiles", { exact: true }).click();
    await page.waitForTimeout(500);
    await expect(page.getByText("bronze.earthquakes")).toBeVisible({ timeout: 10000 });
  });

  test("Quality Assertions shows results", async ({ page }) => {
    await goSecondary(page, "Quality");
    await page.getByText("Assertions", { exact: true }).click();
    await page.waitForTimeout(500);
    await expect(page.getByText("PASS").first()).toBeVisible({ timeout: 10000 });
  });

  test("Quality Contracts has Run button", async ({ page }) => {
    await goSecondary(page, "Quality");
    await page.getByText("Contracts", { exact: true }).click();
    await page.waitForTimeout(500);
    await expect(page.getByRole("button", { name: "Run Contracts" })).toBeVisible({ timeout: 5000 });
  });

  /* ---- Masking ---- */
  test("Masking tab loads with Add Policy button", async ({ page }) => {
    await goSecondary(page, "Masking");
    await expect(page.getByText("Data Masking Policies")).toBeVisible({ timeout: 5000 });
    await expect(page.getByRole("button", { name: /Add Policy/ })).toBeVisible();
  });

  /* ---- Wiki ---- */
  test("Wiki tab shows categories and pages", async ({ page }) => {
    await goSecondary(page, "Wiki");
    // Wiki sidebar shows search input and page links
    await expect(page.getByPlaceholder("Search pages...")).toBeVisible({ timeout: 5000 });
    await expect(page.getByText("Getting Started").first()).toBeVisible();
  });

  /* ---- Docs ---- */
  test("Docs tab shows schema documentation", async ({ page }) => {
    await goSecondary(page, "Docs");
    await expect(page.getByText(/\d+ tables/)).toBeVisible({ timeout: 10000 });
  });

  /* ---- History ---- */
  test("History tab shows pipeline runs", async ({ page }) => {
    await goSecondary(page, "History");
    await expect(page.getByText("Run History")).toBeVisible({ timeout: 5000 });
    await expect(page.getByText("success").first()).toBeVisible({ timeout: 5000 });
  });

  /* ---- Settings ---- */
  test("Settings tab loads all sections", async ({ page }) => {
    await goSecondary(page, "Settings");
    await expect(page.getByRole("heading", { name: "Theme" })).toBeVisible({ timeout: 5000 });
    await expect(page.getByRole("heading", { name: /Secrets/ })).toBeVisible();
  });

  /* ---- Action bar ---- */
  test("Action bar has Run/Transform/Lint/Contract buttons", async ({ page }) => {
    const actions = page.locator(`[data-dp-guide="actions"]`);
    await expect(actions).toBeVisible();
    await expect(actions.getByRole("button", { name: /Run/ }).first()).toBeVisible();
    await expect(actions.getByRole("button", { name: /Transform/ })).toBeVisible();
    await expect(actions.getByRole("button", { name: /Lint/ })).toBeVisible();
    await expect(actions.getByRole("button", { name: /Contract/ })).toBeVisible();
  });

  /* ---- Cross-tab navigation ---- */
  test("Quick Actions navigate to correct tabs", async ({ page }) => {
    await expect(page.getByText("Quick Actions")).toBeVisible({ timeout: 10000 });
    await page.getByText("Run a Query").click();
    await page.waitForTimeout(500);
    // Should now be on Query tab
    await expect(page.locator(`[data-dp-tab][data-dp-active="true"]`).getByText("Query")).toBeVisible({ timeout: 5000 });

    await goTab(page, "Overview");
    await page.waitForTimeout(500);
    await page.getByText("View DAG").click();
    await page.waitForTimeout(500);
    await expect(page.locator("canvas")).toBeVisible({ timeout: 5000 });
  });

  /* ---- Error sweep ---- */
  test("No uncaught JS errors across all tabs", async ({ page }) => {
    const errors: string[] = [];
    page.on("pageerror", (err) => errors.push(err.message));

    for (const tab of ["Overview", "Editor", "Query", "Tables", "Data Sources"]) {
      await goTab(page, tab);
      await page.waitForTimeout(400);
    }

    for (const tab of [
      "Notebooks", "DAG", "Sentinel", "Diff", "Quality",
      "Masking", "Wiki", "Docs", "History", "Settings",
    ]) {
      await goSecondary(page, tab);
      await page.waitForTimeout(400);
    }

    expect(errors).toEqual([]);
  });
});
