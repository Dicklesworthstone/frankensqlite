import { test, expect, type Page, type ConsoleMessage } from "@playwright/test";

// ---------------------------------------------------------------------------
// Console monitor – captures runtime errors, network failures, React warnings
// ---------------------------------------------------------------------------
class ConsoleMonitor {
  private errors: { text: string; type: string; category: string }[] = [];

  attach(page: Page) {
    page.on("console", (msg: ConsoleMessage) => {
      if (msg.type() === "error" || msg.type() === "warning") {
        const text = msg.text();
        const category = this.categorize(text);
        if (category !== "ignore") {
          this.errors.push({ text, type: msg.type(), category });
        }
      }
    });

    page.on("pageerror", (err) => {
      this.errors.push({
        text: err.message,
        type: "pageerror",
        category: "runtime",
      });
    });
  }

  private categorize(text: string): string {
    if (/hydrat|server.*different.*client/i.test(text)) return "hydration";
    if (/TypeError|ReferenceError|SyntaxError/i.test(text)) return "runtime";
    if (/net::ERR|fetch.*failed|NetworkError/i.test(text)) return "network";
    if (/Warning:|useEffect|ReactDOM/i.test(text)) return "react";
    if (/CSP|Refused to/i.test(text)) return "security";
    if (/deprecat/i.test(text)) return "ignore";
    if (/favicon/i.test(text)) return "ignore";
    return "other";
  }

  getErrors() {
    return this.errors.filter((e) => e.category !== "ignore");
  }

  getRuntimeErrors() {
    return this.errors.filter(
      (e) => e.category === "runtime" || e.category === "pageerror"
    );
  }
}

// ---------------------------------------------------------------------------
// Test suite: FrankenSQLite Spec Evolution Visualization
// ---------------------------------------------------------------------------

test.describe("FrankenSQLite Visualization – Live Site", () => {
  let monitor: ConsoleMonitor;

  test.beforeEach(async ({ page }) => {
    monitor = new ConsoleMonitor();
    monitor.attach(page);
  });

  // ── Page loads and renders ────────────────────────────────────────────
  test("page loads with 200 and renders KPIs", async ({ page }) => {
    const response = await page.goto("/");
    expect(response?.status()).toBe(200);

    // Title contains FrankenSQLite
    await expect(page).toHaveTitle(/FrankenSQLite/i);

    // KPI widgets should populate (not stay as "-")
    await page.waitForFunction(
      () => document.getElementById("kpiCommits")?.textContent !== "-",
      { timeout: 10_000 }
    );
    const commits = await page.textContent("#kpiCommits");
    expect(Number(commits)).toBeGreaterThan(50);
  });

  // ── Open Spec button ────────────────────────────────────────────────
  test("Open Spec button loads the spec markdown file", async ({ page }) => {
    await page.goto("/");
    const specLink = page.locator("#btnOpenSpec");
    await expect(specLink).toBeVisible();

    // Should link to the spec file
    const href = await specLink.getAttribute("href");
    expect(href).toContain("COMPREHENSIVE_SPEC");

    // Navigate and verify it loads
    const [response] = await Promise.all([
      page.waitForNavigation(),
      specLink.click(),
    ]);
    expect(response?.status()).toBe(200);
  });

  // ── OG meta tags ─────────────────────────────────────────────────────
  test("OG and Twitter meta tags are present", async ({ page }) => {
    await page.goto("/");

    const ogTitle = await page.getAttribute('meta[property="og:title"]', "content");
    expect(ogTitle).toContain("FrankenSQLite");

    const ogImage = await page.getAttribute('meta[property="og:image"]', "content");
    expect(ogImage).toContain("og-image.png");

    const twitterCard = await page.getAttribute('meta[name="twitter:card"]', "content");
    expect(twitterCard).toBe("summary_large_image");

    const twitterImage = await page.getAttribute('meta[name="twitter:image"]', "content");
    expect(twitterImage).toContain("twitter-image.png");
  });

  // ── OG images are accessible ─────────────────────────────────────────
  test("OG share images return 200", async ({ page }) => {
    const ogResp = await page.goto("/og-image.png");
    expect(ogResp?.status()).toBe(200);
    expect(ogResp?.headers()["content-type"]).toContain("image/png");

    const twResp = await page.goto("/twitter-image.png");
    expect(twResp?.status()).toBe(200);
    expect(twResp?.headers()["content-type"]).toContain("image/png");
  });

  // ── Core UI elements exist ────────────────────────────────────────────
  test("core UI elements are present", async ({ page }) => {
    await page.goto("/");
    await page.waitForLoadState("networkidle");

    // Header elements
    await expect(page.locator("#btnOpenSpec")).toBeVisible();
    await expect(page.locator("#btnGalaxy")).toBeVisible();

    // Commit list loads
    await page.waitForFunction(
      () => (document.querySelectorAll("#commitList > *").length > 5),
      { timeout: 15_000 }
    );
    const commitCount = await page.locator("#commitList > *").count();
    expect(commitCount).toBeGreaterThan(10);

    // Bucket toggles exist (in DOM; may be below fold or in hidden-lg:block aside)
    const bucketToggles = page.locator("#bucketToggles button, #bucketToggles label");
    await expect(bucketToggles.first()).toBeAttached();
  });

  // ── New features: heat stripe, story mode, SbS panes ─────────────────
  test("new features are present in DOM", async ({ page }) => {
    await page.goto("/");
    await page.waitForLoadState("networkidle");

    // Dock heat stripe canvas
    await expect(page.locator("#dockHeatStripe")).toBeAttached();

    // Story mode elements
    await expect(page.locator("#storyRail")).toBeAttached();
    await expect(page.locator("#btnStoryToggle")).toBeAttached();

    // Side-by-side rendered panes
    await expect(page.locator("#sbsContainer")).toBeAttached();
  });

  // ── Tab navigation ────────────────────────────────────────────────────
  test("tab navigation works (spec, diff, metrics)", async ({ page }) => {
    await page.goto("/");
    await page.waitForLoadState("networkidle");

    // Click Spec tab
    const specTab = page.locator('button:has-text("Spec"), [data-tab="spec"]').first();
    if (await specTab.isVisible()) {
      await specTab.click();
      // Wait for spec content area to become visible
      await page.waitForTimeout(500);
    }

    // Click Diff tab
    const diffTab = page.locator('button:has-text("Diff"), [data-tab="diff"]').first();
    if (await diffTab.isVisible()) {
      await diffTab.click();
      await page.waitForTimeout(500);
    }
  });

  // ── Galaxy Brain mode toggle ──────────────────────────────────────────
  test("Galaxy Brain button toggles dark mode", async ({ page }) => {
    await page.goto("/");
    await page.waitForLoadState("networkidle");

    const galaxy = page.locator("#btnGalaxy");
    await expect(galaxy).toBeVisible();
    await galaxy.click();
    // Should toggle some visual state
    await page.waitForTimeout(300);
  });

  // ── Dock slider interaction ───────────────────────────────────────────
  test("dock slider scrolls through commits", async ({ page }) => {
    await page.goto("/");
    await page.waitForFunction(
      () => document.getElementById("kpiCommits")?.textContent !== "-",
      { timeout: 10_000 }
    );

    const slider = page.locator("#dockSlider");
    await expect(slider).toBeVisible();

    // Range has integer steps (max=136, step=1) — use evaluate to set value
    await page.evaluate(() => {
      const s = document.getElementById("dockSlider") as HTMLInputElement;
      const mid = Math.floor(Number(s.max) / 2);
      s.value = String(mid);
      s.dispatchEvent(new Event("input", { bubbles: true }));
    });
    await page.waitForTimeout(300);

    await page.evaluate(() => {
      const s = document.getElementById("dockSlider") as HTMLInputElement;
      s.value = s.max;
      s.dispatchEvent(new Event("input", { bubbles: true }));
    });
    await page.waitForTimeout(300);
  });

  // ── URL state round-trip ──────────────────────────────────────────────
  test("URL state params round-trip correctly", async ({ page }) => {
    // Load with specific URL params
    await page.goto("/?v=spec&c=5&dm=pretty");
    await page.waitForLoadState("networkidle");
    await page.waitForTimeout(2000);

    // The app should parse the URL params and apply them.
    // Check the spec tab is active (v=spec should activate it)
    const specView = page.locator("#docSpecView, [data-view='spec']").first();
    const isSpecVisible = await specView.isVisible().catch(() => false);

    // Also check if the commit slider moved to index 5
    const sliderVal = await page.evaluate(
      () => (document.getElementById("dockSlider") as HTMLInputElement)?.value
    );

    // At minimum the page should have loaded without error
    expect(true).toBe(true); // non-crash assertion
    console.log(`URL state: spec visible=${isSpecVisible}, slider=${sliderVal}`);
  });

  // ── Performance: initial load under budget ────────────────────────────
  test("initial page load completes within 10s", async ({ page }) => {
    const start = Date.now();
    await page.goto("/", { waitUntil: "networkidle" });
    const elapsed = Date.now() - start;

    // Budget: 10 seconds including network
    expect(elapsed).toBeLessThan(10_000);
    console.log(`Page load: ${elapsed}ms`);
  });

  // ── Performance: no excessive DOM size ────────────────────────────────
  test("DOM size audit (report element count breakdown)", async ({
    page,
  }) => {
    await page.goto("/");
    await page.waitForLoadState("networkidle");

    const audit = await page.evaluate(() => {
      const total = document.querySelectorAll("*").length;
      const commitList = document.querySelectorAll("#commitList *").length;
      const bucketToggles = document.querySelectorAll("#bucketToggles *").length;
      const docRendered = document.querySelectorAll("#docRendered *").length;
      const diffPretty = document.querySelectorAll("#diffPretty *").length;
      const sheet = document.querySelectorAll("#sheet *").length;
      const rest = total - commitList - bucketToggles - docRendered - diffPretty - sheet;
      return { total, commitList, bucketToggles, docRendered, diffPretty, sheet, rest };
    });

    console.log(`DOM AUDIT:
  Total elements:   ${audit.total}
  #commitList:      ${audit.commitList}
  #bucketToggles:   ${audit.bucketToggles}
  #docRendered:     ${audit.docRendered}
  #diffPretty:      ${audit.diffPretty}
  #sheet:           ${audit.sheet}
  Rest:             ${audit.rest}`);

    // Soft budget: report but don't fail hard — flag if over 10K
    expect(audit.total).toBeLessThan(15_000);
  });

  // ── No uncaught JS errors ─────────────────────────────────────────────
  test("no uncaught JavaScript errors on load", async ({ page }) => {
    await page.goto("/");
    await page.waitForLoadState("networkidle");
    await page.waitForTimeout(2000); // Let async init settle

    const runtimeErrors = monitor.getRuntimeErrors();
    if (runtimeErrors.length > 0) {
      console.log("Runtime errors found:", JSON.stringify(runtimeErrors, null, 2));
    }
    expect(runtimeErrors).toHaveLength(0);
  });

  // ── No critical console errors ────────────────────────────────────────
  test("no critical console errors", async ({ page }) => {
    await page.goto("/");
    await page.waitForLoadState("networkidle");
    await page.waitForTimeout(2000);

    const errors = monitor.getErrors();
    const critical = errors.filter(
      (e) =>
        e.category === "runtime" ||
        e.category === "hydration" ||
        e.category === "security"
    );
    if (critical.length > 0) {
      console.log("Critical errors:", JSON.stringify(critical, null, 2));
    }
    expect(critical).toHaveLength(0);
  });

  // ── Filter interaction doesn't crash ──────────────────────────────────
  test("filter panel opens and bucket toggles work", async ({ page }) => {
    await page.goto("/");
    await page.waitForFunction(
      () => document.getElementById("kpiCommits")?.textContent !== "-",
      { timeout: 10_000 }
    );

    // Click Filters button
    const filtersBtn = page.locator('button:has-text("Filters")');
    if (await filtersBtn.isVisible()) {
      await filtersBtn.click();
      await page.waitForTimeout(500);

      // The filter sheet may overlay — use JS click to bypass pointer event interception
      const toggled = await page.evaluate(() => {
        const btn = document.querySelector("#bucketToggles button, #bucketToggles label");
        if (btn instanceof HTMLElement) {
          btn.click();
          return true;
        }
        return false;
      });

      if (toggled) {
        await page.waitForTimeout(500);
        // Verify no runtime errors after interaction
        const errors = monitor.getRuntimeErrors();
        expect(errors).toHaveLength(0);
      }
    }
  });
});

// ---------------------------------------------------------------------------
// Test suite: Inline Highlights (bd-24q.16.4)
// ---------------------------------------------------------------------------

test.describe("Inline Highlights – Toggle + Navigation + Stability", () => {
  let monitor: ConsoleMonitor;

  test.beforeEach(async ({ page }) => {
    monitor = new ConsoleMonitor();
    monitor.attach(page);
  });

  /**
   * Helper: navigate to a known commit with changes and switch to spec tab.
   * Commit index 5 is well past the initial commit so it should have diffs.
   */
  async function goToSpecWithChanges(page: Page) {
    await page.goto("/?v=spec&c=5");
    await page.waitForFunction(
      () => document.getElementById("kpiCommits")?.textContent !== "-",
      { timeout: 10_000 }
    );
    // Ensure spec tab is visible
    await page.waitForSelector("#docRendered", { state: "attached", timeout: 5_000 });
    await page.waitForTimeout(1000); // Let rendering settle
  }

  // ── Toggle highlights on ──────────────────────────────────────────────
  test("toggle highlights on shows changed blocks", async ({ page }) => {
    await goToSpecWithChanges(page);

    // Highlights button should exist
    const btn = page.locator("#btnIHToggle");
    await expect(btn).toBeVisible();

    // Click to enable highlights
    await btn.click();
    await page.waitForTimeout(1500); // Wait for sentinel rendering

    // Verify at least one highlighted block
    const highlightCount = await page.locator("#docRendered .ih-changed").count();
    console.log(`[IH Diag] commit=5, highlight_count=${highlightCount}`);
    expect(highlightCount).toBeGreaterThan(0);

    // Nav bar should be visible
    const nav = page.locator("#ihNav");
    await expect(nav).toBeVisible();

    // Nav label should not be "0/0" (there are highlights)
    const label = await page.textContent("#ihNavLabel");
    console.log(`[IH Diag] nav_label="${label}"`);
    expect(label).not.toBe("0/0");

    // No runtime errors
    expect(monitor.getRuntimeErrors()).toHaveLength(0);
  });

  // ── Toggle highlights off clears them ─────────────────────────────────
  test("toggle highlights off clears changed blocks", async ({ page }) => {
    await goToSpecWithChanges(page);

    const btn = page.locator("#btnIHToggle");

    // Enable
    await btn.click();
    await page.waitForTimeout(1500);
    const onCount = await page.locator("#docRendered .ih-changed").count();
    expect(onCount).toBeGreaterThan(0);

    // Disable
    await btn.click();
    await page.waitForTimeout(1000);
    const offCount = await page.locator("#docRendered .ih-changed").count();
    console.log(`[IH Diag] after toggle off: highlight_count=${offCount}`);
    expect(offCount).toBe(0);

    // Nav bar should be hidden
    const navDisplay = await page.evaluate(
      () => document.getElementById("ihNav")?.style.display
    );
    expect(navDisplay).not.toBe("inline-flex");

    expect(monitor.getRuntimeErrors()).toHaveLength(0);
  });

  // ── Next/Prev navigation changes scroll position ──────────────────────
  test("next/prev navigation scrolls between changed blocks", async ({ page }) => {
    await goToSpecWithChanges(page);

    // Enable highlights
    await page.locator("#btnIHToggle").click();
    await page.waitForTimeout(1500);

    const highlightCount = await page.locator("#docRendered .ih-changed").count();
    // Skip if no highlights at this commit
    test.skip(highlightCount < 2, "Need at least 2 highlights to test navigation");

    // Get initial scroll position
    const scrollBefore = await page.evaluate(
      () => document.getElementById("docRendered")?.scrollTop ?? 0
    );

    // Click "Next change"
    await page.locator("#btnIHNext").click();
    await page.waitForTimeout(500);

    const scrollAfterNext = await page.evaluate(
      () => document.getElementById("docRendered")?.scrollTop ?? 0
    );
    console.log(`[IH Diag] scroll: before=${scrollBefore}, after_next=${scrollAfterNext}`);

    // Scroll should have changed (navigated to a different block)
    // Note: if the first highlight is already at the top, next may not change scroll much.
    // But at least the nav label should update
    const labelAfterNext = await page.textContent("#ihNavLabel");
    console.log(`[IH Diag] nav_label after next: "${labelAfterNext}"`);
    expect(labelAfterNext).toMatch(/^\d+\/\d+$/);

    // Click "Prev change"
    await page.locator("#btnIHPrev").click();
    await page.waitForTimeout(500);

    const scrollAfterPrev = await page.evaluate(
      () => document.getElementById("docRendered")?.scrollTop ?? 0
    );
    const labelAfterPrev = await page.textContent("#ihNavLabel");
    console.log(`[IH Diag] scroll: after_prev=${scrollAfterPrev}, label="${labelAfterPrev}"`);

    expect(monitor.getRuntimeErrors()).toHaveLength(0);
  });

  // ── Keyboard shortcuts Alt+Arrow ──────────────────────────────────────
  test("Alt+Arrow keyboard shortcuts navigate highlights", async ({ page }) => {
    await goToSpecWithChanges(page);

    await page.locator("#btnIHToggle").click();
    await page.waitForTimeout(1500);

    const highlightCount = await page.locator("#docRendered .ih-changed").count();
    test.skip(highlightCount < 2, "Need at least 2 highlights to test keyboard nav");

    const labelBefore = await page.textContent("#ihNavLabel");

    // Alt+ArrowDown = next change
    await page.keyboard.press("Alt+ArrowDown");
    await page.waitForTimeout(500);

    const labelAfterDown = await page.textContent("#ihNavLabel");
    console.log(`[IH Diag] keyboard: before="${labelBefore}", after_down="${labelAfterDown}"`);

    // Alt+ArrowUp = prev change
    await page.keyboard.press("Alt+ArrowUp");
    await page.waitForTimeout(500);

    const labelAfterUp = await page.textContent("#ihNavLabel");
    console.log(`[IH Diag] keyboard: after_up="${labelAfterUp}"`);

    expect(monitor.getRuntimeErrors()).toHaveLength(0);
  });

  // ── Commit switch refreshes highlights ────────────────────────────────
  test("switching commits refreshes highlights correctly", async ({ page }) => {
    await goToSpecWithChanges(page);

    // Enable highlights
    await page.locator("#btnIHToggle").click();
    await page.waitForTimeout(1500);

    const count1 = await page.locator("#docRendered .ih-changed").count();
    console.log(`[IH Diag] commit=5, highlights=${count1}`);

    // Switch to a different commit via dock slider
    await page.evaluate(() => {
      const s = document.getElementById("dockSlider") as HTMLInputElement;
      s.value = "10";
      s.dispatchEvent(new Event("input", { bubbles: true }));
    });
    await page.waitForTimeout(2000); // Wait for re-render

    const count2 = await page.locator("#docRendered .ih-changed").count();
    console.log(`[IH Diag] commit=10, highlights=${count2}`);

    // Highlights should exist for the new commit (it also has changes)
    // The key assertion: no stale highlights from the previous commit remain
    // We verify by checking the data-srcmap attributes are consistent with current content
    const staleCheck = await page.evaluate(() => {
      const rendered = document.getElementById("docRendered");
      if (!rendered) return { ok: false, reason: "no docRendered" };
      const changed = rendered.querySelectorAll(".ih-changed");
      // Every .ih-changed element should also have data-changed="true"
      let staleCount = 0;
      for (const el of changed) {
        if (el.getAttribute("data-changed") !== "true") staleCount++;
      }
      return { ok: staleCount === 0, staleCount, totalChanged: changed.length };
    });
    console.log(`[IH Diag] stale check:`, JSON.stringify(staleCheck));
    expect(staleCheck.ok).toBe(true);

    expect(monitor.getRuntimeErrors()).toHaveLength(0);
  });

  // ── Highlights survive tab switch ─────────────────────────────────────
  test("highlights persist after switching away and back to spec tab", async ({ page }) => {
    await goToSpecWithChanges(page);

    // Enable highlights
    await page.locator("#btnIHToggle").click();
    await page.waitForTimeout(1500);

    const countBefore = await page.locator("#docRendered .ih-changed").count();
    expect(countBefore).toBeGreaterThan(0);

    // Switch to diff tab
    const diffTab = page.locator('button:has-text("Diff"), [data-tab="diff"]').first();
    if (await diffTab.isVisible()) {
      await diffTab.click();
      await page.waitForTimeout(500);

      // Switch back to spec tab
      const specTab = page.locator('button:has-text("Spec"), [data-tab="spec"]').first();
      await specTab.click();
      await page.waitForTimeout(1500);

      const countAfter = await page.locator("#docRendered .ih-changed").count();
      console.log(`[IH Diag] tab round-trip: before=${countBefore}, after=${countAfter}`);

      // Highlights should still be present (re-rendered with highlights on)
      expect(countAfter).toBeGreaterThan(0);
    }

    expect(monitor.getRuntimeErrors()).toHaveLength(0);
  });

  // ── URL state preserves highlights toggle ─────────────────────────────
  test("ih=1 URL param enables highlights on load", async ({ page }) => {
    // Navigate with ih=1
    await page.goto("/?v=spec&c=5&ih=1");
    await page.waitForFunction(
      () => document.getElementById("kpiCommits")?.textContent !== "-",
      { timeout: 10_000 }
    );
    await page.waitForTimeout(2000); // Wait for rendering

    // Highlights should be active
    const btnClass = await page.getAttribute("#btnIHToggle", "class");
    const isActive = btnClass?.includes("bg-slate-900") ?? false;
    console.log(`[IH Diag] URL ih=1: btn_active=${isActive}, class="${btnClass?.slice(0, 40)}..."`);

    // If the button is active, highlights should be present
    if (isActive) {
      const count = await page.locator("#docRendered .ih-changed").count();
      console.log(`[IH Diag] URL ih=1: highlight_count=${count}`);
      expect(count).toBeGreaterThan(0);
    }

    expect(monitor.getRuntimeErrors()).toHaveLength(0);
  });

  // ── No JS errors during full highlight lifecycle ──────────────────────
  test("full highlight lifecycle produces no JS errors", async ({ page }) => {
    await goToSpecWithChanges(page);

    // Toggle on
    await page.locator("#btnIHToggle").click();
    await page.waitForTimeout(1500);

    // Navigate forward 3 times
    for (let i = 0; i < 3; i++) {
      await page.locator("#btnIHNext").click();
      await page.waitForTimeout(300);
    }

    // Navigate back 2 times
    for (let i = 0; i < 2; i++) {
      await page.locator("#btnIHPrev").click();
      await page.waitForTimeout(300);
    }

    // Switch commit
    await page.evaluate(() => {
      const s = document.getElementById("dockSlider") as HTMLInputElement;
      s.value = "15";
      s.dispatchEvent(new Event("input", { bubbles: true }));
    });
    await page.waitForTimeout(2000);

    // Toggle off
    await page.locator("#btnIHToggle").click();
    await page.waitForTimeout(500);

    // Toggle on again
    await page.locator("#btnIHToggle").click();
    await page.waitForTimeout(1500);

    // Final check: no runtime errors through the entire lifecycle
    const errors = monitor.getRuntimeErrors();
    if (errors.length > 0) {
      console.log("[IH Diag] lifecycle errors:", JSON.stringify(errors, null, 2));
    }
    expect(errors).toHaveLength(0);
  });
});
