import { describe, it, expect, beforeEach, vi, afterEach } from "vitest";
import React from "react";
import { render, screen, act, fireEvent } from "@testing-library/react";
import { HintProvider, useHint, useHintTriggerFn, useHintSettings, useActiveHint } from "./HintSystem";
import { HINTS } from "./hints";

// Helper component that exposes hint state for testing
function HintTestHarness({ triggers = {}, hintId, children }) {
  return (
    <HintProvider>
      <TriggerSetter triggers={triggers} />
      {hintId && <HintChecker hintId={hintId} />}
      <ActiveHintChecker />
      <ResetButton />
      {children}
    </HintProvider>
  );
}

function TriggerSetter({ triggers }) {
  const setTrigger = useHintTriggerFn();
  React.useEffect(() => {
    for (const [key, value] of Object.entries(triggers)) {
      setTrigger(key, value);
    }
  }, [triggers, setTrigger]);
  return null;
}

function HintChecker({ hintId }) {
  const { visible, dismiss } = useHint(hintId);
  return (
    <div>
      <span data-testid="hint-visible">{String(visible)}</span>
      <button data-testid="dismiss-btn" onClick={dismiss}>Dismiss</button>
    </div>
  );
}

function ActiveHintChecker() {
  const hint = useActiveHint();
  return (
    <div>
      <span data-testid="active-hint-id">{hint?.id || "none"}</span>
      <span data-testid="active-hint-text">{hint?.text || ""}</span>
    </div>
  );
}

function ResetButton() {
  const { resetHints, totalHints, dismissedCount } = useHintSettings();
  return (
    <div>
      <span data-testid="total-hints">{totalHints}</span>
      <span data-testid="dismissed-count">{dismissedCount}</span>
      <button data-testid="reset-btn" onClick={resetHints}>Reset</button>
    </div>
  );
}

describe("HintSystem", () => {
  beforeEach(() => {
    localStorage.clear();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("shows no hint when no conditions are met", () => {
    render(<HintTestHarness triggers={{}} />);
    expect(screen.getByTestId("active-hint-id")).toHaveTextContent("none");
  });

  it("shows hint when condition is met and not dismissed", () => {
    render(
      <HintTestHarness
        triggers={{ pipelineJustCompleted: true }}
        hintId="first-pipeline-complete"
      />
    );
    expect(screen.getByTestId("hint-visible")).toHaveTextContent("true");
    expect(screen.getByTestId("active-hint-id")).toHaveTextContent("first-pipeline-complete");
  });

  it("does not show hint when condition is not met", () => {
    render(
      <HintTestHarness
        triggers={{ pipelineJustCompleted: false }}
        hintId="first-pipeline-complete"
      />
    );
    expect(screen.getByTestId("hint-visible")).toHaveTextContent("false");
  });

  it("does not show hint when already dismissed", () => {
    localStorage.setItem("dp_dismissed_hints", JSON.stringify(["first-pipeline-complete"]));

    render(
      <HintTestHarness
        triggers={{ pipelineJustCompleted: true }}
        hintId="first-pipeline-complete"
      />
    );
    expect(screen.getByTestId("hint-visible")).toHaveTextContent("false");
  });

  it("dismisses hint permanently on dismiss click", () => {
    render(
      <HintTestHarness
        triggers={{ pipelineJustCompleted: true }}
        hintId="first-pipeline-complete"
      />
    );

    expect(screen.getByTestId("hint-visible")).toHaveTextContent("true");

    act(() => {
      fireEvent.click(screen.getByTestId("dismiss-btn"));
    });

    expect(screen.getByTestId("hint-visible")).toHaveTextContent("false");
    const dismissed = JSON.parse(localStorage.getItem("dp_dismissed_hints"));
    expect(dismissed).toContain("first-pipeline-complete");
  });

  it("shows only one hint at a time (highest priority)", () => {
    render(
      <HintTestHarness
        triggers={{
          pipelineJustCompleted: true, // priority 5
          firstFileEdited: true,       // priority 8
          dagOpened: true,             // priority 10
        }}
      />
    );
    expect(screen.getByTestId("active-hint-id")).toHaveTextContent("first-pipeline-complete");
  });

  it("auto-dismisses after timeout", () => {
    render(
      <HintTestHarness
        triggers={{ dagOpened: true }}
        hintId="dag-intro"
      />
    );

    expect(screen.getByTestId("hint-visible")).toHaveTextContent("true");

    act(() => {
      vi.advanceTimersByTime(16000);
    });

    expect(screen.getByTestId("hint-visible")).toHaveTextContent("false");
  });

  it("reset clears all dismissed hints", () => {
    localStorage.setItem("dp_dismissed_hints", JSON.stringify(["first-pipeline-complete", "dag-intro"]));

    render(
      <HintTestHarness triggers={{}} />
    );

    expect(screen.getByTestId("dismissed-count")).toHaveTextContent("2");

    act(() => {
      fireEvent.click(screen.getByTestId("reset-btn"));
    });

    expect(screen.getByTestId("dismissed-count")).toHaveTextContent("0");
    expect(localStorage.getItem("dp_dismissed_hints")).toBeNull();
  });

  it("reports correct total and dismissed counts", () => {
    localStorage.setItem("dp_dismissed_hints", JSON.stringify(["dag-intro", "first-editor-save", "query-panel-intro"]));

    render(<HintTestHarness triggers={{}} />);

    expect(screen.getByTestId("total-hints")).toHaveTextContent(String(HINTS.length));
    expect(screen.getByTestId("dismissed-count")).toHaveTextContent("3");
  });

  it("repeatable hint uses timestamp cooldown instead of permanent dismiss", () => {
    render(
      <HintTestHarness
        triggers={{ connectorStale: true }}
        hintId="connector-stale"
      />
    );

    expect(screen.getByTestId("hint-visible")).toHaveTextContent("true");

    act(() => {
      fireEvent.click(screen.getByTestId("dismiss-btn"));
    });

    expect(screen.getByTestId("hint-visible")).toHaveTextContent("false");

    // Should NOT be in dismissed_hints (repeatable uses timestamps)
    const dismissed = JSON.parse(localStorage.getItem("dp_dismissed_hints") || "[]");
    expect(dismissed).not.toContain("connector-stale");

    // Should have a timestamp stored
    const timestamps = JSON.parse(localStorage.getItem("dp_hint_stale_timestamps") || "{}");
    expect(timestamps["connector-stale"]).toBeDefined();
  });

  it("queue shows next hint after first is dismissed", () => {
    render(
      <HintTestHarness
        triggers={{
          pipelineJustCompleted: true, // priority 5
          dagOpened: true,             // priority 10
        }}
        hintId="first-pipeline-complete"
      />
    );

    // First: highest priority (pipeline complete, priority 5)
    expect(screen.getByTestId("active-hint-id")).toHaveTextContent("first-pipeline-complete");

    act(() => {
      fireEvent.click(screen.getByTestId("dismiss-btn"));
    });

    // After dismiss: next in queue (dag intro, priority 10)
    expect(screen.getByTestId("active-hint-id")).toHaveTextContent("dag-intro");
  });

  it("condition with multiple flags works correctly", () => {
    render(
      <HintTestHarness
        triggers={{ queryPanelOpened: true, warehouseHasTables: false }}
        hintId="query-panel-intro"
      />
    );
    expect(screen.getByTestId("hint-visible")).toHaveTextContent("false");
  });

  it("condition with multiple flags both true shows hint", () => {
    render(
      <HintTestHarness
        triggers={{ queryPanelOpened: true, warehouseHasTables: true }}
        hintId="query-panel-intro"
      />
    );
    expect(screen.getByTestId("hint-visible")).toHaveTextContent("true");
  });

  it("keyboard shortcut hint does not trigger below threshold", () => {
    render(
      <HintTestHarness
        triggers={{ tabSwitchCount: 4 }}
        hintId="keyboard-shortcuts"
      />
    );
    expect(screen.getByTestId("hint-visible")).toHaveTextContent("false");
  });

  it("keyboard shortcut hint shows when tabSwitchCount reaches 5", () => {
    render(
      <HintTestHarness
        triggers={{ tabSwitchCount: 5 }}
        hintId="keyboard-shortcuts"
      />
    );
    expect(screen.getByTestId("hint-visible")).toHaveTextContent("true");
  });
});

describe("hints.js definitions", () => {
  it("all hints have required fields", () => {
    for (const hint of HINTS) {
      expect(hint.id).toBeTruthy();
      expect(hint.text).toBeTruthy();
      expect(typeof hint.text).toBe("string");
      expect(hint.text.length).toBeLessThanOrEqual(150);
      expect(typeof hint.priority).toBe("number");
      expect(typeof hint.condition).toBe("function");
    }
  });

  it("all hint IDs are unique", () => {
    const ids = HINTS.map((h) => h.id);
    expect(new Set(ids).size).toBe(ids.length);
  });

  it("contains all 12 expected hints", () => {
    expect(HINTS.length).toBe(12);
    const expectedIds = [
      "first-pipeline-complete",
      "first-editor-save",
      "query-panel-intro",
      "first-connector-done",
      "connector-stale",
      "git-detected",
      "uncommitted-changes",
      "diff-has-changes",
      "dag-intro",
      "tables-click-columns",
      "keyboard-shortcuts",
      "overview-no-runs",
    ];
    for (const id of expectedIds) {
      expect(HINTS.find((h) => h.id === id)).toBeTruthy();
    }
  });

  it("only connector-stale is repeatable", () => {
    const repeatableHints = HINTS.filter((h) => h.repeatable);
    expect(repeatableHints.length).toBe(1);
    expect(repeatableHints[0].id).toBe("connector-stale");
  });
});
