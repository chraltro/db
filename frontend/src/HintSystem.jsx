import React, { createContext, useContext, useState, useCallback, useEffect, useRef } from "react";
import { HINTS } from "./hints";

const STORAGE_KEY = "dp_dismissed_hints";
const STALE_STORAGE_KEY = "dp_hint_stale_timestamps";

function loadDismissed() {
  try {
    return JSON.parse(localStorage.getItem(STORAGE_KEY) || "[]");
  } catch {
    return [];
  }
}

function saveDismissed(ids) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(ids));
}

function loadStaleTimestamps() {
  try {
    return JSON.parse(localStorage.getItem(STALE_STORAGE_KEY) || "{}");
  } catch {
    return {};
  }
}

function saveStaleTimestamps(ts) {
  localStorage.setItem(STALE_STORAGE_KEY, JSON.stringify(ts));
}

const HintContext = createContext(null);

export function HintProvider({ children }) {
  const [dismissed, setDismissed] = useState(loadDismissed);
  const [hintState, setHintState] = useState({});
  const [activeHintId, setActiveHintId] = useState(null);
  const autoTimerRef = useRef(null);

  // Evaluate which hint should be active based on current state
  const evaluateHints = useCallback(() => {
    const now = Date.now();
    const staleTs = loadStaleTimestamps();

    // Find all eligible hints sorted by priority (lower = higher priority)
    const eligible = HINTS
      .filter((hint) => {
        // Check if dismissed (non-repeatable)
        if (!hint.repeatable && dismissed.includes(hint.id)) return false;

        // Check repeatable hints (stale connector) — 7-day cooldown
        if (hint.repeatable && staleTs[hint.id]) {
          const cooldownMs = 7 * 24 * 60 * 60 * 1000;
          if (now - staleTs[hint.id] < cooldownMs) return false;
        }

        // Check condition
        if (!hint.condition) return false;
        return hint.condition(hintState);
      })
      .sort((a, b) => a.priority - b.priority);

    const next = eligible.length > 0 ? eligible[0].id : null;
    setActiveHintId((prev) => {
      if (prev === next) return prev;
      return next;
    });
  }, [hintState, dismissed]);

  useEffect(() => {
    evaluateHints();
  }, [evaluateHints]);

  // Auto-dismiss timer
  useEffect(() => {
    if (autoTimerRef.current) {
      clearTimeout(autoTimerRef.current);
      autoTimerRef.current = null;
    }
    if (!activeHintId) return;

    const hint = HINTS.find((h) => h.id === activeHintId);
    const timeout = hint?.autoDismissMs ?? 15000;

    autoTimerRef.current = setTimeout(() => {
      dismiss(activeHintId);
    }, timeout);

    return () => {
      if (autoTimerRef.current) {
        clearTimeout(autoTimerRef.current);
        autoTimerRef.current = null;
      }
    };
  }, [activeHintId]);

  const dismiss = useCallback((id) => {
    const hint = HINTS.find((h) => h.id === id);
    if (hint?.repeatable) {
      // Store timestamp instead of permanent dismiss
      const ts = loadStaleTimestamps();
      ts[id] = Date.now();
      saveStaleTimestamps(ts);
    } else {
      setDismissed((prev) => {
        const next = prev.includes(id) ? prev : [...prev, id];
        saveDismissed(next);
        return next;
      });
    }
    setActiveHintId((prev) => (prev === id ? null : prev));
  }, []);

  const setTrigger = useCallback((key, value) => {
    setHintState((prev) => {
      if (prev[key] === value) return prev;
      return { ...prev, [key]: value };
    });
  }, []);

  const resetHints = useCallback(() => {
    localStorage.removeItem(STORAGE_KEY);
    localStorage.removeItem(STALE_STORAGE_KEY);
    setDismissed([]);
    setActiveHintId(null);
  }, []);

  const totalHints = HINTS.length;
  const dismissedCount = dismissed.length;

  const ctx = {
    activeHintId,
    dismissed,
    hintState,
    setTrigger,
    dismiss,
    resetHints,
    totalHints,
    dismissedCount,
  };

  return (
    <HintContext.Provider value={ctx}>
      {children}
    </HintContext.Provider>
  );
}

/**
 * Hook: returns { visible, dismiss, hint } for a given hint ID.
 * Components use this to check if their hint should be shown.
 */
export function useHint(id) {
  const ctx = useContext(HintContext);
  if (!ctx) return { visible: false, dismiss: () => {}, hint: null };

  const visible = ctx.activeHintId === id;
  const hint = HINTS.find((h) => h.id === id);

  return {
    visible,
    dismiss: () => ctx.dismiss(id),
    hint,
  };
}

/**
 * Hook: sets a flag in the shared hint state.
 * Components call this to signal conditions.
 */
export function useHintTrigger(key, value) {
  const ctx = useContext(HintContext);
  useEffect(() => {
    if (ctx) ctx.setTrigger(key, value);
  }, [ctx, key, value]);
}

/**
 * Hook: returns the setTrigger function for imperative use.
 */
export function useHintTriggerFn() {
  const ctx = useContext(HintContext);
  return ctx ? ctx.setTrigger : () => {};
}

/**
 * Hook: returns resetHints and hint counts for Settings.
 */
export function useHintSettings() {
  const ctx = useContext(HintContext);
  if (!ctx) return { resetHints: () => {}, totalHints: 0, dismissedCount: 0 };
  return {
    resetHints: ctx.resetHints,
    totalHints: ctx.totalHints,
    dismissedCount: ctx.dismissedCount,
  };
}

/**
 * Hook: returns the active hint for rendering in the Hint component.
 */
export function useActiveHint() {
  const ctx = useContext(HintContext);
  if (!ctx || !ctx.activeHintId) return null;
  const hint = HINTS.find((h) => h.id === ctx.activeHintId);
  if (!hint) return null;
  return {
    ...hint,
    dismiss: () => ctx.dismiss(hint.id),
  };
}
