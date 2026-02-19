import React, { useState, useEffect, useRef, useCallback } from "react";
import { useActiveHint } from "./HintSystem";

/**
 * Contextual hint card — renders the currently active hint near its target element.
 * Positioned via a `target` CSS selector (data-dp-hint attribute).
 * Falls back to bottom-right corner if target not found.
 */
export default function Hint({ onNavigate }) {
  const activeHint = useActiveHint();
  const [pos, setPos] = useState(null);
  const [visible, setVisible] = useState(false);
  const cardRef = useRef(null);

  const computePosition = useCallback(() => {
    if (!activeHint) {
      setVisible(false);
      return;
    }

    const target = activeHint.target
      ? document.querySelector(activeHint.target)
      : null;

    if (!target) {
      // Fall back to bottom-right corner
      setPos({
        position: "fixed",
        bottom: 72,
        right: 16,
        top: "auto",
        left: "auto",
      });
      setVisible(true);
      return;
    }

    const rect = target.getBoundingClientRect();
    const cardW = 300;
    const cardH = 120;
    const gap = 10;
    const vw = window.innerWidth;
    const vh = window.innerHeight;

    // Try placing below the target
    let top = rect.bottom + gap;
    let left = rect.left + rect.width / 2 - cardW / 2;

    // If it would go below viewport, place above
    if (top + cardH > vh - 60) {
      top = rect.top - cardH - gap;
    }

    // Clamp horizontal
    left = Math.max(8, Math.min(vw - cardW - 8, left));
    // Clamp vertical
    top = Math.max(8, Math.min(vh - cardH - 60, top));

    setPos({
      position: "fixed",
      top,
      left,
      bottom: "auto",
      right: "auto",
    });
    setVisible(true);
  }, [activeHint]);

  useEffect(() => {
    computePosition();
  }, [computePosition]);

  useEffect(() => {
    if (!activeHint) return;
    window.addEventListener("resize", computePosition);
    window.addEventListener("scroll", computePosition, true);
    return () => {
      window.removeEventListener("resize", computePosition);
      window.removeEventListener("scroll", computePosition, true);
    };
  }, [activeHint, computePosition]);

  if (!activeHint || !pos) return null;

  const handleAction = () => {
    if (activeHint.action?.navigate && onNavigate) {
      onNavigate(activeHint.action.navigate);
    }
    if (activeHint.action?.callback) {
      activeHint.action.callback();
    }
    activeHint.dismiss();
  };

  return (
    <div
      ref={cardRef}
      style={{
        ...styles.card,
        ...pos,
        opacity: visible ? 1 : 0,
        transform: visible ? "translateY(0)" : "translateY(6px)",
      }}
    >
      <div style={styles.content}>
        <span style={styles.icon}>*</span>
        <p style={styles.text}>{activeHint.text}</p>
      </div>
      <div style={styles.buttons}>
        {activeHint.action?.label && (
          <button onClick={handleAction} style={styles.primaryBtn}>
            {activeHint.action.label}
          </button>
        )}
        <button onClick={() => activeHint.dismiss()} style={styles.dismissBtn}>
          Got it
        </button>
      </div>
    </div>
  );
}

const styles = {
  card: {
    maxWidth: 300,
    background: "var(--dp-bg-secondary)",
    border: "1px solid var(--dp-border)",
    borderRadius: "var(--dp-radius-lg, 8px)",
    padding: "12px 14px",
    boxShadow: "0 4px 16px rgba(0,0,0,0.15)",
    zIndex: 9000,
    transition: "opacity 0.2s ease, transform 0.2s ease",
    pointerEvents: "auto",
  },
  content: {
    display: "flex",
    gap: "8px",
    alignItems: "flex-start",
    marginBottom: "10px",
  },
  icon: {
    flexShrink: 0,
    width: "18px",
    height: "18px",
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    borderRadius: "50%",
    background: "var(--dp-bg-tertiary)",
    color: "var(--dp-accent)",
    fontSize: "12px",
    fontWeight: 700,
    marginTop: "1px",
  },
  text: {
    margin: 0,
    fontSize: "12px",
    lineHeight: 1.5,
    color: "var(--dp-text-secondary)",
  },
  buttons: {
    display: "flex",
    gap: "6px",
    justifyContent: "flex-end",
  },
  primaryBtn: {
    padding: "4px 12px",
    background: "var(--dp-accent)",
    border: "none",
    borderRadius: "var(--dp-radius, 4px)",
    color: "var(--dp-bg)",
    cursor: "pointer",
    fontSize: "11px",
    fontWeight: 600,
  },
  dismissBtn: {
    padding: "4px 12px",
    background: "none",
    border: "1px solid var(--dp-border-light)",
    borderRadius: "var(--dp-radius, 4px)",
    color: "var(--dp-text-dim)",
    cursor: "pointer",
    fontSize: "11px",
  },
};
