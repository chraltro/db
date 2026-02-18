import React, { useState, useEffect, useCallback } from "react";

export default function GuideTour({ steps, onComplete, isOpen }) {
  const [currentStep, setCurrentStep] = useState(0);
  const [highlightRect, setHighlightRect] = useState(null);
  const [tooltipPos, setTooltipPos] = useState({ top: 0, left: 0 });

  const step = steps[currentStep];

  const computePositions = useCallback(() => {
    if (!isOpen || !step) return;

    if (step.position === "center") {
      setHighlightRect(null);
      setTooltipPos({
        top: "50%",
        left: "50%",
        transform: "translate(-50%, -50%)",
      });
      return;
    }

    const el = document.querySelector(`[data-dp-guide="${step.id}"]`);
    if (!el) {
      setHighlightRect(null);
      setTooltipPos({ top: "50%", left: "50%", transform: "translate(-50%, -50%)" });
      return;
    }

    const rect = el.getBoundingClientRect();
    const pad = 6;
    const hr = {
      top: rect.top - pad,
      left: rect.left - pad,
      width: rect.width + pad * 2,
      height: rect.height + pad * 2,
    };
    setHighlightRect(hr);

    // Position tooltip relative to highlighted area
    const cardW = 300;
    const cardH = 180;
    const gap = 14;
    let t = 0, l = 0;

    switch (step.position) {
      case "right":
        t = hr.top + hr.height / 2 - cardH / 2;
        l = hr.left + hr.width + gap;
        break;
      case "left":
        t = hr.top + hr.height / 2 - cardH / 2;
        l = hr.left - cardW - gap;
        break;
      case "bottom":
        t = hr.top + hr.height + gap;
        l = hr.left + hr.width / 2 - cardW / 2;
        break;
      case "top":
        t = hr.top - cardH - gap;
        l = hr.left + hr.width / 2 - cardW / 2;
        break;
      default:
        t = hr.top + hr.height + gap;
        l = hr.left;
    }

    // Clamp to viewport
    const vw = window.innerWidth;
    const vh = window.innerHeight;
    t = Math.max(16, Math.min(vh - cardH - 16, t));
    l = Math.max(16, Math.min(vw - cardW - 16, l));

    setTooltipPos({ top: t, left: l });
  }, [isOpen, step]);

  useEffect(() => {
    computePositions();
  }, [computePositions, currentStep]);

  useEffect(() => {
    if (!isOpen) return;
    window.addEventListener("resize", computePositions);
    return () => window.removeEventListener("resize", computePositions);
  }, [isOpen, computePositions]);

  // Keyboard navigation
  useEffect(() => {
    if (!isOpen) return;
    function handleKey(e) {
      if (e.key === "Escape") {
        onComplete();
      } else if (e.key === "ArrowRight" || e.key === "Enter") {
        if (currentStep < steps.length - 1) {
          setCurrentStep((s) => s + 1);
        } else {
          onComplete();
        }
      } else if (e.key === "ArrowLeft") {
        if (currentStep > 0) setCurrentStep((s) => s - 1);
      }
    }
    window.addEventListener("keydown", handleKey);
    return () => window.removeEventListener("keydown", handleKey);
  }, [isOpen, currentStep, steps.length, onComplete]);

  // Reset step when opening
  useEffect(() => {
    if (isOpen) setCurrentStep(0);
  }, [isOpen]);

  if (!isOpen || !step) return null;

  return (
    <div style={styles.overlay}>
      {/* Spotlight cutout */}
      {highlightRect ? (
        <div
          style={{
            ...styles.spotlight,
            top: highlightRect.top,
            left: highlightRect.left,
            width: highlightRect.width,
            height: highlightRect.height,
          }}
        />
      ) : (
        <div style={styles.dimBackground} />
      )}

      {/* Tooltip card */}
      <div
        style={{
          ...styles.card,
          ...(typeof tooltipPos.transform === "string"
            ? { top: tooltipPos.top, left: tooltipPos.left, transform: tooltipPos.transform }
            : { top: tooltipPos.top, left: tooltipPos.left }),
        }}
      >
        <div style={styles.stepCounter}>
          {currentStep + 1} of {steps.length}
        </div>
        <h3 style={styles.title}>{step.title}</h3>
        <p style={styles.description}>{step.description}</p>
        <div style={styles.buttons}>
          <button onClick={onComplete} style={styles.btnSkip}>
            Skip
          </button>
          {currentStep > 0 && (
            <button onClick={() => setCurrentStep((s) => s - 1)} style={styles.btnSecondary}>
              Previous
            </button>
          )}
          <button
            onClick={() => {
              if (currentStep < steps.length - 1) {
                setCurrentStep((s) => s + 1);
              } else {
                onComplete();
              }
            }}
            style={styles.btnPrimary}
          >
            {currentStep === steps.length - 1 ? "Done" : "Next"}
          </button>
        </div>
      </div>
    </div>
  );
}

const styles = {
  overlay: {
    position: "fixed",
    inset: 0,
    zIndex: 9999,
    pointerEvents: "none",
  },
  spotlight: {
    position: "fixed",
    borderRadius: "var(--dp-radius-lg, 8px)",
    boxShadow: "0 0 0 9999px rgba(0, 0, 0, 0.55)",
    pointerEvents: "none",
    transition: "all 0.3s ease",
    zIndex: 9999,
  },
  dimBackground: {
    position: "fixed",
    inset: 0,
    background: "rgba(0, 0, 0, 0.55)",
    pointerEvents: "none",
    zIndex: 9999,
  },
  card: {
    position: "fixed",
    background: "var(--dp-bg-secondary)",
    border: "1px solid var(--dp-border-light)",
    borderRadius: "var(--dp-radius-lg, 8px)",
    padding: "18px 22px",
    width: "300px",
    boxShadow: "0 8px 32px rgba(0, 0, 0, 0.35)",
    zIndex: 10000,
    pointerEvents: "auto",
    transition: "none",
  },
  stepCounter: {
    fontSize: "10px",
    color: "var(--dp-text-dim)",
    fontWeight: 600,
    letterSpacing: "0.5px",
    textTransform: "uppercase",
    marginBottom: "8px",
  },
  title: {
    fontSize: "15px",
    fontWeight: 600,
    color: "var(--dp-text)",
    margin: "0 0 6px",
  },
  description: {
    fontSize: "13px",
    color: "var(--dp-text-secondary)",
    lineHeight: 1.6,
    margin: "0 0 16px",
  },
  buttons: {
    display: "flex",
    gap: "8px",
    justifyContent: "flex-end",
  },
  btnPrimary: {
    padding: "6px 16px",
    background: "var(--dp-accent)",
    border: "none",
    borderRadius: "var(--dp-radius, 4px)",
    color: "var(--dp-bg)",
    cursor: "pointer",
    fontSize: "12px",
    fontWeight: 600,
  },
  btnSecondary: {
    padding: "6px 14px",
    background: "var(--dp-btn-bg)",
    border: "1px solid var(--dp-btn-border)",
    borderRadius: "var(--dp-radius, 4px)",
    color: "var(--dp-text)",
    cursor: "pointer",
    fontSize: "12px",
  },
  btnSkip: {
    padding: "6px 14px",
    background: "none",
    border: "none",
    color: "var(--dp-text-dim)",
    cursor: "pointer",
    fontSize: "12px",
    marginRight: "auto",
  },
};
