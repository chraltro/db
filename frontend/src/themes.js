/**
 * Theme definitions for havn.
 *
 * Color themes and font themes are independent — mix and match freely.
 * ThemeProvider composes the active color + font into CSS custom properties.
 */

// ---------------------------------------------------------------------------
// Color themes (backgrounds, borders, accents, status colors, radius)
// ---------------------------------------------------------------------------

export const COLOR_THEMES = {
  "havn-dark": {
    name: "havn Dark",
    description: "Harbour Teal on deep ocean",
    dark: true,
    vars: {
      "--havn-bg": "#0B0E14",
      "--havn-bg-secondary": "#121620",
      "--havn-bg-tertiary": "#080A0F",
      "--havn-border": "#1C2233",
      "--havn-border-light": "#263044",
      "--havn-text": "#D0D6E0",
      "--havn-text-secondary": "#6B7A90",
      "--havn-text-dim": "#3A4560",
      "--havn-accent": "#3ECFB4",
      "--havn-green": "#34D399",
      "--havn-green-border": "#3ECFB4",
      "--havn-red": "#F87171",
      "--havn-yellow": "#FBBF24",
      "--havn-purple": "#7C8CF5",
      "--havn-radius": "6px",
      "--havn-radius-lg": "10px",
      "--havn-btn-bg": "#1C2233",
      "--havn-btn-border": "#263044",
    },
  },
  "havn-light": {
    name: "havn Light",
    description: "Harbour Teal on Nordic grey",
    dark: false,
    vars: {
      "--havn-bg": "#F4F6F8",
      "--havn-bg-secondary": "#E8ECF0",
      "--havn-bg-tertiary": "#FFFFFF",
      "--havn-border": "#D0D7DF",
      "--havn-border-light": "#BCC5D0",
      "--havn-text": "#0B0E14",
      "--havn-text-secondary": "#5A6878",
      "--havn-text-dim": "#94A0B0",
      "--havn-accent": "#2BA88E",
      "--havn-green": "#22A06B",
      "--havn-green-border": "#2BA88E",
      "--havn-red": "#DC2626",
      "--havn-yellow": "#B45309",
      "--havn-purple": "#5B6AD0",
      "--havn-radius": "6px",
      "--havn-radius-lg": "10px",
      "--havn-btn-bg": "#E8ECF0",
      "--havn-btn-border": "#D0D7DF",
    },
  },
  "midnight-terminal": {
    name: "Midnight Terminal",
    description: "Green-on-black terminal aesthetic",
    dark: true,
    vars: {
      "--havn-bg": "#0c0e14",
      "--havn-bg-secondary": "#11131a",
      "--havn-bg-tertiary": "#0a0c11",
      "--havn-border": "#1a1e2e",
      "--havn-border-light": "#252a3a",
      "--havn-text": "#c8cdd8",
      "--havn-text-secondary": "#636b80",
      "--havn-text-dim": "#3a4058",
      "--havn-accent": "#6ee7b7",
      "--havn-green": "#34d399",
      "--havn-green-border": "#4ade80",
      "--havn-red": "#fb7185",
      "--havn-yellow": "#fbbf24",
      "--havn-purple": "#a78bfa",
      "--havn-radius": "4px",
      "--havn-radius-lg": "6px",
      "--havn-btn-bg": "#1a1e2e",
      "--havn-btn-border": "#252a3a",
    },
  },
  "paper-light": {
    name: "Paper Light",
    description: "Warm editorial light theme",
    dark: false,
    vars: {
      "--havn-bg": "#faf8f5",
      "--havn-bg-secondary": "#f0ece6",
      "--havn-bg-tertiary": "#fff",
      "--havn-border": "#ddd7cc",
      "--havn-border-light": "#ccc5b8",
      "--havn-text": "#2c2418",
      "--havn-text-secondary": "#8a7e6e",
      "--havn-text-dim": "#b8ad9c",
      "--havn-accent": "#c45d3e",
      "--havn-green": "#558b44",
      "--havn-green-border": "#6aa858",
      "--havn-red": "#c44040",
      "--havn-yellow": "#b08825",
      "--havn-purple": "#7c5ab8",
      "--havn-radius": "2px",
      "--havn-radius-lg": "4px",
      "--havn-btn-bg": "#ebe5dc",
      "--havn-btn-border": "#ddd7cc",
    },
  },
  "electric": {
    name: "Electric",
    description: "Modern indigo-accented dark theme",
    dark: true,
    vars: {
      "--havn-bg": "#09090b",
      "--havn-bg-secondary": "#121216",
      "--havn-bg-tertiary": "#0d0d10",
      "--havn-border": "#1f1f2e",
      "--havn-border-light": "#2a2a3e",
      "--havn-text": "#e4e4f0",
      "--havn-text-secondary": "#6b6b8a",
      "--havn-text-dim": "#3d3d55",
      "--havn-accent": "#818cf8",
      "--havn-green": "#4ade80",
      "--havn-green-border": "#5eead4",
      "--havn-red": "#f87171",
      "--havn-yellow": "#facc15",
      "--havn-purple": "#c084fc",
      "--havn-radius": "8px",
      "--havn-radius-lg": "12px",
      "--havn-btn-bg": "#1f1f2e",
      "--havn-btn-border": "#2a2a3e",
    },
  },
  "nordic-frost": {
    name: "Nordic Frost",
    description: "Cool professional slate-blue light theme",
    dark: false,
    vars: {
      "--havn-bg": "#e8ecf1",
      "--havn-bg-secondary": "#dce2e9",
      "--havn-bg-tertiary": "#f2f4f7",
      "--havn-border": "#c4cdd8",
      "--havn-border-light": "#b4bfcc",
      "--havn-text": "#1e2a3a",
      "--havn-text-secondary": "#5a6a7e",
      "--havn-text-dim": "#94a3b8",
      "--havn-accent": "#2563eb",
      "--havn-green": "#16a34a",
      "--havn-green-border": "#22c55e",
      "--havn-red": "#dc2626",
      "--havn-yellow": "#ca8a04",
      "--havn-purple": "#7c3aed",
      "--havn-radius": "6px",
      "--havn-radius-lg": "10px",
      "--havn-btn-bg": "#dce2e9",
      "--havn-btn-border": "#c4cdd8",
    },
  },
  "ember": {
    name: "Ember",
    description: "Warm dark theme with amber highlights",
    dark: true,
    vars: {
      "--havn-bg": "#140e0a",
      "--havn-bg-secondary": "#1c1410",
      "--havn-bg-tertiary": "#110c08",
      "--havn-border": "#2e2218",
      "--havn-border-light": "#3d2e20",
      "--havn-text": "#e8ddd0",
      "--havn-text-secondary": "#8a7560",
      "--havn-text-dim": "#4e3d2e",
      "--havn-accent": "#f59e0b",
      "--havn-green": "#84cc16",
      "--havn-green-border": "#a3e635",
      "--havn-red": "#ef4444",
      "--havn-yellow": "#eab308",
      "--havn-purple": "#d97706",
      "--havn-radius": "3px",
      "--havn-radius-lg": "5px",
      "--havn-btn-bg": "#2e2218",
      "--havn-btn-border": "#3d2e20",
    },
  },
  "corporate": {
    name: "Corporate",
    description: "Deliberately vanilla, muted blue, zero personality",
    dark: false,
    vars: {
      "--havn-bg": "#f5f5f5",
      "--havn-bg-secondary": "#eaeaea",
      "--havn-bg-tertiary": "#fff",
      "--havn-border": "#d4d4d4",
      "--havn-border-light": "#c0c0c0",
      "--havn-text": "#333",
      "--havn-text-secondary": "#777",
      "--havn-text-dim": "#aaa",
      "--havn-accent": "#4a7cbc",
      "--havn-green": "#5a9a5a",
      "--havn-green-border": "#6ab06a",
      "--havn-red": "#c05050",
      "--havn-yellow": "#b09030",
      "--havn-purple": "#7868a8",
      "--havn-radius": "4px",
      "--havn-radius-lg": "6px",
      "--havn-btn-bg": "#e0e0e0",
      "--havn-btn-border": "#d4d4d4",
    },
  },
};

// ---------------------------------------------------------------------------
// Font themes (display + monospace pairings)
// ---------------------------------------------------------------------------

export const FONT_THEMES = {
  "outfit-jetbrains": {
    name: "Outfit + JetBrains Mono",
    description: "Clean geometric sans with technical mono",
    vars: {
      "--havn-font": "'Outfit', -apple-system, BlinkMacSystemFont, sans-serif",
      "--havn-font-mono": "'JetBrains Mono', monospace",
    },
  },
  "ibm-plex": {
    name: "IBM Plex Sans + Plex Mono",
    description: "Neutral humanist with matched mono",
    vars: {
      "--havn-font": "'IBM Plex Sans', -apple-system, BlinkMacSystemFont, sans-serif",
      "--havn-font-mono": "'IBM Plex Mono', 'Fira Code', monospace",
    },
  },
  "sora-fira": {
    name: "Sora + Fira Code",
    description: "Geometric display with ligature-rich mono",
    vars: {
      "--havn-font": "'Sora', -apple-system, BlinkMacSystemFont, sans-serif",
      "--havn-font-mono": "'Fira Code', monospace",
    },
  },
  "manrope-jetbrains": {
    name: "Manrope + JetBrains Mono",
    description: "Friendly rounded sans with technical mono",
    vars: {
      "--havn-font": "'Manrope', -apple-system, BlinkMacSystemFont, sans-serif",
      "--havn-font-mono": "'JetBrains Mono', monospace",
    },
  },
  "dm-sans-space": {
    name: "DM Sans + Space Mono",
    description: "Compact geometric with retro-flavored mono",
    vars: {
      "--havn-font": "'DM Sans', -apple-system, BlinkMacSystemFont, sans-serif",
      "--havn-font-mono": "'Space Mono', monospace",
    },
  },
  "crimson-jetbrains": {
    name: "Crimson Pro + JetBrains Mono",
    description: "Editorial serif with technical mono",
    vars: {
      "--havn-font": "'Crimson Pro', 'Source Serif 4', Georgia, serif",
      "--havn-font-mono": "'JetBrains Mono', 'Fira Code', monospace",
    },
  },
  "system": {
    name: "System Default",
    description: "Native OS fonts for maximum performance",
    vars: {
      "--havn-font": "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
      "--havn-font-mono": "Consolas, 'Courier New', monospace",
    },
  },
};

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

export const DEFAULT_COLOR_THEME = "havn-dark";
export const DEFAULT_FONT_THEME = "outfit-jetbrains";

// ---------------------------------------------------------------------------
// Backward-compatible API (composed themes)
// ---------------------------------------------------------------------------

/** Get a composed theme (color + font merged) for a given color/font pair. */
export function getComposedTheme(colorId, fontId) {
  const color = COLOR_THEMES[colorId] || COLOR_THEMES[DEFAULT_COLOR_THEME];
  const font = FONT_THEMES[fontId] || FONT_THEMES[DEFAULT_FONT_THEME];
  return {
    name: color.name,
    description: color.description,
    dark: color.dark,
    vars: { ...color.vars, ...font.vars },
  };
}

// Legacy compat — THEMES as composed objects using default font
export const THEMES = Object.fromEntries(
  Object.entries(COLOR_THEMES).map(([id, color]) => [
    id,
    { ...color, vars: { ...color.vars, ...FONT_THEMES[DEFAULT_FONT_THEME].vars } },
  ])
);

export const DEFAULT_THEME = DEFAULT_COLOR_THEME;

export function getThemeIds() {
  return Object.keys(COLOR_THEMES);
}

export function getTheme(id) {
  return THEMES[id] || THEMES[DEFAULT_THEME];
}

export function getColorThemeIds() {
  return Object.keys(COLOR_THEMES);
}

export function getFontThemeIds() {
  return Object.keys(FONT_THEMES);
}
