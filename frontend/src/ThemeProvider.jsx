import React, { createContext, useContext, useState, useEffect } from "react";
import { COLOR_THEMES, FONT_THEMES, DEFAULT_COLOR_THEME, DEFAULT_FONT_THEME, getComposedTheme } from "./themes";

const ThemeContext = createContext({
  colorThemeId: DEFAULT_COLOR_THEME,
  fontThemeId: DEFAULT_FONT_THEME,
  setColorThemeId: () => {},
  setFontThemeId: () => {},
  // Legacy compat
  themeId: DEFAULT_COLOR_THEME,
  setThemeId: () => {},
});

export function useTheme() {
  return useContext(ThemeContext);
}

function applyTheme(colorId, fontId) {
  const composed = getComposedTheme(colorId, fontId);
  const root = document.documentElement;
  for (const [prop, value] of Object.entries(composed.vars)) {
    root.style.setProperty(prop, value);
  }
  root.style.setProperty("color-scheme", composed.dark ? "dark" : "light");
}

export default function ThemeProvider({ children }) {
  const [colorThemeId, setColorThemeId] = useState(() => {
    // Migrate from old single-theme storage
    const legacy = localStorage.getItem("dp_theme");
    const saved = localStorage.getItem("havn_color_theme");
    return saved || legacy || DEFAULT_COLOR_THEME;
  });

  const [fontThemeId, setFontThemeId] = useState(() => {
    return localStorage.getItem("havn_font_theme") || DEFAULT_FONT_THEME;
  });

  useEffect(() => {
    applyTheme(colorThemeId, fontThemeId);
    localStorage.setItem("havn_color_theme", colorThemeId);
    localStorage.setItem("havn_font_theme", fontThemeId);
  }, [colorThemeId, fontThemeId]);

  // Legacy setThemeId sets color theme
  const setThemeId = setColorThemeId;

  return (
    <ThemeContext.Provider value={{
      colorThemeId,
      fontThemeId,
      setColorThemeId,
      setFontThemeId,
      themeId: colorThemeId,
      setThemeId,
    }}>
      {children}
    </ThemeContext.Provider>
  );
}
