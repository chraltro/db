import React, { createContext, useContext, useState, useEffect } from "react";
import { THEMES, DEFAULT_THEME, getTheme } from "./themes";

const ThemeContext = createContext({
  themeId: DEFAULT_THEME,
  setThemeId: () => {},
});

export function useTheme() {
  return useContext(ThemeContext);
}

function applyTheme(id) {
  const theme = getTheme(id);
  const root = document.documentElement;
  for (const [prop, value] of Object.entries(theme.vars)) {
    root.style.setProperty(prop, value);
  }
  // Set color-scheme for native elements (scrollbars, inputs)
  root.style.setProperty("color-scheme", theme.dark ? "dark" : "light");
}

export default function ThemeProvider({ children }) {
  const [themeId, setThemeId] = useState(() => {
    return localStorage.getItem("dp_theme") || DEFAULT_THEME;
  });

  useEffect(() => {
    applyTheme(themeId);
    localStorage.setItem("dp_theme", themeId);
  }, [themeId]);

  return (
    <ThemeContext.Provider value={{ themeId, setThemeId }}>
      {children}
    </ThemeContext.Provider>
  );
}
