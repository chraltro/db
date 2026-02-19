import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import ThemeProvider from "./ThemeProvider";
import { HintProvider } from "./HintSystem";

ReactDOM.createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <ThemeProvider>
      <HintProvider>
        <App />
      </HintProvider>
    </ThemeProvider>
  </React.StrictMode>,
);
