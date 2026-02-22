import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import ThemeProvider from "./ThemeProvider";
import { HintProvider } from "./HintSystem";
import { AuthProvider } from "./AuthContext";

ReactDOM.createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <ThemeProvider>
      <HintProvider>
        <AuthProvider>
          <App />
        </AuthProvider>
      </HintProvider>
    </ThemeProvider>
  </React.StrictMode>,
);
