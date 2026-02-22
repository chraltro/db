import React, { createContext, useContext, useState, useEffect, useCallback } from "react";
import { api, type UserInfo } from "./api";

interface AuthState {
  authChecked: boolean;
  authRequired: boolean;
  needsSetup: boolean;
  currentUser: UserInfo | null;
  handleLogin: (result: { username: string; role?: string }) => void;
  handleLogout: () => void;
  isAuthenticated: boolean;
}

const AuthContext = createContext<AuthState | null>(null);

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [authChecked, setAuthChecked] = useState(false);
  const [authRequired, setAuthRequired] = useState(false);
  const [needsSetup, setNeedsSetup] = useState(false);
  const [currentUser, setCurrentUser] = useState<UserInfo | null>(null);

  const checkAuth = useCallback(async () => {
    try {
      const status = await api.getAuthStatus();
      if (!status.auth_enabled) {
        setAuthChecked(true);
        setCurrentUser({ username: "local", role: "admin", display_name: "Local User" });
        return;
      }
      if (status.needs_setup) {
        setNeedsSetup(true);
        setAuthRequired(true);
        setAuthChecked(true);
        return;
      }
      // Try existing token
      if (api.getToken()) {
        try {
          const me = await api.getMe();
          setCurrentUser(me);
          setAuthChecked(true);
          return;
        } catch {
          api.setToken(null);
        }
      }
      setAuthRequired(true);
      setAuthChecked(true);
    } catch {
      // Server not requiring auth
      setAuthChecked(true);
      setCurrentUser({ username: "local", role: "admin", display_name: "Local User" });
    }
  }, []);

  useEffect(() => {
    checkAuth();
    const handler = () => setAuthRequired(true);
    window.addEventListener("dp_auth_required", handler);
    return () => window.removeEventListener("dp_auth_required", handler);
  }, [checkAuth]);

  const handleLogin = useCallback((result: { username: string; role?: string }) => {
    setAuthRequired(false);
    setNeedsSetup(false);
    setCurrentUser({ username: result.username, role: result.role || "admin" });
  }, []);

  const handleLogout = useCallback(() => {
    api.setToken(null);
    setCurrentUser(null);
    setAuthRequired(true);
  }, []);

  const isAuthenticated = authChecked && !authRequired;

  return (
    <AuthContext.Provider
      value={{ authChecked, authRequired, needsSetup, currentUser, handleLogin, handleLogout, isAuthenticated }}
    >
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth(): AuthState {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within AuthProvider");
  return ctx;
}
