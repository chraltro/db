import React, { createContext, useContext, useState, useCallback } from "react";
import { api, type OutputEntry, type RunSummary } from "./api";

interface PipelineState {
  running: boolean;
  output: OutputEntry[];
  runSummary: RunSummary | null;
  addOutput: (type: OutputEntry["type"], message: string) => void;
  clearOutput: () => void;
  setRunSummary: (summary: RunSummary | null) => void;
  runTransformAll: (force?: boolean) => Promise<void>;
  runStream: (name: string, force?: boolean) => Promise<void>;
  runLint: (fix?: boolean) => Promise<void>;
  runCurrentScript: (scriptPath: string) => Promise<void>;
  runSingleModel: (modelName: string) => Promise<void>;
  runContracts: () => Promise<void>;
}

const PipelineContext = createContext<PipelineState | null>(null);

interface PipelineProviderProps {
  children: React.ReactNode;
  onTablesChanged: () => void;
  onPipelineComplete?: () => void;
}

export function PipelineProvider({ children, onTablesChanged, onPipelineComplete }: PipelineProviderProps) {
  const [running, setRunning] = useState(false);
  const [output, setOutput] = useState<OutputEntry[]>([]);
  const [runSummary, setRunSummary] = useState<RunSummary | null>(null);

  const addOutput = useCallback((type: OutputEntry["type"], message: string) => {
    const ts = new Date().toLocaleTimeString();
    setOutput((prev) => [...prev, { type, message, ts }]);
  }, []);

  const clearOutput = useCallback(() => setOutput([]), []);

  const runTransformAll = useCallback(async (force: boolean = false) => {
    setRunning(true);
    setRunSummary(null);
    addOutput("info", `Running transform (force=${force})...`);
    try {
      const data = await api.runTransform(null, force);
      const models: { name: string; result: string }[] = [];
      for (const [model, status] of Object.entries(data.results || {})) {
        addOutput(status === "error" ? "error" : "info", `${model}: ${status}`);
        models.push({ name: model, result: status });
      }
      onTablesChanged();

      const summary: RunSummary = {
        type: "transform",
        status: models.some((m) => m.result === "error") ? "failed" : "success",
        models,
        totalRows: 0,
        duration: 0,
        errors: models.filter((m) => m.result === "error").length,
      };
      setRunSummary(summary);
      if (summary.status === "success") onPipelineComplete?.();
    } catch (e: unknown) {
      addOutput("error", (e as Error).message);
    } finally {
      setRunning(false);
    }
  }, [addOutput, onTablesChanged, onPipelineComplete]);

  const runStream = useCallback(async (name: string, force: boolean = false) => {
    setRunning(true);
    setRunSummary(null);
    addOutput("info", `Running pipeline${force ? " (full refresh)" : ""}...`);
    try {
      const data = await api.runStream(name, force);
      const models: { name: string; result: string }[] = [];
      let totalRows = 0;
      let totalDuration = 0;
      let hasError = false;

      for (const step of data.steps || []) {
        addOutput("info", `--- ${step.action} ---`);
        if (step.action === "transform" || step.action === "seed") {
          const results = step.results as Record<string, string>;
          for (const [model, status] of Object.entries(results || {})) {
            addOutput(status === "error" ? "error" : "info", `${model}: ${status}`);
            models.push({ name: model, result: status });
            if (status === "error") hasError = true;
          }
        } else {
          const results = step.results as Array<{
            script?: string;
            status: string;
            error?: string;
            log_output?: string;
            rows_affected?: number;
            duration_ms?: number;
          }>;
          for (const r of results || []) {
            const label = r.script || step.action;
            const msg = r.status === "error" ? `${label}: error — ${r.error}` : `${label}: success (${r.duration_ms}ms)`;
            addOutput(r.status === "error" ? "error" : "info", msg);
            if (r.log_output?.trim()) {
              r.log_output.split("\n").filter((l: string) => l.trim()).forEach((l: string) => addOutput("log", l.trim()));
            }
            if (r.rows_affected) totalRows += r.rows_affected;
            if (r.duration_ms) totalDuration += r.duration_ms;
            if (r.status === "error") hasError = true;
          }
        }
      }

      const durationS = data.duration_seconds ? data.duration_seconds * 1000 : totalDuration;
      addOutput("info", "Pipeline completed.");
      onTablesChanged();

      const summary: RunSummary = {
        type: "stream",
        status: hasError ? "failed" : "success",
        models,
        totalRows,
        duration: Math.round(durationS),
        errors: models.filter((m) => m.result === "error").length,
      };
      setRunSummary(summary);
      if (!hasError) onPipelineComplete?.();
    } catch (e: unknown) {
      addOutput("error", (e as Error).message);
      setRunSummary({
        type: "stream",
        status: "failed",
        models: [],
        totalRows: 0,
        duration: 0,
        errors: 1,
      });
    } finally {
      setRunning(false);
    }
  }, [addOutput, onTablesChanged, onPipelineComplete]);

  const runLint = useCallback(async (fix: boolean = false) => {
    setRunning(true);
    addOutput("info", fix ? "Fixing SQL..." : "Linting SQL...");
    try {
      const data = await api.runLint(fix);
      for (const v of data.violations || []) {
        const tag = fix && !v.fixable ? " (unfixable)" : "";
        addOutput("warn", `${v.file}:${v.line}:${v.col} [${v.code}] ${v.description}${tag}`);
      }
      if (fix) {
        const fixed = data.fixed ?? 0;
        const remaining = data.count;
        const parts: string[] = [];
        if (fixed > 0) parts.push(`${fixed} fixed`);
        if (remaining > 0) parts.push(`${remaining} violation(s) remain (unfixable by SQLFluff)`);
        addOutput("info", parts.length > 0 ? parts.join(", ") + "." : "All fixable violations resolved.");
      } else {
        addOutput("info", data.count === 0 ? "No lint violations found." : `${data.count} violation(s) found.`);
      }
    } catch (e: unknown) {
      addOutput("error", (e as Error).message);
    } finally {
      setRunning(false);
    }
  }, [addOutput]);

  const runCurrentScript = useCallback(async (scriptPath: string) => {
    setRunning(true);
    addOutput("info", `Running ${scriptPath}...`);
    try {
      const data = await api.runScript(scriptPath);
      addOutput(data.status === "error" ? "error" : "info", `${scriptPath}: ${data.status} (${data.duration_ms}ms)`);
      if (data.log_output) data.log_output.split("\n").filter((l: string) => l.trim()).forEach((l: string) => addOutput("log", l));
      if (data.error) addOutput("error", data.error);
    } catch (e: unknown) {
      addOutput("error", (e as Error).message);
    } finally {
      setRunning(false);
    }
  }, [addOutput]);

  const runSingleModel = useCallback(async (modelName: string) => {
    setRunning(true);
    setRunSummary(null);
    addOutput("info", `Running transform for ${modelName}...`);
    try {
      const data = await api.runTransform([modelName], false);
      const models: { name: string; result: string }[] = [];
      for (const [model, status] of Object.entries(data.results || {})) {
        addOutput(status === "error" ? "error" : "info", `${model}: ${status}`);
        models.push({ name: model, result: status });
      }
      onTablesChanged();
      setRunSummary({
        type: "transform",
        status: models.some((m) => m.result === "error") ? "failed" : "success",
        models,
        totalRows: 0,
        duration: 0,
        errors: models.filter((m) => m.result === "error").length,
      });
    } catch (e: unknown) {
      addOutput("error", (e as Error).message);
    } finally {
      setRunning(false);
    }
  }, [addOutput, onTablesChanged]);

  const runContracts = useCallback(async () => {
    setRunning(true);
    addOutput("info", "Running contracts...");
    try {
      const data = await api.runContracts() as {
        total: number;
        passed: number;
        failed: number;
        results: {
          contract_name: string;
          model: string;
          passed: boolean;
          severity: string;
          duration_ms: number;
          error?: string;
          assertions: { expression: string; passed: boolean; detail: string }[];
        }[];
      };

      if (data.total === 0) {
        addOutput("warn", "No contracts found. Create YAML files in contracts/ to get started.");
        return;
      }

      for (const cr of data.results) {
        const status = cr.passed ? "pass" : "FAIL";
        const level = cr.passed ? "info" : "error";
        addOutput(level as OutputEntry["type"], `${status}  ${cr.contract_name} (${cr.model}) [${cr.duration_ms}ms]`);
        for (const a of cr.assertions || []) {
          if (a.passed) {
            addOutput("info", `  pass  ${a.expression}`);
          } else {
            addOutput("error", `  FAIL  ${a.expression} (${a.detail})`);
          }
        }
        if (cr.error) addOutput("error", `  Error: ${cr.error}`);
      }

      addOutput("info", "");
      if (data.failed === 0) {
        addOutput("info", `All ${data.passed} contract(s) passed.`);
      } else {
        addOutput("error", `${data.failed} contract(s) failed, ${data.passed} passed.`);
      }
    } catch (e: unknown) {
      addOutput("error", (e as Error).message);
    } finally {
      setRunning(false);
    }
  }, [addOutput]);

  return (
    <PipelineContext.Provider
      value={{
        running,
        output,
        runSummary,
        addOutput,
        clearOutput,
        setRunSummary,
        runTransformAll,
        runStream,
        runLint,
        runCurrentScript,
        runSingleModel,
        runContracts,
      }}
    >
      {children}
    </PipelineContext.Provider>
  );
}

export function usePipeline(): PipelineState {
  const ctx = useContext(PipelineContext);
  if (!ctx) throw new Error("usePipeline must be used within PipelineProvider");
  return ctx;
}
