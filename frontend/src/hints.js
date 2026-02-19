/**
 * Contextual hint definitions.
 *
 * Each hint has:
 *   id          - Unique, stable string used as localStorage key
 *   text        - One sentence, max ~120 chars
 *   action      - Optional { label, navigate } or { label, callback }
 *   target      - CSS selector for positioning (data-dp-hint attribute)
 *   priority    - Lower number = higher priority in the queue
 *   condition   - Function(hintState) => boolean
 *   repeatable  - If true, uses timestamp cooldown instead of permanent dismiss
 *   autoDismissMs - Override auto-dismiss timeout (default 15000)
 */
export const HINTS = [
  {
    id: "first-pipeline-complete",
    text: "Pipeline complete. Use the Diff tab next time to preview what changes before you run.",
    action: { label: "Open Diff", navigate: "Diff" },
    target: "[data-dp-hint='run-summary']",
    priority: 5,
    condition: (s) => !!s.pipelineJustCompleted,
  },
  {
    id: "first-editor-save",
    text: "Nice. You can run just this model with the Run Model button instead of running the full pipeline.",
    action: null,
    target: "[data-dp-hint='editor-toolbar']",
    priority: 8,
    condition: (s) => !!s.firstFileEdited,
  },
  {
    id: "query-panel-intro",
    text: "Use the schema sidebar on the left to browse tables. Click any table name to insert it into your query.",
    action: null,
    target: "[data-dp-hint='query-sidebar']",
    priority: 7,
    condition: (s) => !!s.queryPanelOpened && !!s.warehouseHasTables,
  },
  {
    id: "first-connector-done",
    text: "Connector ready. It will sync on the schedule you set. You can also trigger a manual sync anytime from Data Sources.",
    action: null,
    target: "[data-dp-hint='connector-list']",
    priority: 6,
    condition: (s) => !!s.firstConnectorDone,
  },
  {
    id: "connector-stale",
    text: "One or more connectors haven't synced in over a week. Check the scheduler or run a manual sync.",
    action: { label: "View connectors", navigate: "Data Sources" },
    target: "[data-dp-hint='connector-health']",
    priority: 3,
    repeatable: true,
    condition: (s) => !!s.connectorStale,
  },
  {
    id: "git-detected",
    text: "dp detected a git repo. Try `dp status` in your terminal or check the Diff tab to see data changes across branches.",
    action: { label: "Open Diff", navigate: "Diff" },
    target: "[data-dp-hint='git-status']",
    priority: 9,
    condition: (s) => !!s.gitDetected,
  },
  {
    id: "uncommitted-changes",
    text: "You have uncommitted changes. Run `dp checkpoint` to commit with an auto-generated message.",
    action: null,
    target: "[data-dp-hint='git-status']",
    priority: 4,
    condition: (s) => !!s.gitDirty && !!s.pipelineRanThisSession,
  },
  {
    id: "diff-has-changes",
    text: "These are the rows that would change. Review them before running the pipeline to catch issues early.",
    action: null,
    target: "[data-dp-hint='diff-results']",
    priority: 8,
    condition: (s) => !!s.hasDiffChanges,
  },
  {
    id: "dag-intro",
    text: "This shows how your models depend on each other. Click any node to jump to its source file.",
    action: null,
    target: "[data-dp-hint='dag-canvas']",
    priority: 10,
    condition: (s) => !!s.dagOpened,
  },
  {
    id: "tables-click-columns",
    text: "Click any column chip to sort the preview. Use 'Query this table' to explore further.",
    action: null,
    target: "[data-dp-hint='columns-bar']",
    priority: 9,
    condition: (s) => !!s.firstTableSelected,
  },
  {
    id: "keyboard-shortcuts",
    text: "Tip: use Alt+1 through Alt+5 to switch between tabs quickly.",
    action: null,
    target: "[data-dp-hint='tab-bar']",
    priority: 12,
    condition: (s) => (s.tabSwitchCount || 0) >= 5,
  },
  {
    id: "overview-no-runs",
    text: "You have data in the warehouse but haven't run a pipeline yet. Set up transforms to build your silver and gold layers.",
    action: { label: "Open Editor", navigate: "Editor" },
    target: "[data-dp-hint='pipeline-health']",
    priority: 6,
    condition: (s) => !!s.overviewNoRuns && !!s.warehouseHasTables,
  },
];
