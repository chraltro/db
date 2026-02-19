import React from "react";

export default class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }

  componentDidCatch(error, info) {
    console.error(`[ErrorBoundary] ${this.props.name || "Unknown"}:`, error, info);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div style={styles.container}>
          <div style={styles.icon}>!</div>
          <div style={styles.title}>
            {this.props.name ? `${this.props.name} crashed` : "Something went wrong"}
          </div>
          <div style={styles.message}>{this.state.error?.message || "An unexpected error occurred."}</div>
          <button
            style={styles.button}
            onClick={() => this.setState({ hasError: false, error: null })}
          >
            Try Again
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}

const styles = {
  container: {
    display: "flex",
    flexDirection: "column",
    alignItems: "center",
    justifyContent: "center",
    height: "100%",
    padding: "24px",
    color: "var(--dp-text-secondary)",
    fontFamily: "var(--dp-font)",
    textAlign: "center",
    gap: "12px",
  },
  icon: {
    width: "36px",
    height: "36px",
    borderRadius: "50%",
    background: "var(--dp-red, #e53e3e)",
    color: "#fff",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    fontSize: "18px",
    fontWeight: "bold",
  },
  title: {
    fontSize: "14px",
    fontWeight: 600,
    color: "var(--dp-text)",
  },
  message: {
    fontSize: "12px",
    color: "var(--dp-text-dim)",
    maxWidth: "400px",
    fontFamily: "var(--dp-font-mono)",
    wordBreak: "break-word",
  },
  button: {
    marginTop: "8px",
    padding: "6px 16px",
    background: "var(--dp-btn-bg)",
    border: "1px solid var(--dp-btn-border)",
    borderRadius: "var(--dp-radius-lg)",
    color: "var(--dp-text)",
    cursor: "pointer",
    fontSize: "12px",
    fontWeight: 500,
  },
};
