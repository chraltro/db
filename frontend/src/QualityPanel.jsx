import React, { useState, useEffect, useMemo } from 'react';
import { api } from './api';

const SUB_TABS = ['Freshness', 'Profiles', 'Assertions', 'Contracts'];

function useSortable(defaultKey, defaultDir = 'asc') {
  const [sortKey, setSortKey] = useState(defaultKey);
  const [sortDir, setSortDir] = useState(defaultDir);
  const toggle = (key) => {
    if (sortKey === key) setSortDir(d => d === 'asc' ? 'desc' : 'asc');
    else { setSortKey(key); setSortDir('asc'); }
  };
  return { sortKey, sortDir, toggle };
}

function sortData(data, key, dir, getter) {
  if (!key) return data;
  return [...data].sort((a, b) => {
    const va = getter(a, key);
    const vb = getter(b, key);
    if (va == null && vb == null) return 0;
    if (va == null) return 1;
    if (vb == null) return -1;
    if (typeof va === 'number' && typeof vb === 'number') return dir === 'asc' ? va - vb : vb - va;
    const sa = String(va).toLowerCase(), sb = String(vb).toLowerCase();
    return dir === 'asc' ? sa.localeCompare(sb) : sb.localeCompare(sa);
  });
}

function SortTh({ label, sortKey, current, dir, onToggle, style }) {
  const arrow = current === sortKey ? (dir === 'asc' ? ' \u25B4' : ' \u25BE') : '';
  return (
    <th style={{ ...style, cursor: 'pointer', userSelect: 'none' }} onClick={() => onToggle(sortKey)}>
      {label}{arrow}
    </th>
  );
}

export default function QualityPanel() {
  const [tab, setTab] = useState('Freshness');
  const [freshness, setFreshness] = useState([]);
  const [profiles, setProfiles] = useState([]);
  const [assertions, setAssertions] = useState([]);
  const [contracts, setContracts] = useState([]);
  const [contractHistory, setContractHistory] = useState([]);
  const [loading, setLoading] = useState(true);
  const [showHistory, setShowHistory] = useState(false);
  const [runningContracts, setRunningContracts] = useState(false);
  const [expandedProfile, setExpandedProfile] = useState(null);

  // Filters
  const [freshnessFilter, setFreshnessFilter] = useState('');
  const [freshnessStatus, setFreshnessStatus] = useState('all');
  const [profileFilter, setProfileFilter] = useState('');
  const [assertionFilter, setAssertionFilter] = useState('');
  const [assertionStatus, setAssertionStatus] = useState('all');
  const [contractFilter, setContractFilter] = useState('');
  const [contractStatus, setContractStatus] = useState('all');

  // Sorting
  const freshSort = useSortable('model');
  const profileSort = useSortable('model');
  const assertionSort = useSortable('model');
  const contractSort = useSortable('model');

  useEffect(() => { loadAll(); }, []);

  const loadAll = async () => {
    setLoading(true);
    try {
      const [f, p, a, c, ch] = await Promise.allSettled([
        api.getFreshness(24),
        api.getProfiles(),
        api.getAssertions(100),
        api.getContracts(),
        api.getContractHistory()
      ]);
      setFreshness(f.status === 'fulfilled' ? (Array.isArray(f.value) ? f.value : []) : []);
      setProfiles(p.status === 'fulfilled' ? (p.value || []) : []);
      setAssertions(a.status === 'fulfilled' ? (a.value || []) : []);
      setContracts(c.status === 'fulfilled' ? (c.value || []) : []);
      setContractHistory(ch.status === 'fulfilled' ? (ch.value || []) : []);
    } catch (e) { console.error(e); }
    setLoading(false);
  };

  const handleRunContracts = async () => {
    setRunningContracts(true);
    try {
      await api.runContracts();
      await loadAll();
    } catch (e) { console.error(e); }
    setRunningContracts(false);
  };

  // Stats
  const totalModels = freshness.length;
  const staleModels = freshness.filter(m => m.is_stale).length;
  const passedAssertions = assertions.filter(a => a.passed === true).length;
  const assertionRate = assertions.length > 0 ? Math.round((passedAssertions / assertions.length) * 100) : 100;
  const passedContracts = contractHistory.filter(c => c.passed === true).length;
  const contractRate = contractHistory.length > 0 ? Math.round((passedContracts / contractHistory.length) * 100) : 100;

  // --- Freshness: fields are model, last_run_at, hours_since_run, is_stale, row_count ---
  const filteredFreshness = useMemo(() => {
    let data = freshness;
    if (freshnessFilter) data = data.filter(m => (m.model || '').toLowerCase().includes(freshnessFilter.toLowerCase()));
    if (freshnessStatus === 'stale') data = data.filter(m => m.is_stale);
    if (freshnessStatus === 'fresh') data = data.filter(m => !m.is_stale);
    return sortData(data, freshSort.sortKey, freshSort.sortDir, (item, key) => {
      if (key === 'model') return item.model || '';
      if (key === 'hours_since_run') return item.hours_since_run ?? 9999;
      if (key === 'is_stale') return item.is_stale ? 1 : 0;
      if (key === 'row_count') return item.row_count ?? 0;
      return item[key];
    });
  }, [freshness, freshnessFilter, freshnessStatus, freshSort.sortKey, freshSort.sortDir]);

  // --- Profiles: fields are model, row_count, column_count, null_percentages (dict), distinct_counts (dict), profiled_at ---
  const filteredProfiles = useMemo(() => {
    let data = profiles;
    if (profileFilter) data = data.filter(p => (p.model || '').toLowerCase().includes(profileFilter.toLowerCase()));
    return sortData(data, profileSort.sortKey, profileSort.sortDir, (item, key) => {
      if (key === 'model') return item.model || '';
      if (key === 'row_count') return item.row_count ?? 0;
      if (key === 'column_count') return item.column_count ?? 0;
      return item[key];
    });
  }, [profiles, profileFilter, profileSort.sortKey, profileSort.sortDir]);

  // --- Assertions: fields are model, expression, passed (bool), detail, checked_at ---
  const filteredAssertions = useMemo(() => {
    let data = assertions;
    if (assertionFilter) data = data.filter(a => (a.model || '').toLowerCase().includes(assertionFilter.toLowerCase()) || (a.expression || '').toLowerCase().includes(assertionFilter.toLowerCase()));
    if (assertionStatus === 'pass') data = data.filter(a => a.passed === true);
    if (assertionStatus === 'fail') data = data.filter(a => a.passed === false);
    return sortData(data, assertionSort.sortKey, assertionSort.sortDir, (item, key) => {
      if (key === 'model') return item.model || '';
      if (key === 'passed') return item.passed ? 0 : 1;
      if (key === 'checked_at') return item.checked_at || '';
      return item[key];
    });
  }, [assertions, assertionFilter, assertionStatus, assertionSort.sortKey, assertionSort.sortDir]);

  // --- Contracts list: name, model, description, severity, assertions (string[]), path ---
  // --- Contract history: contract_name, model, passed (bool), severity, detail (JSON string), checked_at ---
  const filteredContracts = useMemo(() => {
    const source = showHistory ? contractHistory : contracts;
    let data = source;
    if (contractFilter) data = data.filter(c => (c.model || '').toLowerCase().includes(contractFilter.toLowerCase()) || (c.name || c.contract_name || '').toLowerCase().includes(contractFilter.toLowerCase()));
    if (contractStatus === 'pass') data = data.filter(c => c.passed === true);
    if (contractStatus === 'fail') data = data.filter(c => c.passed === false);
    return sortData(data, contractSort.sortKey, contractSort.sortDir, (item, key) => {
      if (key === 'model') return item.model || '';
      if (key === 'name') return item.name || item.contract_name || '';
      if (key === 'severity') return item.severity || '';
      if (key === 'passed') return item.passed ? 0 : 1;
      return item[key];
    });
  }, [contracts, contractHistory, showHistory, contractFilter, contractStatus, contractSort.sortKey, contractSort.sortDir]);

  // Build column detail rows for a profile's null_percentages + distinct_counts
  const profileColumns = (p) => {
    const nulls = p.null_percentages || {};
    const distincts = p.distinct_counts || {};
    const allCols = [...new Set([...Object.keys(nulls), ...Object.keys(distincts)])];
    return allCols.map(col => ({
      name: col,
      null_pct: nulls[col] ?? null,
      distinct: distincts[col] ?? null,
    }));
  };

  return (
    <div style={s.container}>
      <div style={s.header}>
        <div style={s.title}>Data Quality</div>
        <div style={s.cards}>
          <div style={s.card}>
            <div style={s.cardLabel}>Total Models</div>
            <div style={s.cardValue}>{totalModels}</div>
          </div>
          <div style={s.card}>
            <div style={s.cardLabel}>Stale Models</div>
            <div style={{ ...s.cardValue, color: staleModels > 0 ? '#f87171' : '#4ade80' }}>{staleModels}</div>
          </div>
          <div style={s.card}>
            <div style={s.cardLabel}>Assertion Pass Rate</div>
            <div style={{ ...s.cardValue, color: assertionRate === 100 ? '#4ade80' : assertionRate >= 80 ? '#facc15' : '#f87171' }}>{assertionRate}%</div>
          </div>
          <div style={s.card}>
            <div style={s.cardLabel}>Contract Pass Rate</div>
            <div style={{ ...s.cardValue, color: contractRate === 100 ? '#4ade80' : contractRate >= 80 ? '#facc15' : '#f87171' }}>{contractRate}%</div>
          </div>
        </div>
        <div style={s.tabs}>
          {SUB_TABS.map(t => (
            <div key={t} style={{ ...s.tab, ...(tab === t ? s.tabActive : {}) }} onClick={() => setTab(t)}>{t}</div>
          ))}
        </div>
      </div>
      <div style={s.content}>
        {loading ? <p style={{ color: '#888' }}>Loading quality data...</p> : (
          <>
            {/* ─── FRESHNESS ─── */}
            {tab === 'Freshness' && (
              <>
                <div style={s.toolbar}>
                  <input style={s.filterInput} placeholder="Filter by model..." value={freshnessFilter} onChange={e => setFreshnessFilter(e.target.value)} />
                  <select style={s.filterSelect} value={freshnessStatus} onChange={e => setFreshnessStatus(e.target.value)}>
                    <option value="all">All</option>
                    <option value="stale">Stale Only</option>
                    <option value="fresh">Fresh Only</option>
                  </select>
                  <span style={s.count}>{filteredFreshness.length} of {freshness.length}</span>
                </div>
                <table style={s.table}>
                  <thead><tr>
                    <SortTh label="Model" sortKey="model" current={freshSort.sortKey} dir={freshSort.sortDir} onToggle={freshSort.toggle} style={s.th} />
                    <SortTh label="Last Run" sortKey="last_run_at" current={freshSort.sortKey} dir={freshSort.sortDir} onToggle={freshSort.toggle} style={s.th} />
                    <SortTh label="Hours Ago" sortKey="hours_since_run" current={freshSort.sortKey} dir={freshSort.sortDir} onToggle={freshSort.toggle} style={s.th} />
                    <SortTh label="Rows" sortKey="row_count" current={freshSort.sortKey} dir={freshSort.sortDir} onToggle={freshSort.toggle} style={s.th} />
                    <SortTh label="Status" sortKey="is_stale" current={freshSort.sortKey} dir={freshSort.sortDir} onToggle={freshSort.toggle} style={s.th} />
                  </tr></thead>
                  <tbody>
                    {filteredFreshness.length === 0 ? <tr><td colSpan={5} style={{ ...s.td, color: '#666', textAlign: 'center' }}>No freshness data available</td></tr> :
                    filteredFreshness.map((m, i) => (
                      <tr key={i}>
                        <td style={s.td}>{m.model}</td>
                        <td style={s.td}>{m.last_run_at || '\u2014'}</td>
                        <td style={s.td}>{m.hours_since_run != null ? m.hours_since_run.toFixed(1) : '\u2014'}</td>
                        <td style={s.td}>{m.row_count != null ? m.row_count.toLocaleString() : '\u2014'}</td>
                        <td style={s.td}><span style={s.badge(!m.is_stale)}>{m.is_stale ? 'STALE' : 'FRESH'}</span></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </>
            )}

            {/* ─── PROFILES ─── */}
            {tab === 'Profiles' && (
              <>
                <div style={s.toolbar}>
                  <input style={s.filterInput} placeholder="Filter by model..." value={profileFilter} onChange={e => setProfileFilter(e.target.value)} />
                  <span style={s.count}>{filteredProfiles.length} of {profiles.length}</span>
                </div>
                <table style={s.table}>
                  <thead><tr>
                    <SortTh label="Model" sortKey="model" current={profileSort.sortKey} dir={profileSort.sortDir} onToggle={profileSort.toggle} style={s.th} />
                    <SortTh label="Row Count" sortKey="row_count" current={profileSort.sortKey} dir={profileSort.sortDir} onToggle={profileSort.toggle} style={s.th} />
                    <SortTh label="Columns" sortKey="column_count" current={profileSort.sortKey} dir={profileSort.sortDir} onToggle={profileSort.toggle} style={s.th} />
                    <th style={s.th}>Profiled At</th>
                  </tr></thead>
                  <tbody>
                    {filteredProfiles.length === 0 ? <tr><td colSpan={4} style={{ ...s.td, color: '#666', textAlign: 'center' }}>No profiles available. Run profiling first.</td></tr> :
                    filteredProfiles.map((p, i) => {
                      const cols = profileColumns(p);
                      return (
                        <React.Fragment key={i}>
                          <tr>
                            <td style={s.td}>
                              {cols.length > 0 ? (
                                <span style={{ cursor: 'pointer', color: 'var(--havn-accent, #60a5fa)' }} onClick={() => setExpandedProfile(expandedProfile === i ? null : i)}>
                                  {expandedProfile === i ? '\u25BE' : '\u25B8'} {p.model}
                                </span>
                              ) : p.model}
                            </td>
                            <td style={s.td}>{p.row_count != null ? p.row_count.toLocaleString() : '\u2014'}</td>
                            <td style={s.td}>{p.column_count || '\u2014'}</td>
                            <td style={s.td}>{p.profiled_at || '\u2014'}</td>
                          </tr>
                          {expandedProfile === i && cols.map((col, ci) => (
                            <tr key={`${i}-${ci}`} style={{ background: '#111' }}>
                              <td style={{ ...s.td, paddingLeft: 32, fontFamily: 'monospace', fontSize: 12 }}>{col.name}</td>
                              <td style={s.td}>{col.null_pct != null ? `${col.null_pct.toFixed(1)}% null` : '\u2014'}</td>
                              <td style={s.td}>{col.distinct != null ? `${col.distinct.toLocaleString()} distinct` : '\u2014'}</td>
                              <td style={s.td}></td>
                            </tr>
                          ))}
                        </React.Fragment>
                      );
                    })}
                  </tbody>
                </table>
              </>
            )}

            {/* ─── ASSERTIONS ─── */}
            {tab === 'Assertions' && (
              <>
                <div style={s.toolbar}>
                  <input style={s.filterInput} placeholder="Filter by model or expression..." value={assertionFilter} onChange={e => setAssertionFilter(e.target.value)} />
                  <select style={s.filterSelect} value={assertionStatus} onChange={e => setAssertionStatus(e.target.value)}>
                    <option value="all">All</option>
                    <option value="pass">Pass Only</option>
                    <option value="fail">Fail Only</option>
                  </select>
                  <span style={s.count}>{filteredAssertions.length} of {assertions.length}</span>
                </div>
                <table style={s.table}>
                  <thead><tr>
                    <SortTh label="Model" sortKey="model" current={assertionSort.sortKey} dir={assertionSort.sortDir} onToggle={assertionSort.toggle} style={s.th} />
                    <th style={s.th}>Expression</th>
                    <SortTh label="Status" sortKey="passed" current={assertionSort.sortKey} dir={assertionSort.sortDir} onToggle={assertionSort.toggle} style={s.th} />
                    <th style={s.th}>Detail</th>
                    <SortTh label="Checked At" sortKey="checked_at" current={assertionSort.sortKey} dir={assertionSort.sortDir} onToggle={assertionSort.toggle} style={s.th} />
                  </tr></thead>
                  <tbody>
                    {filteredAssertions.length === 0 ? <tr><td colSpan={5} style={{ ...s.td, color: '#666', textAlign: 'center' }}>No assertion results</td></tr> :
                    filteredAssertions.map((a, i) => (
                      <tr key={i}>
                        <td style={s.td}>{a.model}</td>
                        <td style={s.td}><code style={{ fontSize: 12 }}>{a.expression}</code></td>
                        <td style={s.td}><span style={s.badge(a.passed)}>{a.passed ? 'PASS' : 'FAIL'}</span></td>
                        <td style={s.td}>{a.detail || '\u2014'}</td>
                        <td style={s.td}>{a.checked_at || '\u2014'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </>
            )}

            {/* ─── CONTRACTS ─── */}
            {tab === 'Contracts' && (
              <>
                <div style={s.toolbar}>
                  <button style={s.btnPrimary} onClick={handleRunContracts} disabled={runningContracts}>
                    {runningContracts ? 'Running...' : 'Run Contracts'}
                  </button>
                  <button style={s.btn} onClick={() => setShowHistory(!showHistory)}>
                    {showHistory ? 'Show Definitions' : 'Show History'}
                  </button>
                  <input style={s.filterInput} placeholder="Filter by model or name..." value={contractFilter} onChange={e => setContractFilter(e.target.value)} />
                  {showHistory && (
                    <select style={s.filterSelect} value={contractStatus} onChange={e => setContractStatus(e.target.value)}>
                      <option value="all">All</option>
                      <option value="pass">Pass Only</option>
                      <option value="fail">Fail Only</option>
                    </select>
                  )}
                  <span style={s.count}>{filteredContracts.length} of {(showHistory ? contractHistory : contracts).length}</span>
                </div>
                {showHistory ? (
                  <table style={s.table}>
                    <thead><tr>
                      <SortTh label="Contract" sortKey="name" current={contractSort.sortKey} dir={contractSort.sortDir} onToggle={contractSort.toggle} style={s.th} />
                      <SortTh label="Model" sortKey="model" current={contractSort.sortKey} dir={contractSort.sortDir} onToggle={contractSort.toggle} style={s.th} />
                      <SortTh label="Status" sortKey="passed" current={contractSort.sortKey} dir={contractSort.sortDir} onToggle={contractSort.toggle} style={s.th} />
                      <SortTh label="Severity" sortKey="severity" current={contractSort.sortKey} dir={contractSort.sortDir} onToggle={contractSort.toggle} style={s.th} />
                      <th style={s.th}>Checked At</th>
                    </tr></thead>
                    <tbody>
                      {filteredContracts.length === 0 ? (
                        <tr><td colSpan={5} style={{ ...s.td, color: '#666', textAlign: 'center' }}>No contract history. Run contracts first.</td></tr>
                      ) : filteredContracts.map((c, i) => (
                        <tr key={i}>
                          <td style={s.td}>{c.contract_name}</td>
                          <td style={s.td}>{c.model}</td>
                          <td style={s.td}><span style={s.badge(c.passed)}>{c.passed ? 'PASS' : 'FAIL'}</span></td>
                          <td style={s.td}>
                            <span style={{ padding: '2px 8px', borderRadius: 4, fontSize: 11, background: c.severity === 'warn' ? '#3a3a1a' : '#3a1a1a', color: c.severity === 'warn' ? '#facc15' : '#f87171' }}>{c.severity}</span>
                          </td>
                          <td style={s.td}>{c.checked_at || '\u2014'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                ) : (
                  <table style={s.table}>
                    <thead><tr>
                      <SortTh label="Name" sortKey="name" current={contractSort.sortKey} dir={contractSort.sortDir} onToggle={contractSort.toggle} style={s.th} />
                      <SortTh label="Model" sortKey="model" current={contractSort.sortKey} dir={contractSort.sortDir} onToggle={contractSort.toggle} style={s.th} />
                      <th style={s.th}>Description</th>
                      <SortTh label="Severity" sortKey="severity" current={contractSort.sortKey} dir={contractSort.sortDir} onToggle={contractSort.toggle} style={s.th} />
                      <th style={s.th}>Assertions</th>
                    </tr></thead>
                    <tbody>
                      {filteredContracts.length === 0 ? (
                        <tr><td colSpan={5} style={{ ...s.td, color: '#666', textAlign: 'center' }}>No contracts defined. Add YAML files in contracts/</td></tr>
                      ) : filteredContracts.map((c, i) => (
                        <tr key={i}>
                          <td style={s.td}>{c.name}</td>
                          <td style={s.td}>{c.model}</td>
                          <td style={s.td}>{c.description || '\u2014'}</td>
                          <td style={s.td}>
                            <span style={{ padding: '2px 8px', borderRadius: 4, fontSize: 11, background: c.severity === 'warn' ? '#3a3a1a' : '#3a1a1a', color: c.severity === 'warn' ? '#facc15' : '#f87171' }}>{c.severity}</span>
                          </td>
                          <td style={s.td}>{(c.assertions || []).length} rule{(c.assertions || []).length !== 1 ? 's' : ''}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </>
            )}
          </>
        )}
      </div>
    </div>
  );
}

const s = {
  container: { height: '100%', display: 'flex', flexDirection: 'column', background: '#0e0e0e' },
  header: { padding: '16px 20px', borderBottom: '1px solid #222' },
  title: { fontSize: 16, fontWeight: 600, color: '#e0e0e0', marginBottom: 12 },
  cards: { display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12, marginBottom: 16 },
  card: { background: '#161616', border: '1px solid #222', borderRadius: 8, padding: '12px 16px' },
  cardLabel: { fontSize: 11, color: '#888', textTransform: 'uppercase', marginBottom: 4 },
  cardValue: { fontSize: 24, fontWeight: 700, color: '#e0e0e0' },
  tabs: { display: 'flex', gap: 0, borderBottom: '1px solid #222' },
  tab: { padding: '8px 20px', cursor: 'pointer', fontSize: 13, color: '#888', borderBottom: '2px solid transparent', background: 'none' },
  tabActive: { color: '#e0e0e0', borderBottom: '2px solid #2563eb' },
  content: { flex: 1, overflow: 'auto', padding: 20 },
  toolbar: { display: 'flex', gap: 8, marginBottom: 12, alignItems: 'center', flexWrap: 'wrap' },
  filterInput: { padding: '5px 10px', background: '#1a1a1a', color: '#e0e0e0', border: '1px solid #333', borderRadius: 6, fontSize: 12, width: 220 },
  filterSelect: { padding: '5px 8px', background: '#1a1a1a', color: '#e0e0e0', border: '1px solid #333', borderRadius: 6, fontSize: 12 },
  count: { fontSize: 12, color: '#666', marginLeft: 'auto' },
  table: { width: '100%', borderCollapse: 'collapse', fontSize: 13 },
  th: { textAlign: 'left', padding: '8px 12px', borderBottom: '1px solid #333', color: '#888', fontSize: 11, textTransform: 'uppercase' },
  td: { padding: '8px 12px', borderBottom: '1px solid #222', color: '#e0e0e0' },
  badge: (ok) => ({ padding: '2px 8px', borderRadius: 4, fontSize: 11, background: ok ? '#1a3a1a' : '#3a1a1a', color: ok ? '#4ade80' : '#f87171' }),
  btn: { padding: '6px 14px', background: '#2a2a2a', color: '#e0e0e0', border: '1px solid #444', borderRadius: 6, cursor: 'pointer', fontSize: 13 },
  btnPrimary: { padding: '6px 14px', background: '#2563eb', color: '#fff', border: 'none', borderRadius: 6, cursor: 'pointer', fontSize: 13 },
};
