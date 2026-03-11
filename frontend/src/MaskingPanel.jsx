import { useState, useEffect } from 'react';
import { api } from './api';

const METHODS = ['hash', 'redact', 'null', 'partial'];
const ROLES = ['admin', 'editor', 'viewer'];

const emptyPolicy = {
  schema_name: '', table_name: '', column_name: '', method: 'hash',
  method_config: {}, condition_column: '', condition_value: '', exempted_roles: ['admin']
};

export default function MaskingPanel() {
  const [policies, setPolicies] = useState([]);
  const [loading, setLoading] = useState(true);
  const [editing, setEditing] = useState(null); // null | 'new' | policy id
  const [form, setForm] = useState({ ...emptyPolicy });
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState(null);
  const [deleteConfirm, setDeleteConfirm] = useState(null);

  useEffect(() => { loadPolicies(); }, []);

  const loadPolicies = async () => {
    setLoading(true);
    try {
      const data = await api.listMaskingPolicies();
      setPolicies(data || []);
    } catch (e) { setError(e.message); }
    setLoading(false);
  };

  const handleSave = async () => {
    setSaving(true);
    setError(null);
    try {
      const payload = { ...form };
      if (payload.method !== 'partial') delete payload.method_config;
      if (!payload.condition_column) { delete payload.condition_column; delete payload.condition_value; }

      if (editing === 'new') {
        await api.createMaskingPolicy(payload);
      } else {
        await api.updateMaskingPolicy(editing, payload);
      }
      setEditing(null);
      setForm({ ...emptyPolicy });
      await loadPolicies();
    } catch (e) { setError(e.message); }
    setSaving(false);
  };

  const handleEdit = (policy) => {
    setEditing(policy.id);
    setForm({
      schema_name: policy.schema_name || '',
      table_name: policy.table_name || '',
      column_name: policy.column_name || '',
      method: policy.method || 'hash',
      method_config: policy.method_config || {},
      condition_column: policy.condition_column || '',
      condition_value: policy.condition_value || '',
      exempted_roles: policy.exempted_roles || ['admin']
    });
  };

  const handleDelete = async (id) => {
    try {
      await api.deleteMaskingPolicy(id);
      setDeleteConfirm(null);
      await loadPolicies();
    } catch (e) { setError(e.message); }
  };

  const updateForm = (key, value) => setForm(f => ({ ...f, [key]: value }));

  const toggleRole = (role) => {
    setForm(f => ({
      ...f,
      exempted_roles: f.exempted_roles.includes(role)
        ? f.exempted_roles.filter(r => r !== role)
        : [...f.exempted_roles, role]
    }));
  };

  return (
    <div style={s.container}>
      <div style={s.header}>
        <span style={s.title}>Data Masking Policies</span>
        {editing === null && (
          <button style={s.btnPrimary} onClick={() => { setEditing('new'); setForm({ ...emptyPolicy }); }}>
            + Add Policy
          </button>
        )}
      </div>
      <div style={s.content}>
        {error && (
          <div style={s.error}>
            {error}
            <button onClick={() => setError(null)} style={{ background: 'none', border: 'none', color: 'var(--dp-red)', cursor: 'pointer', marginLeft: 8 }}>×</button>
          </div>
        )}

        {editing !== null && (
          <div style={s.formSection}>
            <h4 style={{ color: 'var(--dp-text)', fontSize: 14, marginBottom: 12 }}>
              {editing === 'new' ? 'New Masking Policy' : 'Edit Policy'}
            </h4>
            <div style={s.formRow}>
              <div>
                <label style={s.label}>Schema</label>
                <input style={s.input} value={form.schema_name} onChange={e => updateForm('schema_name', e.target.value)} placeholder="e.g. gold" />
              </div>
              <div>
                <label style={s.label}>Table</label>
                <input style={s.input} value={form.table_name} onChange={e => updateForm('table_name', e.target.value)} placeholder="e.g. customers" />
              </div>
              <div>
                <label style={s.label}>Column</label>
                <input style={s.input} value={form.column_name} onChange={e => updateForm('column_name', e.target.value)} placeholder="e.g. email" />
              </div>
            </div>
            <div style={s.formRow}>
              <div>
                <label style={s.label}>Method</label>
                <select style={s.select} value={form.method} onChange={e => updateForm('method', e.target.value)}>
                  {METHODS.map(m => <option key={m} value={m}>{m}</option>)}
                </select>
              </div>
              {form.method === 'partial' && (
                <>
                  <div>
                    <label style={s.label}>Show First N</label>
                    <input style={s.input} type="number" value={form.method_config.show_first || 0} onChange={e => setForm(f => ({ ...f, method_config: { ...f.method_config, show_first: parseInt(e.target.value) || 0 } }))} />
                  </div>
                  <div>
                    <label style={s.label}>Show Last N</label>
                    <input style={s.input} type="number" value={form.method_config.show_last || 0} onChange={e => setForm(f => ({ ...f, method_config: { ...f.method_config, show_last: parseInt(e.target.value) || 0 } }))} />
                  </div>
                </>
              )}
            </div>
            <div style={s.formRow}>
              <div>
                <label style={s.label}>Condition Column (optional)</label>
                <input style={s.input} value={form.condition_column} onChange={e => updateForm('condition_column', e.target.value)} placeholder="e.g. region" />
              </div>
              <div>
                <label style={s.label}>Condition Value</label>
                <input style={s.input} value={form.condition_value} onChange={e => updateForm('condition_value', e.target.value)} placeholder="e.g. EU" />
              </div>
              <div>
                <label style={s.label}>Exempted Roles</label>
                <div style={{ display: 'flex', gap: 8, marginTop: 4 }}>
                  {ROLES.map(role => (
                    <label key={role} style={{ display: 'flex', alignItems: 'center', gap: 4, color: 'var(--dp-text)', fontSize: 13, cursor: 'pointer' }}>
                      <input type="checkbox" checked={form.exempted_roles.includes(role)} onChange={() => toggleRole(role)} />
                      {role}
                    </label>
                  ))}
                </div>
              </div>
            </div>
            <div style={{ display: 'flex', gap: 8 }}>
              <button style={s.btnPrimary} onClick={handleSave} disabled={saving || !form.schema_name || !form.table_name || !form.column_name}>
                {saving ? 'Saving...' : editing === 'new' ? 'Create Policy' : 'Update Policy'}
              </button>
              <button style={s.btn} onClick={() => { setEditing(null); setForm({ ...emptyPolicy }); }}>Cancel</button>
            </div>
          </div>
        )}

        {loading ? (
          <p style={{ color: 'var(--dp-text-secondary)' }}>Loading policies...</p>
        ) : policies.length === 0 && editing === null ? (
          <div style={{ textAlign: 'center', padding: 40, color: 'var(--dp-text-secondary)' }}>
            <p style={{ fontSize: 15, marginBottom: 8 }}>No masking policies configured</p>
            <p style={{ fontSize: 13 }}>Add policies to automatically mask sensitive data in query results.</p>
          </div>
        ) : policies.length > 0 && (
          <table style={s.table}>
            <thead>
              <tr>
                <th style={s.th}>Schema</th>
                <th style={s.th}>Table</th>
                <th style={s.th}>Column</th>
                <th style={s.th}>Method</th>
                <th style={s.th}>Config</th>
                <th style={s.th}>Condition</th>
                <th style={s.th}>Exempted</th>
                <th style={s.th}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {policies.map(p => (
                <tr key={p.id}>
                  <td style={s.td}>{p.schema_name}</td>
                  <td style={s.td}>{p.table_name}</td>
                  <td style={s.td}><code style={s.code}>{p.column_name}</code></td>
                  <td style={s.td}>
                    <span style={s.badge}>{p.method}</span>
                  </td>
                  <td style={s.td}>{p.method === 'partial' ? `first:${(p.method_config || {}).show_first || 0} last:${(p.method_config || {}).show_last || 0}` : '\u2014'}</td>
                  <td style={s.td}>{p.condition_column ? `${p.condition_column}=${p.condition_value}` : '\u2014'}</td>
                  <td style={s.td}>{(p.exempted_roles || []).join(', ')}</td>
                  <td style={s.td}>
                    <div style={{ display: 'flex', gap: 4 }}>
                      <button style={s.btn} onClick={() => handleEdit(p)}>Edit</button>
                      {deleteConfirm === p.id ? (
                        <>
                          <button style={s.btnDanger} onClick={() => handleDelete(p.id)}>Confirm</button>
                          <button style={{ ...s.btn, fontSize: 12 }} onClick={() => setDeleteConfirm(null)}>Cancel</button>
                        </>
                      ) : (
                        <button style={{ ...s.btn, color: 'var(--dp-red)' }} onClick={() => setDeleteConfirm(p.id)}>Delete</button>
                      )}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

const s = {
  container: { display: 'flex', flexDirection: 'column', height: '100%', overflow: 'hidden' },
  header: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '8px 12px', borderBottom: '1px solid var(--dp-border)' },
  title: { fontWeight: 600, fontSize: 14 },
  content: { flex: 1, overflow: 'auto', padding: '16px 24px', maxWidth: 900 },
  table: { width: '100%', borderCollapse: 'collapse', marginBottom: 12, fontSize: 12 },
  th: { textAlign: 'left', padding: '6px 10px', borderBottom: '2px solid var(--dp-border-light)', color: 'var(--dp-text-secondary)', fontWeight: 600, fontSize: 11, textTransform: 'uppercase' },
  td: { padding: '6px 10px', borderBottom: '1px solid var(--dp-border)' },
  btn: { padding: '4px 10px', background: 'var(--dp-btn-bg)', border: '1px solid var(--dp-btn-border)', borderRadius: 'var(--dp-radius)', color: 'var(--dp-text)', cursor: 'pointer', fontSize: 12 },
  btnPrimary: { padding: '6px 14px', background: 'var(--dp-green)', border: '1px solid var(--dp-green-border)', borderRadius: 'var(--dp-radius-lg)', color: '#fff', cursor: 'pointer', fontSize: 12, fontWeight: 500, whiteSpace: 'nowrap' },
  btnDanger: { padding: '4px 10px', background: 'var(--dp-red-bg)', color: 'var(--dp-red)', border: '1px solid var(--dp-red-border)', borderRadius: 'var(--dp-radius)', cursor: 'pointer', fontSize: 12 },
  input: { padding: '6px 10px', background: 'var(--dp-bg-tertiary)', color: 'var(--dp-text)', border: '1px solid var(--dp-border-light)', borderRadius: 'var(--dp-radius-lg)', fontSize: 13, width: '100%', boxSizing: 'border-box' },
  select: { padding: '6px 10px', background: 'var(--dp-bg-tertiary)', color: 'var(--dp-text)', border: '1px solid var(--dp-border-light)', borderRadius: 'var(--dp-radius)', fontSize: 12 },
  label: { display: 'block', fontSize: 12, color: 'var(--dp-text-secondary)', marginBottom: 4 },
  formRow: { display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 12, marginBottom: 12 },
  formSection: { background: 'var(--dp-bg-secondary)', border: '1px solid var(--dp-border)', borderRadius: 'var(--dp-radius-lg)', padding: 16, marginBottom: 16 },
  badge: { padding: '2px 8px', borderRadius: 'var(--dp-radius)', fontSize: 11, background: 'var(--dp-btn-bg)', color: 'var(--dp-accent)' },
  code: { background: 'var(--dp-btn-bg)', padding: '1px 5px', borderRadius: 3, fontSize: 12, fontFamily: 'var(--dp-font-mono)' },
  error: { padding: '8px 12px', background: 'var(--dp-red-bg)', color: 'var(--dp-red)', border: '1px solid var(--dp-red-border)', borderRadius: 'var(--dp-radius-lg)', marginBottom: 12, fontSize: 13 },
};
