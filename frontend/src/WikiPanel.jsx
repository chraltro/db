import { useState, useEffect, useMemo } from 'react';
import { api } from './api';

function renderMarkdown(md) {
  if (!md) return '';
  let html = md;
  // Code blocks
  html = html.replace(/```(\w*)\n([\s\S]*?)```/g, (_, lang, code) =>
    `<pre style="background:var(--havn-bg-tertiary);padding:12px;border-radius:var(--havn-radius-lg);overflow-x:auto;border:1px solid var(--havn-border)"><code>${code.replace(/</g,'&lt;').replace(/>/g,'&gt;')}</code></pre>`
  );
  // Inline code
  html = html.replace(/`([^`]+)`/g, '<code style="background:var(--havn-btn-bg);padding:2px 6px;border-radius:3px;font-size:0.9em;font-family:var(--havn-font-mono)">$1</code>');
  // Headers
  html = html.replace(/^### (.+)$/gm, '<h3 style="color:var(--havn-text);font-size:16px;margin:20px 0 8px">$1</h3>');
  html = html.replace(/^## (.+)$/gm, '<h2 style="color:var(--havn-text);font-size:20px;margin:24px 0 10px;padding-bottom:6px;border-bottom:1px solid var(--havn-border)">$1</h2>');
  html = html.replace(/^# (.+)$/gm, '<h1 style="color:var(--havn-text);font-size:26px;margin:0 0 16px;padding-bottom:8px;border-bottom:1px solid var(--havn-border-light)">$1</h1>');
  // Bold and italic
  html = html.replace(/\*\*(.+?)\*\*/g, '<strong style="color:var(--havn-text)">$1</strong>');
  html = html.replace(/\*(.+?)\*/g, '<em>$1</em>');
  // Links
  html = html.replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" style="color:var(--havn-accent);text-decoration:none" target="_blank">$1</a>');
  // Tables — collect consecutive pipe rows into a single <table>
  html = html.replace(/(^\|.+\|$\n?)+/gm, (block) => {
    const rows = block.trim().split('\n').filter(r => r.trim());
    let tableHtml = '<table style="border-collapse:collapse;width:100%;margin:12px 0">';
    let isFirst = true;
    for (const row of rows) {
      const cells = row.split('|').filter(c => c.trim());
      if (cells.every(c => /^[\s-:]+$/.test(c))) continue; // skip separator row
      const tag = isFirst ? 'th' : 'td';
      const style = isFirst
        ? 'padding:6px 12px;border:1px solid var(--havn-border);font-weight:600;background:var(--havn-bg-secondary)'
        : 'padding:6px 12px;border:1px solid var(--havn-border)';
      tableHtml += '<tr>' + cells.map(c => `<${tag} style="${style}">${c.trim()}</${tag}>`).join('') + '</tr>';
      isFirst = false;
    }
    tableHtml += '</table>';
    return tableHtml;
  });
  // Lists — group consecutive bullet/numbered lines into <ul>/<ol>
  html = html.replace(/(^- .+$\n?)+/gm, (block) => {
    const items = block.trim().split('\n').map(l => l.replace(/^- /, ''));
    return '<ul style="margin:8px 0;padding-left:24px">' + items.map(t => `<li style="margin:4px 0">${t}</li>`).join('') + '</ul>';
  });
  html = html.replace(/(^\d+\. .+$\n?)+/gm, (block) => {
    const items = block.trim().split('\n').map(l => l.replace(/^\d+\. /, ''));
    return '<ol style="margin:8px 0;padding-left:24px">' + items.map(t => `<li style="margin:4px 0">${t}</li>`).join('') + '</ol>';
  });
  // Paragraphs (double newlines)
  html = html.replace(/\n\n/g, '</p><p style="margin:8px 0;line-height:1.6">');
  // Single newlines that aren't tags
  html = html.replace(/\n(?!<)/g, '<br/>');
  return `<p style="margin:8px 0;line-height:1.6">${html}</p>`;
}

export default function WikiPanel() {
  const [pages, setPages] = useState([]);
  const [activePage, setActivePage] = useState(null);
  const [content, setContent] = useState(null);
  const [loading, setLoading] = useState(true);
  const [contentLoading, setContentLoading] = useState(false);
  const [search, setSearch] = useState('');

  useEffect(() => { loadPages(); }, []);

  const loadPages = async () => {
    setLoading(true);
    try {
      const data = await api.listWikiPages();
      setPages(data || []);
      if (data && data.length > 0) {
        const index = data.find(p => p.slug === 'index') || data[0];
        loadContent(index.slug);
      }
    } catch (e) { console.error(e); }
    setLoading(false);
  };

  const loadContent = async (slug) => {
    setActivePage(slug);
    setContentLoading(true);
    try {
      const data = await api.getWikiPage(slug);
      setContent(data);
    } catch (e) {
      setContent({ title: 'Error', content: 'Failed to load page.' });
    }
    setContentLoading(false);
  };

  const grouped = useMemo(() => {
    const groups = {};
    const filtered = pages.filter(p =>
      !search || p.title.toLowerCase().includes(search.toLowerCase()) || p.slug.includes(search.toLowerCase())
    );
    for (const p of filtered) {
      const cat = p.category || 'Other';
      if (!groups[cat]) groups[cat] = [];
      groups[cat].push(p);
    }
    return groups;
  }, [pages, search]);

  const categoryOrder = ['Getting Started', 'Core Concepts', 'Data Integration', 'Data Quality', 'Security', 'Advanced', 'Reference'];

  const handleOpenNewTab = () => {
    if (activePage) {
      const url = `${window.location.origin}/api/wiki/${activePage}`;
      window.open(url, '_blank');
    }
  };

  return (
    <div style={s.container}>
      <div style={s.sidebar}>
        <div style={s.sidebarHeader}>
          <div style={{ fontSize: 14, fontWeight: 600, color: 'var(--havn-text)', marginBottom: 8 }}>Wiki</div>
          <input style={s.search} placeholder="Search pages..." value={search} onChange={e => setSearch(e.target.value)} />
        </div>
        <div style={s.sidebarContent}>
          {loading ? <p style={{ color: 'var(--havn-text-secondary)', padding: 16 }}>Loading...</p> :
            categoryOrder.filter(c => grouped[c]).map(cat => (
              <div key={cat}>
                <div style={s.category}>{cat}</div>
                {grouped[cat].map(p => (
                  <div key={p.slug} style={s.pageLink(activePage === p.slug)} onClick={() => loadContent(p.slug)}>
                    {p.title}
                  </div>
                ))}
              </div>
            ))
          }
        </div>
      </div>
      <div style={s.main}>
        {contentLoading ? <p style={{ color: 'var(--havn-text-secondary)' }}>Loading...</p> : content ? (
          <>
            <div style={s.mainHeader}>
              <span />
              <button style={s.openBtn} onClick={handleOpenNewTab}>Open in New Tab</button>
            </div>
            <div style={{ color: 'var(--havn-text-secondary)', fontSize: 14, lineHeight: 1.7 }} dangerouslySetInnerHTML={{ __html: renderMarkdown(content.content) }} />
          </>
        ) : (
          <div style={{ color: 'var(--havn-text-secondary)', textAlign: 'center', marginTop: 60 }}>
            <p style={{ fontSize: 16 }}>Select a page from the sidebar</p>
          </div>
        )}
      </div>
    </div>
  );
}

const s = {
  container: { height: '100%', display: 'flex', background: 'var(--havn-bg)' },
  sidebar: { width: 250, borderRight: '1px solid var(--havn-border)', display: 'flex', flexDirection: 'column', flexShrink: 0 },
  sidebarHeader: { padding: '12px 16px', borderBottom: '1px solid var(--havn-border)' },
  sidebarContent: { flex: 1, overflow: 'auto', padding: '8px 0' },
  search: { width: '100%', padding: '6px 10px', background: 'var(--havn-bg-tertiary)', color: 'var(--havn-text)', border: '1px solid var(--havn-border-light)', borderRadius: 'var(--havn-radius-lg)', fontSize: 13, boxSizing: 'border-box' },
  category: { fontSize: 11, color: 'var(--havn-text-secondary)', textTransform: 'uppercase', padding: '12px 16px 4px', letterSpacing: '0.5px' },
  pageLink: (active) => ({
    display: 'block', padding: '6px 16px', cursor: 'pointer', fontSize: 13,
    color: active ? 'var(--havn-accent)' : 'var(--havn-text-secondary)',
    background: active ? 'var(--havn-bg-secondary)' : 'transparent',
    borderLeft: active ? '2px solid var(--havn-accent)' : '2px solid transparent',
    textDecoration: 'none'
  }),
  main: { flex: 1, overflow: 'auto', padding: '24px 40px' },
  mainHeader: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 },
  openBtn: { padding: '4px 12px', background: 'var(--havn-btn-bg)', color: 'var(--havn-text-secondary)', border: '1px solid var(--havn-btn-border)', borderRadius: 'var(--havn-radius)', cursor: 'pointer', fontSize: 12 },
};
