'use strict';
const D = window.REVIEW_DATA;
const $ = id => document.getElementById(id);
const props = new Map(D.propositions.map(p => [p.id, p]));
const policies = new Map(D.policies.map(p => [p.id, p]));
let selectedPolicy = 'C17', selectedProposition = 'P17';
function node(tag, text, className) {
  const n = document.createElement(tag);
  if (text !== undefined) n.textContent = text;
  if (className) n.className = className;
  return n;
}
function locator(key) {
  const [section, anchor] = key.split(':');
  if (anchor.includes('Interp')) {
    const tail = anchor.split('-Interp-')[1];
    const base = anchor.startsWith('53-a') ? '53' : anchor.startsWith('53-b') ? '53(b)' : '16(h)';
    return `Comment ${base}-${tail.replaceAll('-', '.')}`;
  }
  return `§1026.${section}` + anchor.split('-').map(x => `(${x})`).join('');
}
function highlighted(text, terms) {
  const fragment = document.createDocumentFragment();
  const escaped = terms.map(t => t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).sort((a,b) => b.length-a.length);
  const pattern = new RegExp(escaped.join('|'), 'gi');
  let previous = 0;
  for (const match of text.matchAll(pattern)) {
    fragment.append(document.createTextNode(text.slice(previous, match.index)), node('mark', match[0]));
    previous = match.index + match[0].length;
  }
  fragment.append(document.createTextNode(text.slice(previous)));
  return fragment;
}
function setSelection(policyId, propositionId) {
  selectedPolicy = policies.has(policyId) ? policyId : 'C17';
  selectedProposition = props.has(propositionId) ? propositionId : policies.get(selectedPolicy).primary;
  history.replaceState(null, '', `#${selectedPolicy}/${selectedProposition}`);
  render();
}
function render() {
  const policy = policies.get(selectedPolicy);
  const related = new Map(policy.edges.map(e => [e.id, e]));
  const query = $('search').value.trim().toLowerCase();
  $('policy-list').replaceChildren();
  let count = 0;
  for (const p of D.policies) {
    const searchable = [p.id, p.name, ...p.edges.map(e => `${e.id} ${props.get(e.id).name}`)].join(' ').toLowerCase();
    if (!searchable.includes(query)) continue;
    count++;
    const button = node('button', `${p.id} · ${p.name}`, 'policy');
    button.type = 'button'; button.setAttribute('aria-pressed', p.id === selectedPolicy);
    button.onclick = () => setSelection(p.id, p.primary);
    $('policy-list').append(button);
  }
  $('policy-count').textContent = `${count}/31`;
  if (!count) $('policy-list').append(node('p', 'No matching policies.'));
  const title = node('h3', `${policy.id} · ${policy.name}`);
  const constraint = node('details');
  constraint.append(node('summary', 'Actual DFC predicate'), node('pre', policy.constraint));
  const derivation = node('details');
  derivation.append(node('summary', 'Actual derived-input computations'));
  for (const name of policy.functions) {
    const f = D.functions[name];
    derivation.append(node('p', `regulation_z_53_runtime.py:${f.line} · ${name}`, 'muted'), node('pre', f.code));
  }
  $('policy-detail').replaceChildren(title, node('p', 'Select a highlighted proposition to read its supporting text. Shared support is shown separately from direct motivation.', 'muted'), constraint, derivation);
  const ordered = [...D.propositions].sort((a,b) => (a.id === policy.primary ? -1 : b.id === policy.primary ? 1 : Number(related.has(b.id))-Number(related.has(a.id)) || a.id.localeCompare(b.id)));
  $('proposition-list').replaceChildren();
  for (const p of ordered) {
    const edge = related.get(p.id);
    if (!edge && !$('all-propositions').checked) continue;
    const cls = edge ? (p.id === policy.primary ? 'direct' : 'support') : 'unrelated';
    const button = node('button', undefined, `prop ${cls}${p.id === selectedProposition ? ' selected' : ''}`);
    button.type = 'button'; button.setAttribute('aria-pressed', p.id === selectedProposition);
    button.append(node('div', edge?.role || 'Not linked to this policy', 'badge'), node('strong', `${p.id} · ${p.name}`), node('p', p.text));
    button.onclick = () => setSelection(selectedPolicy, p.id);
    $('proposition-list').append(button);
  }
  const proposition = props.get(selectedProposition), edge = related.get(proposition.id);
  const panel = $('source-detail');
  panel.replaceChildren(node('h3', `${proposition.id} · ${proposition.name}`));
  panel.append(node('p', edge ? `${edge.role}: ${edge.reason}` : 'This proposition is not linked to the currently selected policy.', 'box'));
  const analysis = node('div', undefined, 'box');
  analysis.append(node('strong', 'Analysis → implementation'), node('p', proposition.text), node('p', proposition.implementation));
  if (!proposition.standalone) analysis.append(node('p', 'No standalone constraint: this entry supports policy meaning or records an incomplete definition.', 'muted'));
  panel.append(analysis);
  if (proposition.limitation) panel.append(node('div', `Assumption / limitation: ${proposition.limitation}`, 'warning'));
  if (selectedProposition === 'P22') panel.append(node('div', 'Current input limitation: the adapter retains detailed lots and does not accept an arbitrary merged proposal without its underlying allocation.', 'warning'));
  if (selectedProposition === 'P24') panel.append(node('div', 'External definition is shown below for review. Its waiver/refund alternatives and associated commentary are not fully implemented by the current classifier.', 'warning'));
  for (const key of proposition.refs) {
    const source = D.sources[key], article = node('article', undefined, 'source');
    article.append(node('div', `${source.kind}${source.external ? ' · External cross-reference' : ''}`, 'badge'), node('p', locator(key), 'locator'));
    const text = node('p'); text.append(highlighted(source.text, proposition.terms));
    const link = node('a', 'Open this subsection on CFPB ↗'); link.href = source.url; link.target = '_blank'; link.rel = 'noopener noreferrer';
    article.append(text, link); panel.append(article);
  }
  const used = D.policies.filter(p => p.edges.some(e => e.id === proposition.id));
  const reverse = node('details'); reverse.append(node('summary', `Policies linked to ${proposition.id} (${used.length})`));
  for (const p of used) {
    const button = node('button', `${p.id} · ${p.name}`, 'policy');
    button.onclick = () => setSelection(p.id, proposition.id); reverse.append(button);
  }
  panel.append(reverse);
}
for (const m of D.metadata) {
  const p = node('p', `${m.url} — retrieved ${m.retrieved}. ${m.version}. SHA-256: ${m.sha256}`);
  $('metadata').append(p);
}
$('metadata').append(node('p', `Catalog snapshot SHA-256: ${D.catalogSha256}. Runtime snapshot SHA-256: ${D.runtimeSha256}. Generated: ${D.generated}.`, 'muted'));
$('search').oninput = render;
$('all-propositions').onchange = render;
function fromHash() { const [c,p] = location.hash.slice(1).split('/'); setSelection(c,p); }
window.addEventListener('hashchange', fromHash);
fromHash();
