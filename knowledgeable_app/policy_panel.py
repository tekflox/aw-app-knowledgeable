"""The per-tool exec-indexing policy maintenance page — one self-contained
HTML document, no build step (docs/design/exec-index-per-tool-policy.md §7;
slice 3).

This is the half of the card Frederico asked for in his own words: *"dentro da
app do connector do knowledge, dentro do settings dela, vamos ter uma UI que
vamos poder ver essa lista e fazer manutenção manual nela."* Slices 1, 2 and 4
built a policy that enforces; none of them gave a human any way to look at it
or disagree with it.

**Why an HTML page in an iframe and not declarative widgets.** The
workspace's declarative vocabulary (``aw-workspace-ui``'s ``AppWindow.jsx``)
is ``markdown``/``list``/``button``/``iframe``/``app_iframe``/
``collapsible``/``form``/``auth_status``. Its ``list`` renders STATIC strings
from the spec; there is no data-bound repeater, and the ``table`` widget §10
rejected has never rendered at all (the aw-app-proxy scar — months, silently).
A row-per-tool view with its own per-row controls cannot be expressed there.
An iframe onto an app route is the vocabulary's own escape hatch and the
established idiom in this estate: ``aw-app-secrets``' ``panel.py`` is the
closest sibling (row-per-secret, inline controls, no build step), with
``aw-app-whatsapp``, ``aw-app-plaud`` and ``aw-app-remote-screen`` doing the
same.

**Two host constraints this page is written AROUND, not against:**

* **No modals.** The iframe is sandboxed ``allow-scripts allow-forms
  allow-same-origin`` — notably *not* ``allow-modals``, so
  ``confirm()``/``prompt()``/``alert()`` silently do nothing (§12 risk 8 names
  this exactly, and aw-app-secrets' panel carries the same warning). Every
  edit and every confirmation here is an inline row that expands in place. A
  ``confirm()`` on the "clear this override" action would have been a dead
  button that looked like it worked.
* **Narrow and short.** The host renders this in ``.appwin-iframe``, a
  ``min-height: 320px`` box that is narrow in the Settings sidebar and wide in
  a resized window. A multi-column table wraps every cell into a ribbon at the
  narrow end, so rows are flex lines that reflow, and the state is a coloured
  pill rather than a column.

**It loads from the API origin**, so relative ``/api/apps/knowledgeable/…``
fetches are same-origin and the apex ``aw_id_jwt`` cookie authorises them.
There is no token anywhere in this markup and none should be added: a page
that carries a credential in its source is a page that leaks one.

**The five states are the product.** Under the blacklist, ``never declared``
behaves identically to ``declared — indexable``; if this page rendered them
the same, nobody could ever tell which tools have actually been reviewed (§7,
card point f). They get different colours and different words. The unresolved
section shows RAW spellings for the same family of reason: the original defect
was a name that did not match, and a fourth gateway prefix is meant to be
diagnosed from this page rather than from the next incident.
"""
from __future__ import annotations

PANEL_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Exec-indexing policy</title>
<style>
  :root {
    --bg: #0f1115; --panel: #171a21; --border: #262b36; --text: #e6e8ee;
    --muted: #939aab; --accent: #4f8cff; --warn: #f0a020; --danger: #e5484d;
    --ok: #3fb950; --locked: #8b93a7; --review: #a371f7;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; padding: 16px; background: var(--bg); color: var(--text);
    font: 13px/1.5 ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif;
  }
  h2 { font-size: 14px; margin: 0 0 2px; }
  h3 { font-size: 12.5px; margin: 0 0 8px; color: var(--text); }
  .sub { color: var(--muted); font-size: 12px; margin-bottom: 14px; }
  .card {
    background: var(--panel); border: 1px solid var(--border); border-radius: 8px;
    padding: 12px; margin-bottom: 12px;
  }
  .row {
    display: flex; align-items: center; gap: 8px; padding: 8px 0;
    border-bottom: 1px solid var(--border); flex-wrap: wrap;
  }
  .row:last-child { border-bottom: 0; }
  .name { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12.5px; }
  .app { color: var(--muted); font-size: 11.5px; }
  .desc { color: var(--muted); font-size: 11.5px; }
  .grow { flex: 1; min-width: 120px; }
  button {
    background: #222735; color: var(--text); border: 1px solid var(--border);
    border-radius: 6px; padding: 5px 10px; font-size: 12px; cursor: pointer;
  }
  button:hover { background: #2b3243; }
  button.primary { background: var(--accent); border-color: var(--accent); color: #fff; }
  button.danger { color: var(--danger); }
  button:disabled { opacity: .45; cursor: default; }
  input[type=text] {
    background: #10131a; color: var(--text); border: 1px solid var(--border);
    border-radius: 6px; padding: 6px 8px; font-size: 12px; width: 100%;
  }
  label.toggle { display: flex; align-items: center; gap: 6px; font-size: 11.5px;
                 color: var(--muted); cursor: pointer; white-space: nowrap; }
  .pill { font-size: 10.5px; padding: 1px 7px; border-radius: 999px;
          white-space: nowrap; border: 1px solid var(--border); color: var(--muted); }
  /* The five states, visibly distinct. `never` and `indexable` differing is
     the point of this page, not a style choice (§7, card point f). */
  .pill.floor { border-color: var(--locked); color: var(--locked); }
  .pill.override { border-color: var(--accent); color: var(--accent); }
  .pill.declared_restricted { border-color: var(--ok); color: var(--ok); }
  .pill.declared_indexable { border-color: var(--review); color: var(--review); }
  .pill.never_declared { border-color: var(--warn); color: var(--warn); }
  .pill.unresolved { border-color: var(--danger); color: var(--danger); }
  .axes { font-family: ui-monospace, monospace; font-size: 11px; color: var(--muted);
          white-space: nowrap; }
  .axes b { color: var(--text); font-weight: 600; }
  /* Inline expansion — the no-modals constraint. Hidden until the row's own
     Edit/Clear button opens it; never a dialog. */
  .panel { display: none; width: 100%; padding: 8px 0 4px; }
  .panel.open { display: block; }
  .panelgrid { display: flex; gap: 8px; align-items: center; flex-wrap: wrap;
               margin-bottom: 6px; }
  .status { font-size: 11.5px; color: var(--muted); }
  .status.err { color: var(--danger); }
  .status.ok { color: var(--ok); }
  .banner { border: 1px solid var(--warn); color: var(--warn); border-radius: 6px;
            padding: 8px 10px; font-size: 11.5px; margin-bottom: 12px; }
  .banner.err { border-color: var(--danger); color: var(--danger); }
  .legend { display: flex; gap: 6px; flex-wrap: wrap; margin-bottom: 10px; }
  .counts { color: var(--muted); font-size: 11.5px; margin-bottom: 10px; }
  .filters { display: flex; gap: 8px; align-items: center; flex-wrap: wrap;
             margin-bottom: 10px; }
  .filters input[type=text] { max-width: 260px; }
  .raw { font-family: ui-monospace, monospace; font-size: 11.5px; word-break: break-all; }
</style>
</head>
<body>
<h2>Exec-indexing policy, per tool</h2>
<div class="sub">What the execution-graph sync may keep from each tool: its
<b>call</b> arguments and its <b>output</b>. Indexed unless something opts it
out. The four secret-store tools are a hard-coded floor no declaration and no
edit here can lift.</div>

<div id="banner" class="banner" style="display:none"></div>

<div class="card">
  <h3>Effective table</h3>
  <div class="legend" id="legend"></div>
  <div class="counts" id="counts">Loading…</div>
  <div class="filters">
    <input type="text" id="filter" placeholder="filter by app or tool">
    <label class="toggle"><input type="checkbox" id="only-reviewable"> hide never-declared</label>
    <button id="reload">Reload</button>
  </div>
  <div id="rows"></div>
</div>

<div class="card">
  <h3>Unresolved — observed, attributed to nothing, denied</h3>
  <div class="desc" style="margin-bottom:8px">Names the sync saw that parsed as
  MCP-routed but matched no installed app. Their arguments and results are
  dropped (fail closed). Raw spellings, verbatim — a new gateway prefix is
  meant to be spotted here. Acknowledging one is <b>not</b> permitting it: it
  stays denied, and only stops counting as something new to look at.</div>
  <div id="unresolved"><div class="status">Loading…</div></div>
</div>

<script>
const API = '/api/apps/knowledgeable';
let DATA = null;

function el(tag, cls, text) {
  const n = document.createElement(tag);
  if (cls) n.className = cls;
  if (text !== undefined) n.textContent = text;
  return n;
}

async function api(method, path, body) {
  const res = await fetch(API + path, {
    method,
    headers: body ? {'Content-Type': 'application/json'} : {},
    body: body ? JSON.stringify(body) : undefined,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.detail || data.error || (method + ' ' + path + ' -> ' + res.status));
  if (data && data.ok === false) throw new Error(data.error || 'refused');
  return data;
}

function banner(msg, isError) {
  const b = document.getElementById('banner');
  b.textContent = msg || '';
  b.className = 'banner' + (isError ? ' err' : '');
  b.style.display = msg ? 'block' : 'none';
}

function axesText(row) {
  const call = row.call ? 'call ✓' : 'call ✗';
  const out = row.output ? 'output ✓' : 'output ✗';
  const n = el('span', 'axes');
  n.append(el('b', null, call), document.createTextNode('  '), el('b', null, out));
  return n;
}

/* Every edit is an inline expansion, never a dialog: the host's iframe
   sandbox omits allow-modals, so window.confirm / window.prompt are dead no-ops here. */
function overrideEditor(row, onDone) {
  const panel = el('div', 'panel');
  const grid = el('div', 'panelgrid');
  const current = row.override || {call: row.call, output: row.output, note: ''};

  const callBox = el('input');
  callBox.type = 'checkbox';
  callBox.checked = !!current.call;
  const callLabel = el('label', 'toggle');
  callLabel.append(callBox, document.createTextNode('index call arguments'));

  const outBox = el('input');
  outBox.type = 'checkbox';
  outBox.checked = !!current.output;
  const outLabel = el('label', 'toggle');
  outLabel.append(outBox, document.createTextNode('index output'));

  const note = el('input');
  note.type = 'text';
  note.className = 'grow';
  note.placeholder = 'why (required when restricting)';
  note.value = current.note || '';

  grid.append(callLabel, outLabel, note);

  const actions = el('div', 'panelgrid');
  const save = el('button', 'primary', 'Save override');
  const clear = el('button', 'danger', 'Clear override');
  clear.disabled = !row.override;
  const cancel = el('button', null, 'Cancel');
  const status = el('div', 'status');
  actions.append(save, clear, cancel, status);

  /* A "clear" can WIDEN what gets indexed (the row falls back to the manifest
     or to the blacklist default), so it says so before it is pressed rather
     than reading as housekeeping. */
  const hint = el('div', 'desc');
  function refreshHint() {
    if (!row.override) { hint.textContent = ''; return; }
    const decl = row.declared;
    const after = decl
      ? ('the manifest declaration (call ' + (decl.call ? '✓' : '✗') +
         ', output ' + (decl.output ? '✓' : '✗') + ')')
      : 'the blacklist default — indexed on both axes';
    hint.textContent = 'Clearing this override falls back to ' + after + '.';
  }
  refreshHint();

  save.onclick = async () => {
    status.className = 'status';
    status.textContent = 'Saving…';
    save.disabled = true;
    try {
      await api('PUT', '/exec-policy/override', {
        app: row.app, tool: row.tool,
        call: callBox.checked, output: outBox.checked, note: note.value,
      });
      await onDone();
    } catch (e) {
      status.className = 'status err';
      status.textContent = e.message;
      save.disabled = false;
    }
  };
  clear.onclick = async () => {
    status.className = 'status';
    status.textContent = 'Clearing…';
    clear.disabled = true;
    try {
      await api('DELETE', '/exec-policy/override', {app: row.app, tool: row.tool});
      await onDone();
    } catch (e) {
      status.className = 'status err';
      status.textContent = e.message;
      clear.disabled = false;
    }
  };
  cancel.onclick = () => { panel.className = 'panel'; };

  panel.append(grid, hint, actions);
  return panel;
}

function rowNode(row, reload) {
  const wrap = el('div', 'row');
  const label = el('div', 'grow');
  label.append(el('div', 'name', row.tool));
  const meta = el('div', 'app');
  meta.textContent = (row.app || 'unattributed (harness or bare name)')
    + ' · ' + row.source
    + (row.listed_because && row.listed_because.length
        ? ' · listed from ' + row.listed_because.join(', ')
        : '');
  label.append(meta);
  if (row.override) {
    const prov = el('div', 'desc');
    prov.textContent = 'override by ' + row.override.actor
      + (row.override.at ? ' at ' + row.override.at : '')
      + (row.override.note ? ' — ' + row.override.note : '');
    label.append(prov);
  }
  if (row.declared && row.state === 'override') {
    label.append(el('div', 'desc',
      'manifest says call ' + (row.declared.call ? '✓' : '✗')
      + ', output ' + (row.declared.output ? '✓' : '✗')));
  }
  if (row.manifest_error) {
    label.append(el('div', 'desc',
      'this app\\u2019s aw-app.json could not be read (' + row.manifest_error
      + ') — every routed tool of it fails closed until that file is fixed'));
  }

  wrap.append(label, axesText(row), el('span', 'pill ' + row.state, row.state_label));

  if (row.locked) {
    /* No toggle at all, by design (§7). An invariant that offers a switch
       teaches an operator it is negotiable. */
    wrap.append(el('span', 'desc', 'locked'));
    return wrap;
  }
  const edit = el('button', null, row.override ? 'Edit override' : 'Override');
  const panel = overrideEditor(row, async () => { await reload(); });
  edit.onclick = () => {
    panel.className = panel.className.indexOf('open') >= 0 ? 'panel' : 'panel open';
  };
  wrap.append(edit, panel);
  return wrap;
}

/* Same inline-expansion shape for an acknowledgement, and a required reason:
   this action silences an alarm, so why it is safe has to be written down. */
function ackEditor(entry, reload) {
  const panel = el('div', 'panel');
  const grid = el('div', 'panelgrid');
  const reason = el('input');
  reason.type = 'text';
  reason.className = 'grow';
  reason.placeholder = 'why this upstream can never be attributed (required)';
  reason.value = (entry.acknowledged || {}).reason || '';
  const save = el('button', 'primary', 'Acknowledge — keep denying, stop counting');
  const cancel = el('button', null, 'Cancel');
  const status = el('div', 'status');
  grid.append(reason, save, cancel, status);
  save.onclick = async () => {
    status.className = 'status';
    status.textContent = 'Saving…';
    save.disabled = true;
    try {
      await api('PUT', '/exec-policy/ack',
                {upstream: entry.upstream, reason: reason.value});
      await reload();
    } catch (e) {
      status.className = 'status err';
      status.textContent = e.message;
      save.disabled = false;
    }
  };
  cancel.onclick = () => { panel.className = 'panel'; };
  panel.append(grid);
  return panel;
}

function unresolvedNode(entry, reload) {
  const wrap = el('div', 'row');
  const label = el('div', 'grow');
  label.append(el('div', 'raw', entry.name));
  const meta = el('div', 'app');
  meta.textContent = 'upstream ' + (entry.upstream || '(none — nothing to key on)')
    + ' · tool ' + (entry.tool || '(none)');
  label.append(meta);
  if (entry.reason) label.append(el('div', 'desc', entry.reason));
  if (entry.acknowledged) {
    label.append(el('div', 'desc',
      'acknowledged by ' + entry.acknowledged.actor
      + (entry.acknowledged.at ? ' at ' + entry.acknowledged.at : '')));
  }
  wrap.append(label);
  wrap.append(el('span', 'pill ' + (entry.known ? 'floor' : 'unresolved'),
                 entry.known ? 'known — still denied' : 'actionable'));
  if (!entry.acknowledgeable) {
    wrap.append(el('span', 'desc', 'no upstream segment to acknowledge'));
    return wrap;
  }
  if (entry.acknowledged) {
    const undo = el('button', 'danger', 'Withdraw');
    const status = el('div', 'status');
    undo.onclick = async () => {
      undo.disabled = true;
      status.textContent = 'Withdrawing…';
      try {
        await api('DELETE', '/exec-policy/ack', {upstream: entry.upstream});
        await reload();
      } catch (e) {
        status.className = 'status err';
        status.textContent = e.message;
        undo.disabled = false;
      }
    };
    wrap.append(undo, status);
    return wrap;
  }
  const ack = el('button', null, 'Acknowledge…');
  const panel = ackEditor(entry, reload);
  ack.onclick = () => {
    panel.className = panel.className.indexOf('open') >= 0 ? 'panel' : 'panel open';
  };
  wrap.append(ack, panel);
  return wrap;
}

function renderLegend(states) {
  const legend = document.getElementById('legend');
  legend.textContent = '';
  Object.keys(states).forEach((k) => {
    legend.append(el('span', 'pill ' + k, states[k]));
  });
}

function render() {
  const data = DATA;
  renderLegend(data.states || {});
  const c = data.counts || {};
  const bits = [
    c.rows + ' rows',
    c.floor + ' floor',
    c.override + ' override',
    c.declared_restricted + ' declared-restricted',
    c.declared_indexable + ' declared-indexable',
    c.never_declared + ' never declared',
  ];
  if (c.unresolved_names) {
    bits.push(c.unresolved_names + ' unresolved names ('
              + c.unresolved_actionable + ' actionable)');
  }
  const counts = document.getElementById('counts');
  counts.textContent = bits.join(' · ');
  if (data.observed_at) {
    counts.append(el('div', null, 'observed identities: ' + data.observed_count
      + ', last recorded ' + data.observed_at
      + (data.observed_capped ? ' (at the cap — some are not listed)' : '')));
  } else {
    counts.append(el('div', null,
      'the sync has not recorded any observed identities yet — rows below come '
      + 'from manifests only'));
  }

  const errs = data.store_errors || {};
  const keys = Object.keys(errs);
  if (keys.length) {
    banner('This page is showing less than the truth: '
      + keys.map((k) => k + ' → ' + errs[k]).join('; ')
      + '. A human override is the only layer above an app\\u2019s own declaration, '
      + 'so while the override store is unreadable the manifest/default governs. '
      + 'The hard-coded floor is unaffected.', true);
  } else if (data.table && data.table.scan_error) {
    banner('The installed apps tree could not be read (' + data.table.scan_error
      + ') — every MCP-routed name is failing closed right now.', true);
  } else {
    banner('');
  }

  const filter = document.getElementById('filter').value.trim().toLowerCase();
  const hideNever = document.getElementById('only-reviewable').checked;
  const host = document.getElementById('rows');
  host.textContent = '';
  let shown = 0;
  (data.rows || []).forEach((row) => {
    if (hideNever && row.state === 'never_declared') return;
    if (filter) {
      const hay = ((row.app || '') + ' ' + row.tool).toLowerCase();
      if (hay.indexOf(filter) < 0) return;
    }
    shown += 1;
    host.append(rowNode(row, load));
  });
  if (!shown) host.append(el('div', 'status', 'No rows match.'));

  const uhost = document.getElementById('unresolved');
  uhost.textContent = '';
  if (!(data.unresolved || []).length) {
    uhost.append(el('div', 'status',
      'Nothing unresolved on the last sync tick. Every routed name it saw '
      + 'resolved to an installed app.'));
  } else {
    (data.unresolved || []).forEach((entry) => uhost.append(unresolvedNode(entry, load)));
  }
}

async function load() {
  try {
    DATA = await api('GET', '/exec-policy');
    render();
  } catch (e) {
    banner('Could not load the policy table: ' + e.message, true);
    document.getElementById('counts').textContent = '';
    document.getElementById('rows').textContent = '';
    document.getElementById('unresolved').textContent = '';
  }
}

document.getElementById('reload').onclick = load;
document.getElementById('filter').oninput = () => { if (DATA) render(); };
document.getElementById('only-reviewable').onchange = () => { if (DATA) render(); };
load();
</script>
</body>
</html>
"""
