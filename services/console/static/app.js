/* avapuck console. Plain JS, no build step. Talks only to /api/*. */
(() => {
  const $ = (s, el = document) => el.querySelector(s);
  const $$ = (s, el = document) => Array.from(el.querySelectorAll(s));
  const esc = (s) => String(s ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
  const pill = (text, cls) => `<span class="pill ${cls || "neutral"}">${esc(text)}</span>`;
  const fmt = (iso) => iso ? new Date(iso).toISOString().replace("T", " ").replace(".000Z", "Z") : "";

  async function api(path, opts = {}) {
    const r = await fetch(path, { credentials: "same-origin", ...opts });
    if (r.status === 401 && path !== "/api/login") { showLogin(); throw new Error("not logged in"); }
    const body = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(body.detail || `${r.status}`);
    return body;
  }
  const json = (method, path, data) => api(path, { method, headers: { "Content-Type": "application/json" }, body: JSON.stringify(data) });

  // ── auth ──
  function showLogin() { $("#login").hidden = false; $("#tabs").hidden = true; $("#who").hidden = true; $$(".tab").forEach(t => t.hidden = true); }
  function showApp(user) {
    $("#login").hidden = true; $("#tabs").hidden = false; $("#who").hidden = false; $("#username").textContent = user;
    activate(localStorage.getItem("tab") || "upload");
  }
  $("#login-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const f = new FormData(e.target);
    $("#login-error").hidden = true;
    try {
      const me = await json("POST", "/api/login", { username: f.get("username"), password: f.get("password") });
      showApp(me.username);
    } catch (err) { $("#login-error").textContent = err.message; $("#login-error").hidden = false; }
  });
  $("#logout").addEventListener("click", async () => { await json("POST", "/api/logout", {}); showLogin(); });

  // ── tabs ──
  const loaders = { upload: () => Promise.all([loadSource(), loadPipelines()]), pipelines: loadPipelines,
                    runs: loadRuns, kafka: loadKafka, spark: loadSpark, traffic: loadTraffic };
  function activate(name) {
    $$("#tabs button").forEach(b => b.classList.toggle("active", b.dataset.tab === name));
    $$(".tab").forEach(t => t.hidden = t.id !== `tab-${name}`);
    localStorage.setItem("tab", name);
    loaders[name]?.().catch(console.error);
  }
  $$("#tabs button").forEach(b => b.addEventListener("click", () => activate(b.dataset.tab)));
  $$("[data-refresh]").forEach(b => b.addEventListener("click", () => loaders[b.dataset.refresh]().catch(console.error)));

  // ── source ──
  async function loadSource() {
    const s = await api("/api/source");
    $("#source-line").textContent = `EXTRACT_MODE=${s.extract_mode} · BigQuery ${s.bigquery_configured ? "configured" : "not configured"} · upload cap ${s.upload_max_rows || "none"}`;
    $("#source-note").textContent = s.note;
  }

  // ── pipelines: every DAG, three presets ──
  let pipelines = [];
  function renderPipelines() {
    const preset = $("#preset").value;
    const shown = pipelines.filter(p => preset === "all" || (preset === "responding" && p.responding) || (preset === "connected" && p.connected));
    const sel = $("#dag-select"), keep = sel.value;
    sel.innerHTML = shown.map(p => `<option value="${esc(p.dag_id)}">${esc(p.dag_id)} · ${p.kind}${p.responding ? " · 200" : " · " + p.api_status}${p.connected ? " · connected" : ""}${p.paused ? " · paused" : ""}</option>`).join("")
      || `<option value="" disabled>none in this preset</option>`;
    if ([...sel.options].some(o => o.value === keep)) sel.value = keep;
    else if (shown.some(p => p.dag_id === "trips_pipeline")) sel.value = "trips_pipeline";
    describePipeline();
  }
  function describePipeline() {
    const p = pipelines.find(x => x.dag_id === $("#dag-select").value);
    $("#pipeline-line").innerHTML = p
      ? `${pill(p.kind, "neutral")} schedule <code>${esc(p.schedule || "manual")}</code> · API ${pill(p.api_status, p.responding ? "ok" : "bad")} · ${pill(p.connected ? "connected" : "not connected", p.connected ? "ok" : "warn")} <span class="muted">${esc(p.connection_reason)}</span>${p.last_run ? ` · last run ${pill(p.last_run.state, p.last_run.state)} ${esc(fmt(p.last_run.end_date))}` : ""}`
      : "";
  }
  async function loadPipelines() {
    const d = await api("/api/pipelines");
    pipelines = d.pipelines;
    const o = $("#preset").options;
    o[0].textContent = `All pipelines (${d.presets.all})`;
    o[1].textContent = `Responding 200 (${d.presets.responding})`;
    o[2].textContent = `Connected (${d.presets.connected}) · watchdog window ${d.watchdog.window}`;
    renderPipelines();
  }
  $("#preset").addEventListener("change", renderPipelines);
  $("#dag-select").addEventListener("change", describePipeline);

  // ── upload ──
  let upload = null;
  const dz = $("#dropzone"), fileInput = $("#file-input");
  ["dragenter", "dragover"].forEach(ev => dz.addEventListener(ev, e => { e.preventDefault(); dz.classList.add("over"); }));
  ["dragleave", "drop"].forEach(ev => dz.addEventListener(ev, e => { e.preventDefault(); dz.classList.remove("over"); }));
  dz.addEventListener("drop", e => { const f = e.dataTransfer.files[0]; if (f) sendFile(f); });
  fileInput.addEventListener("change", () => { if (fileInput.files[0]) sendFile(fileInput.files[0]); });

  function sendFile(file) {
    const prog = $("#upload-progress"), bar = $(".bar", prog), label = $(".label", prog), out = $("#upload-result");
    prog.hidden = false; out.hidden = true; bar.style.width = "0%"; label.textContent = file.name;
    const fd = new FormData(); fd.append("file", file);
    const xhr = new XMLHttpRequest();
    xhr.open("POST", "/api/upload");
    xhr.upload.onprogress = e => { if (e.lengthComputable) bar.style.width = `${Math.round(100 * e.loaded / e.total)}%`; };
    xhr.onload = () => {
      let body = {}; try { body = JSON.parse(xhr.responseText); } catch {}
      if (xhr.status === 200) {
        upload = body;
        out.className = "result ok"; out.hidden = false;
        out.innerHTML = `Uploaded <strong>${esc(body.name)}</strong> (${(body.bytes / 1048576).toFixed(2)} MB) as <code>${esc(body.file_id)}</code>. Suggested dataset: <code>${esc(body.dataset_suggestion)}</code>.`;
        $("#run-form [name=dataset]").placeholder = body.dataset_suggestion;
        $("#run-button").disabled = false;
      } else {
        if (xhr.status === 401) return showLogin();
        out.className = "result bad"; out.hidden = false; out.textContent = body.detail || `upload failed (${xhr.status})`;
      }
    };
    xhr.onerror = () => { out.className = "result bad"; out.hidden = false; out.textContent = "upload failed"; };
    xhr.send(fd);
  }

  $("#run-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    if (!upload) return;
    const f = new FormData(e.target), out = $("#run-result");
    const data = { file_id: upload.file_id };
    if (f.get("dag_id")) data.dag_id = f.get("dag_id");
    if (f.get("dataset")) data.dataset = f.get("dataset");
    if (f.get("row_limit") !== "") data.row_limit = Number(f.get("row_limit"));
    $("#run-button").disabled = true;
    try {
      const r = await json("POST", "/api/run", data);
      out.className = "result ok"; out.hidden = false;
      out.innerHTML = `Run <code>${esc(r.dag_run_id)}</code> of <code>${esc(r.dag_id)}</code> is ${pill(r.state || "queued", r.state)}. Dataset <code>${esc(r.conf.dataset)}</code>, cap ${r.conf.row_limit || "none"}. <a href="${esc(r.airflow_url)}" target="_blank" rel="noopener">Open in Airflow</a> · watch the Runs tab.`;
    } catch (err) { out.className = "result bad"; out.hidden = false; out.textContent = err.message; }
    finally { $("#run-button").disabled = false; }
  });

  // ── runs ──
  async function loadRuns() {
    const { runs } = await api("/api/runs");
    $("#runs-table tbody").innerHTML = runs.map(r => {
      const v = r.verdict;
      return `<tr>
        <td>${esc(r.dag_id || "")}</td>
        <td class="mono">${esc(r.dag_run_id)}</td>
        <td>${pill(r.source, r.source === "upload" ? "ok" : "neutral")}</td>
        <td>${esc(r.dataset || "")}</td>
        <td>${pill(r.state, r.state)}</td>
        <td>${v ? pill(v.proceed ? "proceed" : "do not proceed", v.proceed ? "ok" : "bad") + " " + pill(v.status, v.status) : pill("no verdict yet", "neutral")}</td>
        <td style="white-space:normal">${v ? esc((v.reasons || []).join("; ")) : ""}</td>
        <td class="mono">${esc(fmt(r.logical_date))}</td>
      </tr>`;
    }).join("") || `<tr><td colspan="8" class="muted">No runs yet.</td></tr>`;
  }

  // ── kafka ──
  let kafkaOffset = 0; const kafkaLimit = 50;
  async function loadKafka() {
    const f = new FormData($("#kafka-filters"));
    const q = new URLSearchParams({ limit: kafkaLimit, offset: kafkaOffset });
    for (const [k, v] of f.entries()) if (v) q.set(k, v);
    const d = await api(`/api/kafka-log?${q}`);
    $("#kafka-counts").textContent = Object.entries(d.counts).map(([k, v]) => `${k}: ${v}`).join(" · ") || "no messages yet";
    $("#kafka-table tbody").innerHTML = d.rows.map(r => `<tr>
      <td class="mono">${r.id}</td><td class="mono">${esc(fmt(r.received_at))}</td><td>${esc(r.topic)}</td>
      <td>${pill(r.event_type, r.event_type === "run.verdict" ? "ok" : r.event_type === "invalid" ? "bad" : "neutral")}</td>
      <td>${esc(r.dataset || "")}</td><td class="mono">${esc(fmt(r.run_slot))}</td><td class="mono">${esc(r.key || "")}</td>
      <td class="mono">${esc((r.event_id || "").slice(0, 12))}</td>
      <td class="payload">${esc(JSON.stringify(r.payload)).slice(0, 400)}</td></tr>`).join("")
      || `<tr><td colspan="9" class="muted">Nothing matches.</td></tr>`;
    $("#kafka-page").textContent = `${d.offset + 1}–${Math.min(d.offset + kafkaLimit, d.total)} of ${d.total}`;
    $("#kafka-prev").disabled = kafkaOffset === 0; $("#kafka-next").disabled = kafkaOffset + kafkaLimit >= d.total;
  }
  $("#kafka-filters").addEventListener("submit", e => { e.preventDefault(); kafkaOffset = 0; loadKafka().catch(console.error); });
  $("#kafka-prev").addEventListener("click", () => { kafkaOffset = Math.max(0, kafkaOffset - kafkaLimit); loadKafka(); });
  $("#kafka-next").addEventListener("click", () => { kafkaOffset += kafkaLimit; loadKafka(); });

  // ── spark ──
  async function loadSpark() {
    const s = await api("/api/spark");
    const m = s.master;
    $("#spark-summary").textContent = `${m.alive_workers} alive · ${m.cores_used}/${m.cores} cores · ${m.active_apps} running, ${m.waiting_apps} waiting${m.error ? " · master unreachable" : ""}`;
    $("#workers").innerHTML = s.workers.map(w => {
      const pct = w.cores ? Math.round(100 * (w.cores_used || 0) / w.cores) : 0;
      return `<div class="worker ${w.health}">
        <div class="row between"><strong>${esc(w.host)}</strong>${pill(w.health, w.health)}</div>
        <div class="id" title="${esc(w.id)}">${esc(w.id)}</div>
        <div>${w.cores_used || 0}/${w.cores} cores · ${w.memory_used_mb || 0}/${w.memory_mb} MB</div>
        <div class="meter"><i style="width:${pct}%"></i></div>
        <div class="muted small">state ${esc(w.state)}</div>
      </div>`;
    }).join("") || `<p class="muted">No workers reported by the master.</p>`;
    const d = s.last_decision;
    $("#scaler-line").textContent = `${s.enabled ? "enabled" : "disabled"} · socket ${s.socket_mounted ? "reachable" : "unreachable: " + (s.docker_error || "")} · min ${s.limits.min_workers}, max ${s.limits.max_workers}, up ≥ ${Math.round(s.limits.up_threshold * 100)}%, down ≤ ${Math.round(s.limits.down_threshold * 100)}%, cooldown ${s.limits.cooldown_seconds}s` + (d ? ` · last: ${d.action} (${d.reason})` : "") + (s.manual_target != null ? ` · manual target ${s.manual_target}` : "");
    $("#scaler-history").innerHTML = s.history.map(h => `<li><span class="mono">${esc(fmt(h.at))}</span> ${pill(h.action, h.action === "up" ? "ok" : h.action === "down" ? "warn" : "neutral")} ${esc(h.reason)}${h.detail ? " · " + esc(h.detail) : ""}</li>`).join("") || "<li class='muted'>No decisions yet.</li>";
  }
  $("#scale-form").addEventListener("submit", async e => {
    e.preventDefault();
    try { await json("POST", "/api/spark/scale", { workers: Number(new FormData(e.target).get("workers")) }); await loadSpark(); }
    catch (err) { alert(err.message); }
  });

  // ── traffic ──
  async function loadTraffic() {
    const t = await api("/api/traffic");
    const badge = $("#attack-badge");
    badge.textContent = t.under_attack ? "ANOMALOUS LOAD" : "within profile";
    badge.className = `badge ${t.under_attack ? "bad" : "ok"}`;
    $("#traffic-table tbody").innerHTML = Object.entries(t.categories).map(([name, c]) => `<tr>
      <td>${esc(name)}</td><td class="mono">${c.hits_in_window}</td><td class="mono">${c.total_per_window}</td><td class="mono">${c.per_client_per_window}</td>
      <td>${c.anomaly_until_seconds > 0 ? pill(`refused ${c.anomaly_until_seconds}s`, "bad") : pill("ok", "ok")}</td>
      <td class="mono small">${c.top_clients.map(([ip, n]) => `${esc(ip)} (${n})`).join(", ")}</td></tr>`).join("");
    $("#blocked").innerHTML = Object.entries(t.blocked_clients).map(([ip, b]) => `<li><span class="mono">${esc(ip)}</span> · ${esc(b.category)} · ${b.seconds_left}s left</li>`).join("") || "<li class='muted'>None.</li>";
    $("#alerts").innerHTML = t.recent_alerts.map(a => `<li><span class="mono">${esc(new Date(a.at * 1000).toISOString())}</span> ${pill(a.kind, a.kind.includes("blocked") || a.kind === "traffic_anomaly" ? "bad" : "ok")} ${esc(Object.entries(a).filter(([k]) => !["kind", "at"].includes(k)).map(([k, v]) => `${k}=${v}`).join(" "))}</li>`).join("") || "<li class='muted'>None.</li>";
  }

  // ── boot ──
  api("/api/me").then(me => showApp(me.username)).catch(showLogin);
  setInterval(() => { const t = localStorage.getItem("tab"); if (!$("#tabs").hidden && (t === "spark" || t === "traffic" || t === "runs")) loaders[t]().catch(() => {}); }, 15000);
})();
