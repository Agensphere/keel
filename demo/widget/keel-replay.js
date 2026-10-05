/**
 * <keel-replay> — replays a recorded KEEL run in the browser. Zero dependencies.
 *
 *   <script type="module" src="keel-replay.js"></script>
 *   <keel-replay src="/keel/flagship.json" chaos-src="/keel/chaos-summary.json"></keel-replay>
 *
 * Attributes
 *   src         URL of a keel.recording/v1 JSON file (or put the JSON in a child
 *               <script type="application/json" data-recording> element).
 *   chaos-src   URL of a keel.chaos/v1 summary (or <script type="application/json" data-chaos>).
 *   speed       Playback speed multiplier (default 1).
 *   autoplay    Start playing when visible.
 *
 * Styling: every color, font and radius is a CSS custom property on the host (see :host below),
 * and the main regions are exposed as ::part(console | provider | diff | button | label).
 *
 * What it shows is a replay of recorded events, not a live engine. The label says so.
 */

const SCHEMA = "keel.recording/v1";

const css = `
/* Agensphere tokens (Rev 2026.10): paper/night grounds, one accent, square corners, 1px rules, no shadows.
   Fonts are loaded by the host page: Newsreader 300/400, IBM Plex Sans 400/500, IBM Plex Mono 400/500. */
:host {
  --keel-bg: #EDEBE5;
  --keel-surface: #EDEBE5;
  --keel-fg: #151514;
  --keel-soft: #3d3c38;
  --keel-muted: #66655F;
  --keel-rule: rgba(21,21,20,.14);
  --keel-border: #151514;
  --keel-accent: #D9481C;
  --keel-on-accent: #121211;
  --keel-accent-tint: rgba(217,72,28,.05);
  --keel-hatch: repeating-linear-gradient(135deg, rgba(21,21,20,.06) 0 1px, transparent 1px 10px);
  --keel-serif: 'Newsreader', Georgia, 'Times New Roman', serif;
  --keel-font: 'IBM Plex Sans', -apple-system, 'Segoe UI', Roboto, sans-serif;
  --keel-mono: 'IBM Plex Mono', ui-monospace, SFMono-Regular, Menlo, monospace;
  --keel-radius: 0;
  --keel-gap: 16px;
  --keel-log-height: 300px;
  display: block;
  font: 400 14px/1.5 var(--keel-font);
  color: var(--keel-fg);
  background: var(--keel-bg);
  border: 1px solid var(--keel-border);
  border-radius: var(--keel-radius);
  padding: var(--keel-gap);
}
@media (prefers-color-scheme: dark) {
  :host(:not([theme="light"])) {
    --keel-bg: #121211; --keel-surface: #121211; --keel-fg: #E9E7E1; --keel-soft: #C9C6BE; --keel-muted: #8E8C85;
    --keel-rule: rgba(233,231,225,.14); --keel-border: rgba(233,231,225,.4); --keel-accent-tint: rgba(217,72,28,.10);
    --keel-hatch: repeating-linear-gradient(135deg, rgba(233,231,225,.07) 0 1px, transparent 1px 10px);
  }
}
:host([theme="dark"]) {
  --keel-bg: #121211; --keel-surface: #121211; --keel-fg: #E9E7E1; --keel-soft: #C9C6BE; --keel-muted: #8E8C85;
  --keel-rule: rgba(233,231,225,.14); --keel-border: rgba(233,231,225,.4); --keel-accent-tint: rgba(217,72,28,.10);
  --keel-hatch: repeating-linear-gradient(135deg, rgba(233,231,225,.07) 0 1px, transparent 1px 10px);
}
* { box-sizing: border-box; border-radius: var(--keel-radius); }
.label, .panel h4, .sub.mono { font: 500 10.5px/1.3 var(--keel-mono); letter-spacing: .1em; text-transform: uppercase; color: var(--keel-muted); }
.kicker { font: 500 10.5px/1 var(--keel-mono); letter-spacing: .1em; color: var(--keel-accent); }
.top { display: flex; flex-wrap: wrap; gap: 12px; align-items: flex-end; justify-content: space-between; padding-bottom: 14px; margin-bottom: 14px; border-bottom: 1px solid var(--keel-border); }
.title { font: 300 26px/1.05 var(--keel-serif); letter-spacing: -.02em; margin-top: 6px; }
.title em { font-style: italic; }
.sub { color: var(--keel-soft); font-size: 13px; }
.grid { display: grid; grid-template-columns: minmax(0, 1.6fr) minmax(0, 1fr); gap: var(--keel-gap); }
@media (max-width: 760px) { .grid { grid-template-columns: 1fr; } }
.panel { background: var(--keel-surface); border: 1px solid var(--keel-border); padding: 12px; min-width: 0; }
.panel h4 { margin: 0 0 10px; padding-bottom: 8px; border-bottom: 1px solid var(--keel-rule); display: flex; justify-content: space-between; gap: 8px; }
.controls { display: flex; flex-wrap: wrap; gap: 8px; }
button { font: 500 11px/1 var(--keel-mono); letter-spacing: .08em; text-transform: uppercase; padding: 11px 13px; border: 1px solid var(--keel-border); background: transparent; color: var(--keel-fg); cursor: pointer; transition: background .15s ease, color .15s ease; }
button:hover:not(:disabled) { background: var(--keel-fg); color: var(--keel-bg); }
button:disabled { opacity: .4; cursor: default; }
button:focus-visible { outline: 2px solid var(--keel-accent); outline-offset: 2px; }
button.primary { background: var(--keel-fg); color: var(--keel-bg); }
button.primary:hover:not(:disabled) { background: transparent; color: var(--keel-fg); }
:host([theme="dark"]) button.primary { background: var(--keel-accent); color: var(--keel-on-accent); border-color: var(--keel-accent); }
@media (prefers-color-scheme: dark) { :host(:not([theme="light"])) button.primary { background: var(--keel-accent); color: var(--keel-on-accent); border-color: var(--keel-accent); } }
button .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; background: var(--keel-accent); margin-right: 7px; vertical-align: 0; }
button.pulse { animation: pulse 1s steps(2, jump-none) infinite; }
@keyframes pulse { 50% { background: var(--keel-accent-tint); border-color: var(--keel-accent); } }
.worker { font: 400 12px/1.4 var(--keel-mono); padding: 8px; margin-bottom: 10px; border: 1px solid var(--keel-rule); min-height: 34px; display: flex; gap: 8px; align-items: center; }
.worker::before { content: ""; width: 8px; height: 8px; border-radius: 50%; background: var(--keel-muted); flex: none; }
.worker.live::before { background: var(--keel-accent); }
.worker.dead { border: 1px dashed var(--keel-border); background: var(--keel-hatch); }
.worker.dead::before { background: transparent; border: 1px solid var(--keel-fg); }
.worker.ok { border-color: var(--keel-border); }
.worker.ok::before { content: "✓"; width: auto; height: auto; background: none; color: var(--keel-fg); }
ol.log { list-style: none; margin: 0; padding: 0; font: 400 12px/1.6 var(--keel-mono); max-height: var(--keel-log-height); overflow: auto; }
ol.log li { display: grid; grid-template-columns: 2.2em 6.5em 1fr; gap: 6px; padding: 3px 6px; border-bottom: 1px solid var(--keel-rule); border-left: 2px solid transparent; }
ol.log li .seq { color: var(--keel-muted); text-align: right; }
ol.log li .step { color: var(--keel-fg); font-weight: 500; }
ol.log li .what { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: var(--keel-soft); }
ol.log li.e1 { border-left: 2px dashed var(--keel-muted); }
ol.log li.e2 { border-left: 2px solid var(--keel-fg); }
ol.log li.e3 { border-left: 2px solid var(--keel-accent); background: var(--keel-accent-tint); }
ol.log li.note { grid-template-columns: 1fr; color: var(--keel-fg); background: var(--keel-hatch); }
ol.log li.info { grid-template-columns: 1fr; color: var(--keel-muted); font-style: italic; }
.refund { font: 400 12px/1.5 var(--keel-mono); padding: 10px; border: 1px solid var(--keel-border); margin-bottom: 8px; }
.refund b { font: 300 22px/1 var(--keel-serif); letter-spacing: -.01em; margin-right: 4px; }
.empty { min-height: 64px; display: grid; place-items: center; background: var(--keel-hatch); border: 1px dashed var(--keel-rule); color: var(--keel-muted); font: 500 10.5px/1 var(--keel-mono); letter-spacing: .1em; text-transform: uppercase; }
.counter { display: grid; grid-template-columns: repeat(3, 1fr); margin-top: 12px; border-top: 1px solid var(--keel-border); }
.counter div { padding: 10px 6px 4px 0; border-bottom: 1px solid var(--keel-rule); }
.counter b { display: block; font: 300 40px/1 var(--keel-serif); letter-spacing: -.03em; font-variant-numeric: tabular-nums; }
.counter span { font: 500 10px/1.2 var(--keel-mono); letter-spacing: .1em; text-transform: uppercase; color: var(--keel-muted); }
.hint { margin-top: 12px; font: italic 300 18px/1.3 var(--keel-serif); color: var(--keel-fg); min-height: 1.4em; }
.diff { margin-top: var(--keel-gap); display: none; }
.diff.show { display: block; }
.cols { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
@media (max-width: 760px) { .cols { grid-template-columns: 1fr; } }
.traj { font: 400 12px/1.7 var(--keel-mono); border-top: 1px solid var(--keel-rule); margin-top: 6px; }
.traj div { border-bottom: 1px solid var(--keel-rule); }
.traj .ins::before { content: "+ "; color: var(--keel-accent); }
.traj .del::before { content: "− "; }
.msg { font-size: 13.5px; padding: 10px; border: 1px solid var(--keel-rule); color: var(--keel-soft); margin-top: 6px; }
.bars { display: grid; gap: 10px; font: 400 12px/1.3 var(--keel-mono); align-content: start; }
.bar { display: grid; grid-template-columns: 7.5em 1fr 5.5em; align-items: center; gap: 8px; }
.bar .track { height: 8px; border: 1px solid var(--keel-rule); }
.bar .fill { height: 100%; background: var(--keel-fg); transition: width .3s ease; }
.bar .fill.b { background: var(--keel-accent); }
.bar .v { text-align: right; font-variant-numeric: tabular-nums; }
.key { display: inline-block; width: 10px; height: 10px; vertical-align: -1px; margin-right: 4px; background: var(--keel-fg); }
.key.b { background: var(--keel-accent); }
.badge { font: 500 10px/1 var(--keel-mono); letter-spacing: .1em; text-transform: uppercase; color: var(--keel-fg); }
.label-foot { margin-top: 14px; padding-top: 10px; border-top: 1px solid var(--keel-rule); font: 400 11.5px/1.5 var(--keel-mono); color: var(--keel-muted); }
.label-foot code { color: var(--keel-fg); }
@media (prefers-reduced-motion: reduce) { * { animation: none !important; transition: none !important; } }
`;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);
const usd = (micros) => `$${(micros / 1e6).toFixed(4)}`;
const money = (cents) => `$${(cents / 100).toFixed(2)}`;

function describe(ev) {
  const b = ev.body;
  switch (b.type) {
    case "RunStarted": return `run started · ${b.workflow}@${b.workflow_version}`;
    case "LlmRequested": return `ask ${b.request?.model ?? "model"}`;
    case "LlmCompleted": {
      const r = b.response || {};
      const calls = (r.tool_calls || []).map((c) => `→ ${c.name}`).join(" ");
      return calls || `“${(r.content || "").slice(0, 80)}${(r.content || "").length > 80 ? "…" : ""}”`;
    }
    case "EffectIntent": return `intent ${b.adapter} · key ${b.idem_key.slice(0, 13)}…${b.simulated ? " · simulated" : ""}`;
    case "EffectCommitted": {
      const o = b.output || {};
      if (o.id && o.amount_cents != null) return `${b.source} · ${o.id} ${money(o.amount_cents)}`;
      if (o.order_id) return `${b.source} · ${o.order_id} ${money(o.amount_cents)} ${o.status}`;
      return b.source;
    }
    case "EffectInDoubt": return `in doubt · ${b.reason}`;
    case "EffectAborted": return `rejected · ${b.error}`;
    case "SignalReceived": return `signal ${b.name} ${JSON.stringify(b.payload)}`;
    case "Forked": return `forked at ${b.from_step} · ${JSON.stringify(b.overrides)}`;
    case "RunCompleted": return `completed · ${b.output?.decision ?? ""}`;
    case "RunFailed": return `failed · ${b.error}`;
    default: return b.type;
  }
}

class KeelReplay extends HTMLElement {
  static get observedAttributes() { return ["src", "chaos-src"]; }

  constructor() {
    super();
    this.root = this.attachShadow({ mode: "open" });
    this.rec = null;
    this.chaos = null;
    this.token = 0;
    this.killResolve = null;
  }

  connectedCallback() {
    this.render();
    this.load().catch((e) => this.fail(e));
  }

  attributeChangedCallback(name, oldV, newV) {
    if (oldV !== null && oldV !== newV && this.isConnected) this.load().catch((e) => this.fail(e));
  }

  get speed() { return Math.max(0.1, parseFloat(this.getAttribute("speed") || "1")); }

  async readJson(attr, selector) {
    const inline = this.querySelector(selector);
    if (inline) return JSON.parse(inline.textContent);
    const url = this.getAttribute(attr);
    if (!url) return null;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`${url}: HTTP ${res.status}`);
    return res.json();
  }

  async load() {
    this.rec = await this.readJson("src", "script[data-recording]");
    this.chaos = await this.readJson("chaos-src", "script[data-chaos]");
    if (!this.rec || this.rec.schema !== SCHEMA) throw new Error(`expected a ${SCHEMA} recording`);
    this.main = this.rec.branches.find((b) => !b.parent_branch);
    this.fork = this.rec.branches.find((b) => b.parent_branch);
    this.kill = (this.rec.annotations || []).find((a) => a.kind === "worker_killed");
    this.reset();
    if (this.hasAttribute("autoplay")) {
      const io = new IntersectionObserver((es) => { if (es.some((e) => e.isIntersecting)) { io.disconnect(); this.play(); } });
      io.observe(this);
    }
  }

  fail(e) {
    this.$(".hint").textContent = `Could not load recording: ${e.message}`;
  }

  $(s) { return this.root.querySelector(s); }

  render() {
    this.root.innerHTML = `
      <style>${css}</style>
      <div class="top">
        <div><div class="kicker">PW-01 — KEEL · REFUND AGENT</div><div class="title">One crash. <em>One</em> refund.</div><div class="sub" id="ticket"></div></div>
        <div class="controls">
          <button class="primary" id="run" part="button">Run →</button>
          <button id="kill" part="button" disabled><span class="dot"></span>Kill worker</button>
          <button id="chaos" part="button" disabled>Run 20 · random kills</button>
          <button id="fork" part="button" disabled>Fork at triage#1</button>
          <button id="reset" part="button">Reset</button>
        </div>
      </div>
      <div class="grid">
        <section class="panel" part="console">
          <h4><span>Event log</span><span>branch <span id="branch" class="badge">main</span></span></h4>
          <div class="worker" id="worker">idle</div>
          <ol class="log" id="log"></ol>
        </section>
        <section class="panel" part="provider">
          <h4><span id="provider">Stripe (test)</span><span id="refundcount">0 refunds</span></h4>
          <div id="refunds"><div class="empty">No refunds</div></div>
          <div class="counter" id="counter" hidden>
            <div><b id="c-runs">0</b><span>runs</span></div>
            <div><b id="c-kills">0</b><span>workers killed</span></div>
            <div><b id="c-refunds">0</b><span>refunds</span></div>
          </div>
        </section>
      </div>
      <div class="hint" id="hint"></div>
      <section class="panel diff" id="diff" part="diff"></section>
      <div class="label-foot" part="label" id="label"></div>`;
    this.$("#run").onclick = () => this.play();
    this.$("#kill").onclick = () => this.killResolve?.();
    this.$("#chaos").onclick = () => this.playChaos();
    this.$("#fork").onclick = () => this.playFork();
    this.$("#reset").onclick = () => this.reset();
  }

  reset() {
    this.token++;
    this.killResolve = null;
    if (!this.rec) return;
    const input = this.rec.run.input || {};
    this.$("#ticket").textContent = input.body ? `${input.ticket_id} · ${input.order_id} · “${input.body}”` : "";
    this.$("#log").innerHTML = "";
    this.$("#refunds").innerHTML = `<div class="empty">No refunds</div>`;
    this.$("#refundcount").textContent = "0 refunds";
    this.$("#worker").className = "worker";
    this.$("#worker").textContent = "idle";
    this.$("#branch").textContent = "main";
    this.$("#diff").className = "panel diff";
    this.$("#counter").hidden = true;
    this.refundIds = new Set();
    for (const id of ["run"]) this.$(`#${id}`).disabled = false;
    for (const id of ["kill", "fork", "chaos"]) this.$(`#${id}`).disabled = true;
    this.$("#kill").classList.remove("pulse");
    const models = [...new Set(this.main.events.filter((e) => e.body.type === "LlmCompleted").map((e) => e.body.response?.provider))].join(", ");
    this.$("#provider").textContent = this.chaos?.provider && this.chaos.provider !== "stripe-test" ? "Payments · Stripe API fake" : "Stripe · test mode";
    const provider = this.chaos?.provider === "stripe-test" ? "Stripe test mode" : this.chaos?.provider ? "a local Stripe-compatible fake" : "the recorded provider";
    this.$("#label").innerHTML = `Replaying recorded runs (model: <code>${esc(models)}</code>, payments: ${esc(provider)}). The engine that produced them is in the repo: <code>keel replay</code> reproduces every step offline.`;
    this.hint("Press Run. Then press Kill worker while it’s on refund#1.");
  }

  hint(t) { this.$("#hint").textContent = t; }

  worker(text, cls = "") {
    const w = this.$("#worker");
    w.className = `worker ${cls}`;
    w.textContent = text;
  }

  line(html, cls = "") {
    const li = document.createElement("li");
    li.className = cls;
    li.innerHTML = html;
    const log = this.$("#log");
    log.appendChild(li);
    log.scrollTop = log.scrollHeight;
  }

  logEvent(ev, epochIndex) {
    this.line(
      `<span class="seq">${ev.seq}</span><span class="step">${esc(ev.body.step_id || "")}</span><span class="what" title="${esc(describe(ev))}">${esc(describe(ev))}</span>`,
      `e${Math.min(epochIndex, 3)}`,
    );
  }

  addRefund(r, note) {
    if (!r || this.refundIds.has(r.id)) return;
    this.refundIds.add(r.id);
    const box = this.$("#refunds");
    if (box.querySelector(".empty")) box.innerHTML = "";
    const d = document.createElement("div");
    d.className = "refund";
    d.innerHTML = `<b>${money(r.amount_cents)}</b> refunded · ${esc(r.id)}<br><span class="sub">${esc(r.payment_intent || "")}${note ? " · " + esc(note) : ""}</span>`;
    box.appendChild(d);
    this.$("#refundcount").textContent = `${this.refundIds.size} refund${this.refundIds.size === 1 ? "" : "s"}`;
  }

  async wait(ms, token) {
    await sleep(Math.min(ms, 1600) / this.speed);
    return token === this.token;
  }

  /** Play the main branch: worker 1 until the crash, the kill, lease expiry, worker 2 recovers. */
  async play() {
    const token = ++this.token;
    this.reset();
    this.token = token;
    this.$("#run").disabled = true;
    const evs = this.main.events;
    const epochs = [...new Set(evs.filter((e) => e.epoch > 0).map((e) => e.epoch))];
    const segs = this.main.workers || [];
    let prevAt = evs[0]?.at_ms ?? 0;
    let lastEpoch = 0;
    const killAfterSeq = this.kill && epochs.length > 1 ? segs[0]?.last_seq : null;

    for (const ev of evs) {
      if (ev.epoch !== lastEpoch && ev.epoch > 0) {
        const idx = epochs.indexOf(ev.epoch) + 1;
        const seg = segs.find((s) => s.epoch === ev.epoch);
        if (idx > 1) {
          this.worker(`worker-${String.fromCharCode(96 + idx)} claimed the run · lease epoch ${ev.epoch}`, "live");
          if (!(await this.wait(700, token))) return;
          const replayed = seg?.replayed_steps ?? 0;
          this.line(`replayed ${replayed} recorded step${replayed === 1 ? "" : "s"} from the log in milliseconds · 0 model calls · 0 tokens`, "info");
          if (!(await this.wait(900, token))) return;
          const intent = evs.find((e) => e.body.type === "EffectIntent" && e.seq <= (segs[0]?.last_seq ?? 0) && !evs.some((c) => c.body.type === "EffectCommitted" && c.body.step_id === e.body.step_id && c.seq <= (segs[0]?.last_seq ?? 0)));
          if (intent) this.line(`open intent ${esc(intent.body.step_id)} found · retrying with the same idempotency key`, "info");
        } else {
          this.worker(`worker-a holds the lease · epoch ${ev.epoch}`, "live");
        }
        lastEpoch = ev.epoch;
      }
      if (!(await this.wait(ev.at_ms - prevAt, token))) return;
      prevAt = ev.at_ms;
      this.logEvent(ev, Math.max(1, epochs.indexOf(ev.epoch) + 1));
      if (ev.body.type === "EffectCommitted" && ev.body.output?.id?.startsWith("re_")) {
        const dup = this.refundIds.has(ev.body.output.id);
        this.addRefund(ev.body.output);
        if (dup) this.line(`Stripe returned the existing refund ${esc(ev.body.output.id)} for that key · no second refund`, "info");
      }
      if (killAfterSeq != null && ev.seq === killAfterSeq) {
        if (!(await this.crash(token))) return;
        prevAt = evs.find((e) => e.seq === ev.seq + 1)?.at_ms ?? prevAt;
      }
    }
    this.worker("run completed", "ok");
    this.hint(this.kill ? "One crash, one refund. Now fork the run onto another model." : "Done. Fork the run onto another model.");
    this.$("#fork").disabled = !this.fork;
    this.$("#chaos").disabled = !this.chaos;
  }

  async crash(token) {
    const k = this.kill;
    const isRefund = (k.adapter || "").includes("refund");
    this.worker(isRefund ? "worker-a is calling Stripe for refund#1…" : "worker-a is working…", "live");
    if (k.point === "after_call" && k.provider_result) {
      this.addRefund(k.provider_result, "the provider executed this");
    }
    const kill = this.$("#kill");
    kill.disabled = false;
    kill.classList.add("pulse");
    this.hint("Kill the worker now: the refund reached Stripe but KEEL hasn’t recorded it yet.");
    await Promise.race([new Promise((r) => (this.killResolve = r)), sleep(7000 / this.speed)]);
    this.killResolve = null;
    kill.disabled = true;
    kill.classList.remove("pulse");
    if (token !== this.token) return false;
    this.worker(`worker-a ${k.signal === "SIGKILL" ? "kill -9" : "killed"} · pid ${k.pid}`, "dead");
    this.line(`worker-a killed: ${esc(k.reason)}. Its commit was never written.`, "note");
    this.hint("The lease is expiring. Any worker can pick the run up.");
    const lease = 3;
    for (let s = lease; s > 0; s--) {
      this.worker(`worker-a dead · lease expires in ${s}s`, "dead");
      if (!(await this.wait(1000, token))) return false;
    }
    return true;
  }

  async playChaos() {
    if (!this.chaos) return;
    const token = ++this.token;
    this.$("#chaos").disabled = true;
    this.$("#counter").hidden = false;
    let runs = 0, kills = 0, refunds = 0;
    const set = () => {
      this.$("#c-runs").textContent = runs;
      this.$("#c-kills").textContent = kills;
      this.$("#c-refunds").textContent = refunds;
    };
    set();
    this.hint(`${this.chaos.total_runs} real runs, each worker process kill -9’d at a random point, then audited at the provider.`);
    for (const r of this.chaos.runs) {
      if (!(await this.wait(350, token))) return;
      runs++;
      kills += r.killed ? 1 : 0;
      refunds += r.refunds_at_provider;
      set();
    }
    const dup = this.chaos.duplicates.length;
    this.hint(`${runs} runs · ${kills} workers killed · ${refunds} refunds · ${dup} duplicates · every run replays offline with zero external calls.`);
  }

  async playFork() {
    if (!this.fork) return;
    const token = ++this.token;
    this.$("#fork").disabled = true;
    const ov = this.fork.overrides || {};
    this.$("#branch").textContent = this.fork.label;
    this.$("#log").innerHTML = "";
    this.worker(`fork at ${this.fork.from_step} · model ${ov.model ?? "same"} · effects ${ov.effects ?? "inherit"}`, "live");
    this.hint("History before the fork point is shared, not copied. Refunds in this branch are simulated.");
    let prevAt = null;
    for (const ev of this.fork.events) {
      if (!(await this.wait(prevAt == null ? 300 : (ev.at_ms - prevAt) * 0.5, token))) return;
      prevAt = ev.at_ms;
      this.logEvent(ev, 3);
    }
    this.line(`Stripe still shows ${this.refundIds.size} refund${this.refundIds.size === 1 ? "" : "s"}: the fork moved no money.`, "info");
    this.showDiff();
  }

  showDiff() {
    const d = this.rec.diffs?.[0];
    if (!d) return;
    const traj = (d.trajectory || [])
      .map((op) => op.op === "equal" ? `<div>  ${esc(op.a)}</div>` : op.op === "insert" ? `<div class="ins">+ ${esc(op.b)}</div>` : `<div class="del">- ${esc(op.a)}</div>`)
      .join("");
    const last = (d.content || []).at(-1);
    const e = d.economics;
    const bar = (label, a, b, fmt) => {
      const max = Math.max(a, b, 1);
      return `<div class="bar"><span>${label}</span><div><div class="track"><div class="fill" style="width:${(a / max) * 100}%"></div></div><div class="track" style="margin-top:3px"><div class="fill b" style="width:${(b / max) * 100}%"></div></div></div><span class="v">${fmt(a)}<br>${fmt(b)}</span></div>`;
    };
    const o = d.outcome;
    this.$("#diff").innerHTML = `
      <h4><span>Diff ${esc(d.a)} ↔ ${esc(d.b)}</span><span class="badge">${o?.same_outcome ? "✓ same outcome" : "outcome changed"}</span></h4>
      <div class="cols">
        <div><div class="sub mono">Trajectory · aligned by step id</div><div class="traj">${traj}</div></div>
        <div class="bars">
          <div class="sub mono"><span class="key"></span>${esc(d.a)} &nbsp; <span class="key b"></span>${esc(d.b)}</div>
          ${bar("tokens", e.a.tokens_in + e.a.tokens_out, e.b.tokens_in + e.b.tokens_out, (v) => v.toLocaleString())}
          ${bar("cost", e.a.cost_micros, e.b.cost_micros, usd)}
          ${bar("model+tool", e.a.active_ms, e.b.active_ms, (v) => `${(v / 1000).toFixed(1)}s`)}
        </div>
      </div>
      ${last ? `<div class="cols" style="margin-top:12px"><div><div class="sub mono">${esc(d.a)} · ${esc(last.step_id)}</div><div class="msg">${esc(last.a)}</div></div><div><div class="sub mono">${esc(d.b)} · ${esc(last.step_id)}</div><div class="msg">${esc(last.b)}</div></div></div>` : ""}
      <div class="sub mono" style="margin-top:10px">tokens ${e.tokens_delta_pct > 0 ? "+" : ""}${e.tokens_delta_pct}% · cost ${e.cost_delta_pct > 0 ? "+" : ""}${e.cost_delta_pct}% · judge “${esc(o?.judge)}”: ${o?.same_outcome ? "same decision and amount" : "outcome changed: " + esc((o?.changed_fields || []).join(", "))}</div>`;
    this.$("#diff").className = "panel diff show";
    this.hint("Same outcome, different model, every number measured from the recorded runs.");
  }
}

if (!customElements.get("keel-replay")) customElements.define("keel-replay", KeelReplay);
export { KeelReplay };
