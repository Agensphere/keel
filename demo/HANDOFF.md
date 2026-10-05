# KEEL demo: handoff for the website

This is Stage 1 of the website plan: a **recorded replay** of real KEEL runs. It needs no backend, no keys and no build step.

## Files

| File | What it is |
| --- | --- |
| `widget/keel-replay.js` | The `<keel-replay>` web component. A plain ES module with zero dependencies (~20 KB unminified). |
| `widget/keel-replay.d.ts` | Typings for TS/React projects. |
| `widget/index.html` | A working example page. |
| `recordings/flagship.json` | One run: worker killed after Stripe executed the refund, recovered by a second worker, then forked onto another model and diffed. |
| `recordings/chaos-summary.json` | 20 runs with workers `kill -9`'d at random points, audited at the payment provider. |

> **The current recordings are placeholders.** They were made with KEEL's scripted model and a local Stripe-compatible fake. Before launch they will be re-recorded with Azure AI Foundry models and Stripe test mode; the file names and schema stay the same. The widget's footer reads the provider names from the recording, so the label stays accurate either way.

## Try it locally

```bash
python3 -m http.server 8765 -d demo
# open http://127.0.0.1:8765/widget/
```

## Embed

Host the two JSON files and the JS file anywhere static (CDN, `/public`).

```html
<script type="module" src="/keel/keel-replay.js"></script>
<keel-replay src="/keel/flagship.json" chaos-src="/keel/chaos-summary.json"></keel-replay>
```

**React / Next.js.** Import the module once on the client, for example `useEffect(() => { import("/keel/keel-replay.js") }, [])` or a `next/script` tag with `type="module"`. Then render `<keel-replay src="…" />`; React 19 passes the attributes through as-is. Add `keel-replay.d.ts` to get JSX typing.

**Astro / plain HTML.** Use the snippet as-is.

To avoid an extra request, inline the data instead of `src`:

```html
<keel-replay>
  <script type="application/json" data-recording>{ …flagship.json… }</script>
  <script type="application/json" data-chaos>{ …chaos-summary.json… }</script>
</keel-replay>
```

### Attributes

| Attribute | Default | |
| --- | --- | --- |
| `src` | – | URL of a `keel.recording/v1` file |
| `chaos-src` | – | URL of a `keel.chaos/v1` file; enables "Run 20 with random kills" |
| `speed` | `1` | playback speed multiplier |
| `autoplay` | off | starts when scrolled into view |
| `theme` | auto | `light` or `dark`; by default follows `prefers-color-scheme` |

## Styling to the brand

Everything is a CSS custom property on the element. Map the site's tokens onto these:

```css
keel-replay {
  --keel-bg: var(--site-bg);
  --keel-surface: var(--site-surface);
  --keel-fg: var(--site-text);
  --keel-muted: var(--site-text-muted);
  --keel-border: var(--site-border);
  --keel-accent: var(--site-accent);   /* main branch, primary button */
  --keel-ok: #2b8a3e;                  /* recovery worker, fork branch, refunds */
  --keel-warn: #b7791f;
  --keel-danger: #c92a2a;              /* kill button, crash */
  --keel-font: var(--site-font);
  --keel-mono: var(--site-mono);
  --keel-radius: 12px;
  --keel-gap: 16px;
}
keel-replay::part(console) { /* left panel */ }
keel-replay::part(provider) { /* Stripe panel */ }
keel-replay::part(diff) { /* fork diff */ }
keel-replay::part(button) { /* all buttons */ }
keel-replay::part(label) { /* honesty footer */ }
```

The layout collapses to one column below 720 px.

## What the visitor does

1. **Run.** Steps stream into the event log: `triage#1`, `lookup#1`, `triage#2`, then `refund#1` intent.
2. **Kill worker** (pulses while the refund is in flight). The Stripe panel shows the refund the provider just executed, the worker dies, and the lease counts down.
3. worker-b claims the run, replays 3 recorded steps with 0 model calls, retries with the same idempotency key, and Stripe returns the *existing* refund. The panel still shows one refund.
4. **Run 20 with random kills.** The counter animates 20 runs, 19 kills and 20 refunds from the chaos summary.
5. **Fork at triage#1.** The alt-model branch plays with the refund simulated, then the diff appears: trajectory, final messages, and token/cost/time bars.

If the visitor never presses Kill, the recorded crash happens on its own after a few seconds.

## Analytics hooks (suggested)

Wrap the buttons' clicks from outside (they are in shadow DOM, so listen on the element):

```js
document.querySelector("keel-replay").addEventListener("click", (e) => {
  const id = e.composedPath()[0]?.id; // run | kill | chaos | fork | reset
  if (id) analytics.track("keel_demo", { action: id });
});
```

## Copy that must stay

The footer label ("Replaying recorded runs … `keel replay` reproduces every step offline") is part of the honesty contract in the shipping plan. Restyle it, but keep it.
