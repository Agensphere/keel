# KEEL demo: handoff for the website

The site **links out** to a hosted demo page; it does not embed it. This folder *is* that page: a static site with no backend, no keys and no build step.

| Path | What it is |
| --- | --- |
| `index.html` | The hosted PW-01 page, built on the Agensphere tokens (paper/night, one accent, square, 1px rules). |
| `media/poster.png` | 1600×1000 poster for the §04 card frame (paper). Also `poster-dark.png` and `poster-diff.png`. |
| `media/keel-loop.mp4`, `.webm` | 1600×1000, 28 s loop for the card frame (crash → recover → 20 runs → fork → diff). |
| `PW-01.md` | Card fields and the seven-part teardown copy. |
| `widget/keel-replay.js` | The `<keel-replay>` web component the page uses. Zero dependencies, with `.d.ts` typings. |
| `recordings/` | `flagship.json` (`keel.recording/v1`) and `chaos-summary.json` (`keel.chaos/v1`). |
| `assets/logo-light.png` | The site's mark, used unmodified with `mix-blend-mode: multiply`. Hidden on night until we have `logo-dark-version.png`. |

**Hosting:** GitHub Pages from this repo (`.github/workflows/pages.yml`) at `keel.agensphere.com`. See `PW-01.md` → Hosting.

> **The current recordings are placeholders.** They were made with KEEL's scripted model and a local Stripe-compatible fake. Before launch they will be re-recorded with Azure AI Foundry models and Stripe test mode; the file names and schema stay the same. The page reads the provider names from the recording, so its label stays accurate either way. Posters and the loop are regenerated after re-recording.

## Try it locally

```bash
python3 -m http.server 8765 -d demo
# open http://127.0.0.1:8765/            (the page)
# open http://127.0.0.1:8765/?state=diff&theme=light   (jump to the finished state)
```

## Reusing the widget elsewhere (optional)

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

## Styling

The widget's defaults already match the Agensphere tokens (Rev 2026.10): paper `#EDEBE5` and night `#121211`, ink, signal orange `#D9481C` as the only accent, radius 0, 1px rules, no shadows, and success shown as ink with ✓. The host page loads the fonts (Newsreader, IBM Plex Sans, IBM Plex Mono). If `src/styles/tokens/` in `agensphere-web` changes, override the matching variables:

```css
keel-replay {
  --keel-bg: var(--bg);
  --keel-fg: var(--fg);
  --keel-soft: var(--fg-soft);
  --keel-muted: var(--mute);
  --keel-rule: var(--rule);
  --keel-border: var(--rule-strong);
  --keel-accent: var(--accent);
  --keel-accent-tint: var(--accent-tint);
  --keel-hatch: var(--hatch);
  --keel-serif: var(--font-serif);
  --keel-font: var(--font-sans);
  --keel-mono: var(--font-mono);
  --keel-log-height: 340px;
}
keel-replay::part(console) {}   /* event log panel */
keel-replay::part(provider) {}  /* payments panel */
keel-replay::part(diff) {}      /* fork diff */
keel-replay::part(button) {}    /* all buttons */
keel-replay::part(label) {}     /* honesty footer */
```

Below 760 px the layout collapses to one column.

## What the visitor does

1. **Run.** Steps stream into the event log: `triage#1`, `lookup#1`, `triage#2`, then `refund#1` intent.
2. **Kill worker** (pulses while the refund is in flight). The payments panel shows the refund the provider just executed, the worker dies, and the lease counts down.
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
