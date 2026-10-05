/** <keel-replay src="…recording.json" chaos-src="…chaos-summary.json" speed="1" autoplay theme="light|dark"> */
export declare class KeelReplay extends HTMLElement {}
declare global {
  interface HTMLElementTagNameMap {
    "keel-replay": KeelReplay;
  }
}
