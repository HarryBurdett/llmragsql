#!/usr/bin/env python3
"""Build the crakd.ai commercial decks for bank-reconcile and gocardless.

One shell, several decks. The shell (crakd.ai brand CSS + the narration and
calculator engine) is proven in apassist-commercial-presentation.html; keeping
it here means a brand or behaviour fix lands on every deck at once instead of
being copy-pasted three times.

Output (self-contained, assets inlined as data URIs because the Artifact CSP
blocks external requests):
    ../demos/gocardless-commercial-presentation.html
    ../demos/bank-reconciliation-commercial-presentation.html

Facts checked against the repos on 2026-08-18 — bank-reconcile 2.8.76,
gocardless 2.0.71, both running on Opera SE and Opera 3 through the shared
opera-write library and the Opera 3 write agent.

Honesty rules, deliberate and load-bearing:
  * no invented ROI or performance figures — the money slide is a calculator
    the prospect fills in with their own numbers;
  * customer and supplier names are invented, never a real client's;
  * every capability claimed was located in the app's own source.

Usage:  python3 build-decks.py
"""
import base64
import pathlib
import sys

HERE = pathlib.Path(__file__).parent
BRAND = HERE.parent / "brand"
SHOTS = HERE.parent / "screenshots"
DEMOS = HERE.parent / "demos"

# ---------------------------------------------------------------------------
# Assets
# ---------------------------------------------------------------------------

def data_uri(path: pathlib.Path, mime: str) -> str:
    if not path.exists():
        sys.exit(f"missing asset: {path}")
    return f"data:{mime};base64," + base64.b64encode(path.read_bytes()).decode()


def assets() -> dict:
    return {
        "FONT_JAKARTA": data_uri(BRAND / "jakarta.woff2", "font/woff2"),
        "FONT_JETBRAINS": data_uri(BRAND / "jetbrains.woff2", "font/woff2"),
        "LOGO_CRAKD": data_uri(BRAND / "logo-crakd-ai.png", "image/png"),
        "LOGO_OPERA": data_uri(BRAND / "pegasus-opera-logo.png", "image/png"),
    }


def shot(name: str) -> str:
    return data_uri(SHOTS / name, "image/png")


# ---------------------------------------------------------------------------
# Shared shell
# ---------------------------------------------------------------------------

CSS = """
  @font-face { font-family:'Plus Jakarta Sans'; src:url(__FONT_JAKARTA__) format('woff2'); font-weight:200 800; font-display:block; }
  @font-face { font-family:'JetBrains Mono'; src:url(__FONT_JETBRAINS__) format('woff2'); font-weight:400 500; font-display:block; }

  :root {
    --paper:#FFFFFF; --paper-tint:#F8FAFC; --paper-deep:#F1F5F9;
    --ink:#020617; --ink-muted:#475569; --ink-faint:#64748B;
    --line:#E2E8F0; --line-soft:#EFF2F7;
    --indigo:#4338CA; --cyan:#0E7490;
    --indigo-wash:rgba(67,56,202,0.07);
    --grad:linear-gradient(135deg,#4338CA,#0E7490);
    --good-ink:#15803D; --good-bg:#DCFCE7; --good-line:#86EFAC;
    --info-ink:#1D4ED8; --info-bg:#DBEAFE; --info-line:#93C5FD;
    --warn-ink:#92400E; --warn-bg:#FEF3C7; --warn-line:#FCD34D;
    --bad-ink:#B91C1C;  --bad-bg:#FEE2E2;  --bad-line:#FCA5A5;
    --sans:'Plus Jakarta Sans',system-ui,-apple-system,'Segoe UI',sans-serif;
    --mono:'JetBrains Mono',ui-monospace,'SF Mono',Consolas,monospace;
    --pad:clamp(1.4rem,3.4vw,4.2rem);
  }

  * { box-sizing:border-box; }
  html { scroll-behavior:smooth; }
  body { margin:0; background:var(--paper); color:var(--ink); font-family:var(--sans);
         font-size:clamp(15px,1vw,17px); line-height:1.6; -webkit-font-smoothing:antialiased; }

  @media (prefers-reduced-motion:reduce) { html{scroll-behavior:auto;} *{animation:none!important;transition:none!important;} }

  .deck { scroll-snap-type:y mandatory; overflow-y:auto; height:100vh; }
  .slide { scroll-snap-align:start; min-height:100vh; padding:var(--pad); display:flex;
           flex-direction:column; justify-content:center; border-bottom:1px solid var(--line-soft);
           background:var(--paper); }
  .slide:nth-child(even) { background:var(--paper-tint); }
  .slide-inner { width:100%; max-width:1180px; margin:0 auto; }

  .rail { position:fixed; right:clamp(0.6rem,1.5vw,1.5rem); top:50%; transform:translateY(-50%);
          display:flex; flex-direction:column; gap:0.5rem; z-index:20; }
  .rail a { display:block; width:8px; height:8px; border-radius:50%; background:#CBD5E1;
            border:1px solid transparent; transition:background 160ms ease,transform 160ms ease; }
  .rail a:hover,.rail a:focus-visible { background:var(--indigo); transform:scale(1.4); outline:none; border-color:var(--indigo); }
  .rail a.is-here { background:var(--indigo); }

  .stamp,.ip-line { position:fixed; bottom:clamp(0.85rem,2vw,1.4rem); font-family:var(--mono);
                    font-size:0.64rem; color:var(--ink-faint); z-index:20; }
  .stamp { left:clamp(0.9rem,2vw,1.7rem); letter-spacing:0.13em; text-transform:uppercase; }
  .ip-line { right:clamp(0.9rem,2vw,1.7rem); letter-spacing:0.04em; }
  @media (max-width:900px) { .rail,.stamp,.ip-line { display:none; } }

  .eyebrow { font-family:var(--mono); font-size:0.68rem; letter-spacing:0.18em; text-transform:uppercase;
             color:var(--indigo); margin:0 0 1rem; }
  .eyebrow-plain { color:var(--ink-faint); }

  h1,h2,h3 { text-wrap:balance; margin:0; }
  h1 { font-size:clamp(2.4rem,5.6vw,4.6rem); line-height:1.03; letter-spacing:-0.04em; font-weight:800; }
  h2 { font-size:clamp(1.6rem,3.1vw,2.7rem); line-height:1.12; letter-spacing:-0.03em; font-weight:700; margin-bottom:1.2rem; }
  h3 { font-size:1rem; font-weight:700; margin-bottom:0.3rem; letter-spacing:-0.01em; }

  .grad-text { background:var(--grad); -webkit-background-clip:text; background-clip:text; color:transparent; }
  .lede { font-size:clamp(1rem,1.35vw,1.28rem); color:var(--ink-muted); max-width:62ch; line-height:1.55; }
  .note { font-size:0.83rem; color:var(--ink-faint); max-width:74ch; }

  .pill { display:inline-block; background:var(--indigo-wash); border:1px solid rgba(67,56,202,0.22);
          color:var(--indigo); padding:0.4rem 1rem; border-radius:999px; font-size:0.82rem;
          font-weight:600; width:fit-content; }

  .stack { display:flex; flex-direction:column; gap:1.35rem; }
  .stack-tight { display:flex; flex-direction:column; gap:0.8rem; }
  .grid-2 { display:grid; grid-template-columns:repeat(auto-fit,minmax(280px,1fr)); gap:1rem; }
  .grid-3 { display:grid; grid-template-columns:repeat(auto-fit,minmax(225px,1fr)); gap:1rem; }
  .split { display:grid; grid-template-columns:minmax(0,1fr) minmax(0,1fr); gap:clamp(1.3rem,3.4vw,3rem); align-items:center; }
  .split-wide { grid-template-columns:minmax(0,1.25fr) minmax(0,1fr); }
  @media (max-width:900px) { .split,.split-wide { grid-template-columns:minmax(0,1fr); } }

  .panel { background:var(--paper); border:1px solid var(--line); border-radius:12px;
           padding:1.15rem 1.3rem; box-shadow:0 1px 2px rgba(2,6,23,0.04); }
  .panel-quiet { background:transparent; border:1px solid var(--line); border-radius:12px; padding:1.05rem 1.2rem; }
  .panel-lead { border-left:3px solid var(--indigo); }
  .panel p,.panel-quiet p { margin:0; color:var(--ink-muted); font-size:0.9rem; }

  /* align-self is load-bearing: in a flex column, align-items:stretch makes
     width:auto resolve to the full column width and overrides the intrinsic
     aspect ratio, rendering the logo stretched across the slide. */
  .wordmark { height:clamp(26px,2.4vw,34px); width:auto; align-self:flex-start; display:block; }

  .opera-lockup { display:flex; align-items:center; gap:0.85rem; background:var(--paper);
                  border:1px solid var(--line); border-radius:10px; padding:0.6rem 0.95rem; width:fit-content; }
  .opera-lockup img { height:clamp(34px,3.4vw,46px); width:auto; display:block; }
  .opera-lockup span { font-family:var(--mono); font-size:0.68rem; letter-spacing:0.09em;
                       text-transform:uppercase; color:var(--ink-faint); line-height:1.5; }

  figure { margin:0; }
  /* Both dimensions auto with two maxima: the aspect ratio is kept and the
     element box ends up exactly the rendered image, so no letterboxed plate
     shows through beside it. */
  figure img { display:block; width:auto; height:auto; max-width:100%; max-height:54vh;
               border:1px solid var(--line); border-radius:12px; background:var(--paper-tint);
               box-shadow:0 16px 36px rgba(2,6,23,0.10); }
  /* Fixed height, auto width: the house captures have differing aspect ratios,
     so matching on height keeps the captions on one line instead of stepping. */
  .shots figure img { height:clamp(150px,30vh,255px); max-height:none; width:auto; }
  figcaption { font-family:var(--mono); font-size:0.64rem; letter-spacing:0.08em; text-transform:uppercase;
               color:var(--ink-faint); margin-top:0.65rem; }
  .shots { display:grid; grid-template-columns:repeat(auto-fit,minmax(300px,1fr)); gap:1.4rem; }

  .thread { background:var(--paper); border:1px solid var(--line); border-radius:12px; overflow:hidden; }
  .thread-head { padding:0.6rem 1rem; border-bottom:1px solid var(--line); font-family:var(--mono);
                 font-size:0.68rem; letter-spacing:0.07em; color:var(--ink-faint); text-transform:uppercase;
                 background:var(--paper-deep); }
  .thread-body { padding:1rem 1.1rem; font-size:0.92rem; }
  .quote { border-left:2px solid var(--line); padding-left:0.85rem; color:var(--ink-muted); font-size:0.92rem; }
  .quote strong { color:var(--ink); }

  .chip { display:inline-block; font-family:var(--mono); font-size:0.66rem; letter-spacing:0.04em;
          padding:0.16rem 0.5rem; border-radius:999px; border:1px solid; white-space:nowrap; font-weight:500; }
  .chip-settled { color:var(--good-ink); background:var(--good-bg); border-color:var(--good-line); }
  .chip-flight { color:var(--info-ink); background:var(--info-bg); border-color:var(--info-line); }
  .chip-hold { color:var(--warn-ink); background:var(--warn-bg); border-color:var(--warn-line); }
  .chip-bad { color:var(--bad-ink); background:var(--bad-bg); border-color:var(--bad-line); }
  .chip-none { color:var(--ink-muted); background:var(--paper-deep); border-color:var(--line); }
  .chip-row { display:flex; flex-wrap:wrap; gap:0.38rem; margin-bottom:0.7rem; }

  .flow { display:flex; flex-direction:column; gap:0.35rem; }
  .flow-step { display:grid; grid-template-columns:2.1rem minmax(0,1fr); gap:0.85rem; align-items:start;
               padding:0.68rem 0; border-bottom:1px solid var(--line-soft); }
  .flow-step:last-child { border-bottom:none; }
  .flow-num { font-family:var(--mono); font-size:0.68rem; color:var(--indigo); padding-top:0.26rem; font-weight:500; }
  .flow-step h3 { margin-bottom:0.1rem; }
  .flow-step p { margin:0; color:var(--ink-muted); font-size:0.88rem; }

  .ladder { display:flex; flex-direction:column; gap:0.45rem; }
  .rung { display:grid; grid-template-columns:minmax(0,1fr) auto; gap:1rem; align-items:center;
          background:var(--paper); border:1px solid var(--line); border-left:3px solid var(--indigo);
          border-radius:10px; padding:0.75rem 1rem; box-shadow:0 1px 2px rgba(2,6,23,0.04); }
  .rung p { margin:0; font-size:0.88rem; color:var(--ink-muted); }
  .rung strong { color:var(--ink); font-weight:700; }
  .rung-tag { font-family:var(--mono); font-size:0.64rem; letter-spacing:0.09em; text-transform:uppercase;
              color:var(--indigo); white-space:nowrap; }

  .figs { display:grid; grid-template-columns:repeat(auto-fit,minmax(145px,1fr)); gap:1rem; }
  .fig { border-top:2px solid var(--line); padding-top:0.8rem; }
  .fig-n { font-family:var(--mono); font-variant-numeric:tabular-nums; font-size:clamp(1.5rem,2.7vw,2.1rem);
           color:var(--indigo); line-height:1; letter-spacing:-0.02em; font-weight:500; }
  .fig-l { font-size:0.78rem; color:var(--ink-muted); margin-top:0.35rem; }

  .calc { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:1rem; }
  .calc label { display:block; font-size:0.74rem; color:var(--ink-muted); margin-bottom:0.35rem; font-weight:600; }
  .calc input { width:100%; background:var(--paper); border:1px solid var(--line); border-radius:8px;
                color:var(--ink); font-family:var(--mono); font-variant-numeric:tabular-nums;
                font-size:1rem; padding:0.5rem 0.65rem; }
  .calc input:focus-visible { outline:2px solid var(--indigo); outline-offset:1px; border-color:var(--indigo); }
  .readout { margin-top:1.4rem; background:var(--paper); border:1px solid var(--line);
             border-left:3px solid var(--indigo); border-radius:12px; padding:1.15rem 1.3rem;
             display:grid; grid-template-columns:repeat(auto-fit,minmax(155px,1fr)); gap:1.1rem;
             box-shadow:0 1px 2px rgba(2,6,23,0.04); }
  .readout .fig { border-top:none; padding-top:0; }
  .readout .fig-n { color:var(--ink); }
  .readout .fig-n.hero { background:var(--grad); -webkit-background-clip:text; background-clip:text; color:transparent; }

  .ticks { list-style:none; margin:0; padding:0; display:flex; flex-direction:column; gap:0.62rem; }
  /* NB: do NOT make these list items grid containers. They mix a <strong> with a
     following text node, and a grid container promotes each fragment to its own
     grid item — the text then wraps one row per fragment and the list grows to
     several thousand pixels. The marker is absolutely positioned instead. */
  .ticks li { position:relative; padding-left:1.5rem; font-size:0.9rem; color:var(--ink-muted); }
  .ticks li::before { content:""; position:absolute; left:0; top:0.4rem; width:0.85rem; height:0.85rem;
                      border-radius:50%; background:var(--indigo-wash); border:1px solid rgba(67,56,202,0.3); }
  .ticks li::after { content:""; position:absolute; left:0.29rem; top:0.64rem; width:0.28rem; height:0.14rem;
                     border-left:1.5px solid var(--indigo); border-bottom:1.5px solid var(--indigo);
                     transform:rotate(-45deg); }
  .ticks li strong { color:var(--ink); font-weight:700; }

  .bars { list-style:none; margin:0; padding:0; display:flex; flex-direction:column; gap:0.65rem; }
  .bars li { border-left:3px solid var(--bad-line); padding-left:0.85rem; font-size:0.9rem; color:var(--ink-muted); }
  .bars li strong { color:var(--ink); font-weight:700; display:block; }

  .rule { height:1px; background:var(--line); border:none; margin:0; }
  .footer-line { display:flex; flex-wrap:wrap; gap:0.5rem 1.4rem; font-family:var(--mono); font-size:0.68rem;
                 letter-spacing:0.09em; text-transform:uppercase; color:var(--ink-faint); }
  .legal { font-size:0.72rem; line-height:1.6; color:var(--ink-faint); max-width:82ch; }
  .legal p { margin:0 0 0.5rem; }
  .legal p:last-child { margin-bottom:0; }

  a { color:var(--indigo); }
  a:focus-visible { outline:2px solid var(--indigo); outline-offset:2px; }

  /* Short viewports (1280x720 projectors, small laptops). */
  @media (max-height:800px) {
    :root { --pad:clamp(1.1rem,2.5vw,2.8rem); }
    h1 { font-size:clamp(2rem,4.6vw,3.4rem); }
    h2 { font-size:clamp(1.4rem,2.5vw,2rem); margin-bottom:0.85rem; }
    .lede { font-size:0.98rem; }
    figure img { max-height:46vh; }
    .shots figure img { height:clamp(140px,26vh,210px); }
    .flow-step { padding:0.48rem 0; }
    .panel,.panel-quiet { padding:0.88rem 1rem; }
    .rung { padding:0.6rem 0.9rem; }
    .stack { gap:0.95rem; }
    .legal { font-size:0.68rem; }
  }

  .bar { position:fixed; left:50%; bottom:clamp(0.7rem,1.8vw,1.4rem); transform:translateX(-50%);
         display:flex; align-items:center; gap:0.5rem; background:rgba(255,255,255,0.94);
         border:1px solid var(--line); border-radius:999px; padding:0.38rem 0.48rem 0.38rem 0.55rem;
         z-index:30; backdrop-filter:blur(8px); box-shadow:0 6px 20px rgba(2,6,23,0.10); }
  .bar button { font-family:var(--mono); font-size:0.66rem; letter-spacing:0.08em; text-transform:uppercase;
                color:var(--ink-muted); background:transparent; border:1px solid var(--line);
                border-radius:999px; padding:0.34rem 0.7rem; cursor:pointer; font-weight:500;
                transition:color 140ms ease,border-color 140ms ease,background 140ms ease; }
  .bar button:hover { color:var(--ink); border-color:#CBD5E1; }
  .bar button.on { color:#FFFFFF; background:var(--indigo); border-color:var(--indigo); }
  .bar button:focus-visible { outline:2px solid var(--indigo); outline-offset:2px; }
  .bar-status { font-family:var(--mono); font-size:0.63rem; letter-spacing:0.08em; text-transform:uppercase;
                color:var(--ink-faint); padding:0 0.45rem 0 0.15rem; min-width:10.5rem; }
  @media (max-width:560px) { .bar-status { display:none; } }
"""

# The engine: narrated auto-advance, calculator, keyboard control.
# Non-ASCII is escaped so the money figures survive being served without a
# charset header.
JS = """
  (function () {
    var gbp = function (n) { return '\\u00A3' + Math.round(n).toLocaleString('en-GB'); };
    var MIDDOT = ' \\u00B7 ';

    var year = new Date().getFullYear();
    Array.prototype.forEach.call(document.querySelectorAll('.yr'), function (el) { el.textContent = year; });

    // ---- calculator (each deck supplies its own compute()) ----
    var calcInputs = Array.prototype.slice.call(document.querySelectorAll('.calc input'));
    function num(id) { return Math.max(0, Number(document.getElementById(id).value) || 0); }
    function put(id, text) { var el = document.getElementById(id); if (el) { el.textContent = text; } }
    function recalc() { if (window.__compute) { window.__compute(num, put, gbp); } }
    calcInputs.forEach(function (el) { el.addEventListener('input', recalc); });
    recalc();

    // ---- position ----
    var deck = document.getElementById('deck');
    var slides = Array.prototype.slice.call(document.querySelectorAll('.slide'));
    var dots = Array.prototype.slice.call(document.querySelectorAll('.rail a'));
    var stamp = document.getElementById('stamp');
    var total = slides.length;
    function pad(n) { return (n < 10 ? '0' : '') + n; }

    // ---- narration ----
    var NARRATION = window.__narration || [];
    var speech = ('speechSynthesis' in window) && ('SpeechSynthesisUtterance' in window);
    var playing = false, muted = false, idx = 0, advanceTimer = null, utterance = null, voice = null;
    var btnPlay = document.getElementById('btn-play');
    var btnMute = document.getElementById('btn-mute');
    var status = document.getElementById('bar-status');
    var SILENT_DWELL = 11000, GAP_AFTER_SPEECH = 1200;

    function pickVoice() {
      if (!speech) { return; }
      var voices = window.speechSynthesis.getVoices() || [];
      // Same preference order as the other house demos: a British voice.
      voice = voices.filter(function (v) {
        return v.name.indexOf('Daniel') > -1 ||
               v.name.indexOf('Google UK English Male') > -1 ||
               v.name.indexOf('English (United Kingdom)') > -1;
      })[0] || voices.filter(function (v) { return /^en-GB/i.test(v.lang); })[0] || null;
    }
    if (speech) { pickVoice(); window.speechSynthesis.onvoiceschanged = pickVoice; }

    function stopSpeech() {
      if (!speech) { return; }
      try { window.speechSynthesis.cancel(); } catch (err) { /* nothing useful to do */ }
      utterance = null;
    }
    function clearAdvance() { if (advanceTimer) { clearTimeout(advanceTimer); advanceTimer = null; } }
    function setStatus(t) { if (status) { status.textContent = t; } }

    function render() {
      btnPlay.textContent = playing ? 'Pause' : (idx === 0 ? 'Play narrated' : 'Resume');
      btnPlay.setAttribute('aria-pressed', playing ? 'true' : 'false');
      btnPlay.classList.toggle('on', playing);
      btnMute.textContent = muted ? 'Sound off' : 'Sound on';
      btnMute.setAttribute('aria-pressed', muted ? 'true' : 'false');
      if (!playing) {
        setStatus(idx === 0 ? 'Space to play' + MIDDOT + 'arrows to move' : 'Paused' + MIDDOT + 'space to resume');
      } else if (muted || !speech) {
        setStatus('Auto-advancing' + MIDDOT + 'space to pause');
      } else {
        setStatus('Narrating ' + pad(idx + 1) + ' of ' + pad(total));
      }
    }

    function queueAdvance(ms) {
      clearAdvance();
      advanceTimer = setTimeout(function () {
        if (!playing) { return; }
        if (idx >= total - 1) { playing = false; render(); return; }
        go(idx + 1);
      }, ms);
    }

    // Advance when the speech finishes rather than on a fixed timer: length
    // varies by voice and rate, so a timer either clips or dawdles.
    function playCurrent() {
      clearAdvance(); stopSpeech();
      if (!playing) { return; }
      var script = NARRATION[idx];
      if (muted || !speech || !script) { queueAdvance(SILENT_DWELL); render(); return; }
      utterance = new window.SpeechSynthesisUtterance(script);
      utterance.rate = 0.95; utterance.pitch = 1; utterance.volume = 1;
      if (voice) { utterance.voice = voice; }
      utterance.onend = function () { if (playing) { queueAdvance(GAP_AFTER_SPEECH); } };
      // If the browser refuses to speak (autoplay policy, missing voice),
      // don't strand the deck on one slide.
      utterance.onerror = function () { if (playing) { queueAdvance(SILENT_DWELL); } };
      window.speechSynthesis.speak(utterance);
      render();
    }

    function play() { playing = true; playCurrent(); render(); }
    function pause() { playing = false; clearAdvance(); stopSpeech(); render(); }
    function toggle() { if (playing) { pause(); } else { play(); } }

    btnPlay.addEventListener('click', toggle);
    btnMute.addEventListener('click', function () { muted = !muted; if (playing) { playCurrent(); } render(); });

    if ('IntersectionObserver' in window) {
      var obs = new IntersectionObserver(function (entries) {
        entries.forEach(function (e) {
          if (!e.isIntersecting) { return; }
          var i = slides.indexOf(e.target);
          if (i < 0 || i === idx) { return; }
          idx = i;
          dots.forEach(function (d, di) { d.classList.toggle('is-here', di === i); });
          if (stamp) { stamp.textContent = pad(i + 1) + ' / ' + pad(total); }
          if (playing) { playCurrent(); } else { render(); }
        });
      }, { root: deck, threshold: 0.5 });
      slides.forEach(function (s) { obs.observe(s); });
    }

    function current() {
      var best = 0, bestDist = Infinity;
      slides.forEach(function (s, i) {
        var d = Math.abs(s.getBoundingClientRect().top);
        if (d < bestDist) { bestDist = d; best = i; }
      });
      return best;
    }
    function go(i) { slides[Math.min(total - 1, Math.max(0, i))].scrollIntoView({ behavior: 'smooth', block: 'start' }); }

    document.addEventListener('keydown', function (e) {
      var tag = (e.target && e.target.tagName) || '';
      if (tag === 'INPUT' || tag === 'TEXTAREA') { return; }
      if (e.key === ' ' || e.key === 'Spacebar') { e.preventDefault(); toggle(); }
      else if (e.key === 'm' || e.key === 'M') { e.preventDefault(); btnMute.click(); }
      else if (e.key === 'ArrowDown' || e.key === 'ArrowRight' || e.key === 'PageDown') { e.preventDefault(); go(current() + 1); }
      else if (e.key === 'ArrowUp' || e.key === 'ArrowLeft' || e.key === 'PageUp') { e.preventDefault(); go(current() - 1); }
      else if (e.key === 'Home') { e.preventDefault(); go(0); }
      else if (e.key === 'End') { e.preventDefault(); go(total - 1); }
    });

    window.addEventListener('beforeunload', stopSpeech);
    render();
  })();
"""

LEGAL = """
      <div class="legal">
        <p>
          &copy; <span class="yr">2026</span> crakd.ai. All rights reserved. This software &mdash; including its
          design, code, workflows and documentation &mdash; is the intellectual property of crakd.ai and is
          protected by copyright and other intellectual property laws. It is licensed for use, not sold.
          Unauthorised copying, distribution, modification or reverse engineering is prohibited.
        </p>
        <p>
          Pegasus Opera 3 and Opera SE are trademarks of Pegasus Software. This product is an independent
          integration and is not affiliated with or endorsed by Pegasus Software.
        </p>
      </div>
"""


def build(deck: dict) -> pathlib.Path:
    a = assets()
    css = CSS.replace("__FONT_JAKARTA__", a["FONT_JAKARTA"]).replace("__FONT_JETBRAINS__", a["FONT_JETBRAINS"])
    slides = deck["slides"](a)
    n = len(slides)
    if n != len(deck["narration"]):
        sys.exit(f"{deck['file']}: {n} slides but {len(deck['narration'])} narration scripts")

    rail_parts = []
    for i in range(n):
        here = ' class="is-here"' if i == 0 else ''
        rail_parts.append(f'<a href="#s{i+1}" aria-label="Slide {i+1}"{here}></a>')
    rail = "".join(rail_parts)
    body = "".join(f'\n  <section class="slide" id="s{i+1}">\n    <div class="slide-inner">{s}</div>\n  </section>\n'
                   for i, s in enumerate(slides))
    # Decks carry the token rather than importing LEGAL, which would be circular.
    body = body.replace("__LEGAL__", LEGAL)
    narration = ",\n      ".join(f'"{s}"' for s in deck["narration"])

    html = f"""<meta charset="utf-8">
<title>{deck['title']}</title>
<!--
  {deck['title']} — commercial presentation.  GENERATED FILE.

  Do not hand-edit: rebuild with  python3 ../generators/build-decks.py
  Content and narration live in that script; the brand shell is shared with the
  other crakd.ai decks so a fix lands on all of them at once.

  {deck['provenance']}
-->
<style>{css}</style>

<nav class="rail" aria-label="Slides">{rail}</nav>
<div class="stamp" id="stamp">01 / {n:02d}</div>
<div class="ip-line">&copy; <span class="yr">2026</span> crakd.ai &mdash; All rights reserved.</div>

<div class="bar" role="group" aria-label="Presentation controls">
  <button id="btn-play" type="button" aria-pressed="false">Play narrated</button>
  <button id="btn-mute" type="button" aria-pressed="false" title="Mute narration (M)">Sound on</button>
  <span class="bar-status" id="bar-status" aria-live="polite">Space to play &middot; arrows to move</span>
</div>

<main class="deck" id="deck">{body}</main>

<script>
  window.__narration = [
      {narration}
  ];
  window.__compute = {deck['compute']};
</script>
<script>{JS}</script>
"""
    out = DEMOS / deck["file"]
    out.write_text(html, encoding="utf-8")
    kb = out.stat().st_size // 1024
    if kb > 16 * 1024:
        sys.exit(f"{out.name} is {kb} KB — over the 16 MB Artifact limit")
    print(f"  {out.name:52s} {n} slides  {kb} KB")
    return out


# ---------------------------------------------------------------------------
# Deck content
# ---------------------------------------------------------------------------
from deck_gocardless import DECK as GOCARDLESS  # noqa: E402
from deck_bankrec import DECK as BANKREC        # noqa: E402

if __name__ == "__main__":
    DEMOS.mkdir(exist_ok=True)
    print("building crakd.ai commercial decks:")
    for d in (GOCARDLESS, BANKREC):
        build(d)
    print("done")
