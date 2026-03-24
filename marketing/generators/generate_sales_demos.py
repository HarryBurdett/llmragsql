#!/usr/bin/env python3
"""
Generate premium sales demos for GoCardless and Bank Reconciliation.
Produces self-contained HTML files with embedded Opera logos,
voice narration, and responsive design for desktop + mobile.
"""
import base64
import sys
from pathlib import Path

DEMOS_DIR = Path(__file__).parent
LOGO_DIR = Path(__file__).parent.parent / "frontend" / "public"


def load_logo_b64(name: str) -> str:
    path = LOGO_DIR / name
    if not path.exists():
        return ""
    return base64.b64encode(path.read_bytes()).decode()


SE_LOGO = load_logo_b64("opera-se-logo.png")
O3_LOGO = load_logo_b64("opera3-logo.png")

# ─────────────────────────────────────────────────────────
# Shared CSS + JS framework
# ─────────────────────────────────────────────────────────

SHARED_CSS = r"""
:root {
  --bg: #0a0e1a;
  --surface: #111827;
  --card: #1a2236;
  --border: #283348;
  --accent: #3b82f6;
  --accent2: #06b6d4;
  --accent3: #8b5cf6;
  --green: #10b981;
  --amber: #f59e0b;
  --text: #e2e8f0;
  --muted: #94a3b8;
  --dim: #64748b;
  --radius: 16px;
}
* { margin:0; padding:0; box-sizing:border-box; }
html, body { height:100%; overflow:hidden; }
body {
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
  background: var(--bg);
  color: var(--text);
  -webkit-font-smoothing: antialiased;
}

/* Header bar */
.topbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 10px 24px;
  background: rgba(17,24,39,0.95);
  backdrop-filter: blur(12px);
  border-bottom: 1px solid var(--border);
  position: fixed; top:0; left:0; right:0; z-index:100;
  height: 52px;
}
.topbar-left { display:flex; align-items:center; gap:10px; }
.topbar-left img { height:28px; }
.topbar-left .brand {
  font-size: 0.75rem;
  font-weight: 600;
  color: var(--muted);
  text-transform: uppercase;
  letter-spacing: 1.5px;
}
.topbar-right { display:flex; align-items:center; gap:8px; }
.topbar-right button {
  background: var(--card);
  border: 1px solid var(--border);
  color: var(--muted);
  padding: 5px 10px;
  border-radius: 8px;
  font-size: 0.75rem;
  cursor: pointer;
  transition: all 0.2s;
}
.topbar-right button:hover { background: var(--border); color: var(--text); }
.topbar-right button.active { background: var(--accent); color: white; border-color: var(--accent); }
.topbar-right .speed {
  font-size: 0.7rem;
  color: var(--dim);
  min-width: 32px;
  text-align: center;
}

/* Progress */
.progress-track {
  position: fixed; top:52px; left:0; right:0; height:3px;
  background: var(--border); z-index:99;
}
.progress-fill {
  height:100%; background: linear-gradient(90deg, var(--accent), var(--accent2));
  transition: width 0.6s ease;
  border-radius: 0 2px 2px 0;
}

/* Slide container */
.slides {
  position: fixed;
  top: 55px; left: 0; right: 0; bottom: 0;
  overflow: hidden;
}
.slide {
  position: absolute;
  inset: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  opacity: 0;
  transform: scale(0.96);
  transition: opacity 0.6s ease, transform 0.6s ease;
  pointer-events: none;
  padding: 24px;
}
.slide.active {
  opacity: 1;
  transform: scale(1);
  pointer-events: auto;
}

/* Title slide */
.title-slide {
  text-align: center;
  max-width: 800px;
}
.title-slide .logos {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 24px;
  margin-bottom: 32px;
}
.title-slide .logos img { height: 48px; }
.title-slide .logos .divider {
  width: 1px; height: 40px;
  background: var(--border);
}
.title-slide h1 {
  font-size: clamp(1.8rem, 4vw, 3rem);
  font-weight: 800;
  line-height: 1.1;
  margin-bottom: 16px;
  background: linear-gradient(135deg, #fff 0%, var(--accent2) 100%);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
}
.title-slide .subtitle {
  font-size: clamp(1rem, 2vw, 1.3rem);
  color: var(--muted);
  line-height: 1.5;
  max-width: 600px;
  margin: 0 auto 24px;
}
.title-slide .badge {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  background: rgba(59,130,246,0.15);
  border: 1px solid rgba(59,130,246,0.3);
  color: var(--accent);
  padding: 6px 16px;
  border-radius: 20px;
  font-size: 0.8rem;
  font-weight: 600;
}

/* Content slide */
.content-slide {
  width: 100%;
  max-width: 960px;
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 32px;
  align-items: center;
}
.content-left {
  padding-right: 16px;
}
.content-left .step-badge {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 0.7rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 1.5px;
  color: var(--accent2);
  margin-bottom: 12px;
}
.content-left .step-badge .dot {
  width: 6px; height: 6px;
  border-radius: 50%;
  background: var(--accent2);
}
.content-left h2 {
  font-size: clamp(1.4rem, 3vw, 2rem);
  font-weight: 700;
  line-height: 1.2;
  margin-bottom: 12px;
  color: #fff;
}
.content-left p {
  font-size: 0.95rem;
  color: var(--muted);
  line-height: 1.6;
  margin-bottom: 16px;
}
.content-right {
  background: var(--card);
  border-radius: var(--radius);
  border: 1px solid var(--border);
  padding: 20px;
  min-height: 260px;
}

/* Visual elements */
.stat-row {
  display: flex;
  gap: 12px;
  margin-bottom: 12px;
}
.stat-card {
  flex: 1;
  background: var(--surface);
  border-radius: 10px;
  padding: 14px;
  border: 1px solid var(--border);
  text-align: center;
}
.stat-card .value {
  font-size: 1.4rem;
  font-weight: 700;
  color: var(--green);
}
.stat-card .label {
  font-size: 0.7rem;
  color: var(--dim);
  margin-top: 2px;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

.feature-list {
  list-style: none;
}
.feature-list li {
  display: flex;
  align-items: flex-start;
  gap: 10px;
  padding: 8px 0;
  font-size: 0.88rem;
  color: var(--muted);
  border-bottom: 1px solid rgba(40,51,72,0.5);
}
.feature-list li:last-child { border-bottom: none; }
.feature-list .icon {
  width: 20px; height: 20px;
  border-radius: 50%;
  display: flex; align-items: center; justify-content: center;
  font-size: 0.65rem;
  flex-shrink: 0;
  margin-top: 1px;
}
.icon-green { background: rgba(16,185,129,0.2); color: var(--green); }
.icon-blue { background: rgba(59,130,246,0.2); color: var(--accent); }
.icon-amber { background: rgba(245,158,11,0.2); color: var(--amber); }
.icon-purple { background: rgba(139,92,246,0.2); color: var(--accent3); }

.mock-table {
  width: 100%;
  font-size: 0.75rem;
  border-collapse: collapse;
}
.mock-table th {
  text-align: left;
  padding: 6px 8px;
  color: var(--dim);
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  font-size: 0.65rem;
  border-bottom: 1px solid var(--border);
}
.mock-table td {
  padding: 7px 8px;
  color: var(--muted);
  border-bottom: 1px solid rgba(40,51,72,0.4);
}
.mock-table .amount { color: var(--green); font-weight: 600; font-variant-numeric: tabular-nums; }
.mock-table .neg { color: #f87171; }
.mock-table .match { color: var(--accent); }
.tag {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 0.65rem;
  font-weight: 600;
}
.tag-green { background: rgba(16,185,129,0.15); color: var(--green); }
.tag-blue { background: rgba(59,130,246,0.15); color: var(--accent); }
.tag-amber { background: rgba(245,158,11,0.15); color: var(--amber); }

.flow-step {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 10px 14px;
  background: var(--surface);
  border-radius: 10px;
  margin-bottom: 8px;
  border: 1px solid var(--border);
}
.flow-num {
  width: 28px; height: 28px;
  border-radius: 50%;
  background: linear-gradient(135deg, var(--accent), var(--accent2));
  color: white;
  display: flex; align-items: center; justify-content: center;
  font-size: 0.75rem; font-weight: 700;
  flex-shrink: 0;
}
.flow-text {
  font-size: 0.82rem;
  color: var(--muted);
}
.flow-text strong { color: var(--text); }

/* CTA slide */
.cta-slide {
  text-align: center;
  max-width: 700px;
}
.cta-slide h2 {
  font-size: clamp(1.6rem, 3.5vw, 2.4rem);
  font-weight: 800;
  margin-bottom: 16px;
  color: #fff;
}
.cta-slide p {
  font-size: 1rem;
  color: var(--muted);
  line-height: 1.6;
  margin-bottom: 24px;
}
.cta-slide .logos {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 20px;
  margin-top: 24px;
}
.cta-slide .logos img { height: 40px; opacity: 0.8; }
.cta-slide .logos .plus {
  color: var(--dim);
  font-size: 1.2rem;
}
.cta-badge {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  background: linear-gradient(135deg, var(--accent), var(--accent2));
  color: white;
  padding: 10px 24px;
  border-radius: 12px;
  font-size: 0.9rem;
  font-weight: 600;
  box-shadow: 0 4px 20px rgba(59,130,246,0.3);
}

/* Pill navigation dots */
.nav-dots {
  position: fixed;
  bottom: 20px;
  left: 50%;
  transform: translateX(-50%);
  display: flex;
  gap: 6px;
  z-index: 100;
}
.nav-dot {
  width: 8px; height: 8px;
  border-radius: 50%;
  background: var(--border);
  transition: all 0.3s;
  cursor: pointer;
  border: none;
}
.nav-dot.active {
  background: var(--accent);
  width: 24px;
  border-radius: 4px;
}

/* Responsive - mobile */
@media (max-width: 768px) {
  .content-slide {
    grid-template-columns: 1fr;
    gap: 16px;
    align-content: start;
    padding-top: 8px;
  }
  .content-left { padding-right: 0; }
  .content-right { min-height: auto; padding: 14px; }
  .slide { padding: 12px; }
  .topbar { padding: 8px 12px; }
  .stat-card .value { font-size: 1.1rem; }
  .mock-table { font-size: 0.68rem; }
  .nav-dots { bottom: 12px; }
  .title-slide .logos img { height: 36px; }
}
"""

SHARED_JS = r"""
let currentSlide = 0;
let autoPlay = true;
let voiceOn = true;
let muted = false;
let timer = null;
let speed = 15; // seconds per slide

const slides = document.querySelectorAll('.slide');
const dots = document.querySelectorAll('.nav-dot');
const total = slides.length;

function showSlide(n) {
  if (n < 0) n = 0;
  if (n >= total) n = 0;
  slides.forEach(s => s.classList.remove('active'));
  dots.forEach(d => d.classList.remove('active'));
  currentSlide = n;
  slides[n].classList.add('active');
  if (dots[n]) dots[n].classList.add('active');
  document.querySelector('.progress-fill').style.width = ((n + 1) / total * 100) + '%';
  speak(n);
  resetTimer();
}

function next() { showSlide(currentSlide + 1); }
function prev() { showSlide(currentSlide - 1); }

function resetTimer() {
  clearInterval(timer);
  if (autoPlay) timer = setInterval(next, speed * 1000);
}

function togglePlay() {
  autoPlay = !autoPlay;
  document.getElementById('playBtn').textContent = autoPlay ? '⏸' : '▶';
  document.getElementById('playBtn').classList.toggle('active', autoPlay);
  resetTimer();
}

function toggleMute() {
  muted = !muted;
  voiceOn = !muted;
  document.getElementById('muteBtn').textContent = muted ? '🔇' : '🔊';
  if (muted) speechSynthesis.cancel();
}

function setSpeed(s) {
  speed = s;
  document.getElementById('speedLabel').textContent = s + 's';
  resetTimer();
}

function speak(n) {
  speechSynthesis.cancel();
  if (muted || !voiceOn) return;
  const slide = slides[n];
  const title = slide.querySelector('h1, h2');
  const desc = slide.querySelector('p, .subtitle');
  if (!title) return;
  const u1 = new SpeechSynthesisUtterance(title.textContent);
  u1.rate = 0.95;
  u1.pitch = 1;
  speechSynthesis.speak(u1);
  if (desc) {
    u1.onend = () => {
      setTimeout(() => {
        if (muted) return;
        const u2 = new SpeechSynthesisUtterance(desc.textContent);
        u2.rate = 0.92;
        speechSynthesis.speak(u2);
      }, 600);
    };
  }
}

// Keyboard navigation
document.addEventListener('keydown', e => {
  if (e.key === 'ArrowRight' || e.key === ' ') { e.preventDefault(); next(); }
  if (e.key === 'ArrowLeft') { e.preventDefault(); prev(); }
  if (e.key === 'm') toggleMute();
  if (e.key === 'p') togglePlay();
});

// Touch swipe support
let touchX = 0;
document.addEventListener('touchstart', e => { touchX = e.touches[0].clientX; });
document.addEventListener('touchend', e => {
  const dx = e.changedTouches[0].clientX - touchX;
  if (Math.abs(dx) > 60) { dx < 0 ? next() : prev(); }
});

// Start
showSlide(0);
"""


def topbar_html(title: str) -> str:
    return f"""
  <div class="topbar">
    <div class="topbar-left">
      <img src="data:image/png;base64,{SE_LOGO}" alt="Opera SE" />
      <img src="data:image/png;base64,{O3_LOGO}" alt="Opera 3" />
      <span class="brand">{title}</span>
      <span style="font-size:0.6rem;color:var(--accent2);margin-left:8px;letter-spacing:1.5px;text-transform:uppercase;font-weight:700">crakd.ai</span>
    </div>
    <div class="topbar-right">
      <button id="playBtn" class="active" onclick="togglePlay()">⏸</button>
      <button id="muteBtn" onclick="toggleMute()">🔊</button>
      <button onclick="setSpeed(Math.max(5, speed-5))">−</button>
      <span id="speedLabel" class="speed">15s</span>
      <button onclick="setSpeed(Math.min(30, speed+5))">+</button>
      <button onclick="prev()">◀</button>
      <button onclick="next()">▶</button>
    </div>
  </div>
  <div class="progress-track"><div class="progress-fill"></div></div>
"""


def nav_dots_html(count: int) -> str:
    dots = "".join(f'<button class="nav-dot{" active" if i == 0 else ""}" onclick="showSlide({i})"></button>' for i in range(count))
    return f'<div class="nav-dots">{dots}</div>'


def wrap_html(title: str, slides_html: str, slide_count: int) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<title>{title}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<style>{SHARED_CSS}</style>
</head>
<body>
{topbar_html(title)}
<div class="slides">
{slides_html}
</div>
{nav_dots_html(slide_count)}
<script>{SHARED_JS}</script>
</body>
</html>"""


# ─────────────────────────────────────────────────────────
# GOCARDLESS DEMO SLIDES
# ─────────────────────────────────────────────────────────

def gocardless_slides() -> tuple:
    slides = []

    # 1. Title
    slides.append(f"""
    <div class="title-slide">
      <div class="logos">
        <img src="data:image/png;base64,{SE_LOGO}" alt="Opera SE" />
        <div class="divider"></div>
        <img src="data:image/png;base64,{O3_LOGO}" alt="Opera 3" />
      </div>
      <div style="margin-bottom:6px;font-size:0.7rem;text-transform:uppercase;letter-spacing:3px;color:var(--accent2);font-weight:600">crakd.ai — Automating the Accounting Function</div>
      <div style="margin-bottom:8px;font-size:0.6rem;text-transform:uppercase;letter-spacing:2px;color:var(--dim);font-weight:500">AI Solution for Opera</div>
      <h1>GoCardless Direct Debit Automation</h1>
      <p class="subtitle">Eliminate manual payment processing. Automatically import, match, allocate, and post Direct Debit collections straight into Opera — powered by a secure, on-premise AI environment that keeps your financial data protected.</p>
      <span class="badge">Works with Opera SQL SE &amp; Opera 3</span>
    </div>
    """)

    # 2. The Problem
    slides.append("""
    <div class="content-slide">
      <div class="content-left">
        <div class="step-badge"><span class="dot"></span> The Challenge</div>
        <h2>Manual DD Processing Costs You Hours Every Week</h2>
        <p>Each GoCardless payout contains dozens of individual customer payments. Manually identifying each one, matching to invoices, and keying into Opera is slow, tedious, and error-prone.</p>
      </div>
      <div class="content-right">
        <div class="stat-row">
          <div class="stat-card"><div class="value">45 min</div><div class="label">Per payout manually</div></div>
          <div class="stat-card"><div class="value">2-3</div><div class="label">Payouts per week</div></div>
        </div>
        <div class="stat-row">
          <div class="stat-card"><div class="value" style="color:#f87171">5%</div><div class="label">Typical error rate</div></div>
          <div class="stat-card"><div class="value" style="color:var(--amber)">2 hrs+</div><div class="label">Weekly time wasted</div></div>
        </div>
        <ul class="feature-list" style="margin-top:12px">
          <li><span class="icon icon-amber">!</span> Cross-referencing GoCardless dashboard with Opera</li>
          <li><span class="icon icon-amber">!</span> Manually splitting fees and reclaiming VAT</li>
          <li><span class="icon icon-amber">!</span> Errors only discovered at month-end reconciliation</li>
        </ul>
      </div>
    </div>
    """)

    # 3. Automated Import
    slides.append("""
    <div class="content-slide">
      <div class="content-left">
        <div class="step-badge"><span class="dot"></span> Step 1</div>
        <h2>Automated Payout Detection</h2>
        <p>The system monitors your email inbox for GoCardless payout notifications. When a payout arrives, it automatically fetches full payment details via the GoCardless API. Zero manual intervention.</p>
      </div>
      <div class="content-right">
        <div class="flow-step"><div class="flow-num">1</div><div class="flow-text"><strong>Email arrives</strong> — GoCardless payout notification detected</div></div>
        <div class="flow-step"><div class="flow-num">2</div><div class="flow-text"><strong>API fetch</strong> — Individual payments retrieved automatically</div></div>
        <div class="flow-step"><div class="flow-num">3</div><div class="flow-text"><strong>Customer match</strong> — Payments linked to Opera accounts</div></div>
        <div class="flow-step"><div class="flow-num">4</div><div class="flow-text"><strong>Review &amp; post</strong> — One click to import everything</div></div>
        <div style="margin-top:12px;padding:10px;background:rgba(16,185,129,0.1);border-radius:8px;border:1px solid rgba(16,185,129,0.2)">
          <div style="font-size:0.78rem;color:var(--green)">From email to posted — typically under 60 seconds</div>
        </div>
      </div>
    </div>
    """)

    # 4. Customer Matching
    slides.append("""
    <div class="content-slide">
      <div class="content-left">
        <div class="step-badge"><span class="dot"></span> Step 2</div>
        <h2>Intelligent Customer Matching</h2>
        <p>Each payment is automatically matched to the correct Opera customer using mandate references, invoice numbers, and fuzzy name matching. The system learns from your corrections and improves over time.</p>
      </div>
      <div class="content-right">
        <table class="mock-table">
          <tr><th>Payment</th><th>Amount</th><th>Customer</th><th>Status</th></tr>
          <tr><td>INV-2847</td><td class="amount">£1,250.00</td><td class="match">Harris Foods Ltd</td><td><span class="tag tag-green">Matched</span></td></tr>
          <tr><td>DD-9031</td><td class="amount">£847.50</td><td class="match">Vertec Systems</td><td><span class="tag tag-green">Matched</span></td></tr>
          <tr><td>DD-9032</td><td class="amount">£2,100.00</td><td class="match">A Harris &amp; Co</td><td><span class="tag tag-green">Matched</span></td></tr>
          <tr><td>DD-9034</td><td class="amount">£425.00</td><td class="match">St Anton Ltd</td><td><span class="tag tag-green">Matched</span></td></tr>
          <tr><td>DD-9035</td><td class="amount">£3,800.00</td><td class="match">Physique Mgmt</td><td><span class="tag tag-blue">95%</span></td></tr>
        </table>
        <ul class="feature-list" style="margin-top:12px">
          <li><span class="icon icon-green">&#10003;</span> Mandate-based matching for instant identification</li>
          <li><span class="icon icon-green">&#10003;</span> AI fuzzy matching as intelligent fallback</li>
          <li><span class="icon icon-green">&#10003;</span> Self-learning — accuracy improves with each import</li>
          <li><span class="icon icon-blue">&#10003;</span> Auto-allocates receipts to matching invoices</li>
        </ul>
      </div>
    </div>
    """)

    # 5. Fees & VAT
    slides.append("""
    <div class="content-slide">
      <div class="content-left">
        <div class="step-badge"><span class="dot"></span> Step 3</div>
        <h2>Automatic Fee Separation &amp; VAT Reclaim</h2>
        <p>GoCardless transaction fees are automatically extracted, posted to your fees nominal, and the input VAT is tracked for reclaim on your next VAT return. No manual journals required.</p>
      </div>
      <div class="content-right">
        <div style="padding:14px;background:var(--surface);border-radius:10px;border:1px solid var(--border);margin-bottom:12px">
          <div style="display:flex;justify-content:space-between;margin-bottom:8px">
            <span style="font-size:0.8rem;color:var(--dim)">Payout Total</span>
            <span style="font-size:0.95rem;font-weight:700;color:var(--text)">£8,422.50</span>
          </div>
          <div style="display:flex;justify-content:space-between;margin-bottom:8px">
            <span style="font-size:0.8rem;color:var(--dim)">Customer Payments</span>
            <span style="font-size:0.95rem;font-weight:600;color:var(--green)">£8,455.98</span>
          </div>
          <div style="display:flex;justify-content:space-between;margin-bottom:8px">
            <span style="font-size:0.8rem;color:var(--dim)">GoCardless Fees</span>
            <span style="font-size:0.95rem;font-weight:600;color:#f87171">-£27.90</span>
          </div>
          <div style="display:flex;justify-content:space-between;border-top:1px solid var(--border);padding-top:8px">
            <span style="font-size:0.8rem;color:var(--dim)">VAT Reclaimable</span>
            <span style="font-size:0.95rem;font-weight:600;color:var(--accent)">£5.58</span>
          </div>
        </div>
        <ul class="feature-list">
          <li><span class="icon icon-green">&#10003;</span> Fees posted to your chosen nominal code</li>
          <li><span class="icon icon-green">&#10003;</span> VAT tracked in zvtran &amp; nvat for returns</li>
          <li><span class="icon icon-green">&#10003;</span> Net amount hits the bank — balances perfectly</li>
        </ul>
      </div>
    </div>
    """)

    # 6. Opera Posting
    slides.append("""
    <div class="content-slide">
      <div class="content-left">
        <div class="step-badge"><span class="dot"></span> Step 4</div>
        <h2>Seamless Opera Posting</h2>
        <p>One click posts everything — cashbook, sales ledger, nominal, bank balance, and customer accounts all updated simultaneously. Receipts auto-allocate to matching invoices. What used to take minutes per transaction now happens in seconds.</p>
      </div>
      <div class="content-right">
        <table class="mock-table">
          <tr><th>Opera Table</th><th>Action</th><th>Detail</th></tr>
          <tr><td>Cashbook</td><td>Sales Receipt</td><td><span class="tag tag-green">Posted</span></td></tr>
          <tr><td>Sales Ledger</td><td>Receipt on account</td><td><span class="tag tag-green">Posted</span></td></tr>
          <tr><td>Nominal Ledger</td><td>Dr Bank, Cr Debtors</td><td><span class="tag tag-green">Posted</span></td></tr>
          <tr><td>Bank Balance</td><td>Updated</td><td><span class="tag tag-green">+£8,422</span></td></tr>
          <tr><td>Customer Balance</td><td>Reduced</td><td><span class="tag tag-green">Updated</span></td></tr>
          <tr><td>VAT Return</td><td>Input VAT on fees</td><td><span class="tag tag-blue">Tracked</span></td></tr>
          <tr><td>Invoice Allocation</td><td>Auto-allocated</td><td><span class="tag tag-green">Matched</span></td></tr>
        </table>
        <div style="margin-top:12px;padding:8px 12px;background:rgba(59,130,246,0.1);border-radius:8px;font-size:0.78rem;color:var(--accent)">
          Auto-allocates receipts to matching invoices — single invoice exact match or amount match across outstanding invoices
        </div>
      </div>
    </div>
    """)

    # 7. Mandate Management
    slides.append("""
    <div class="content-slide">
      <div class="content-left">
        <div class="step-badge"><span class="dot"></span> Mandates</div>
        <h2>Full Mandate Visibility</h2>
        <p>See every GoCardless mandate linked to its Opera customer. Instantly spot customers without mandates, pending approvals, and cancelled mandates. Create payment requests directly against outstanding invoices.</p>
      </div>
      <div class="content-right">
        <div class="stat-row">
          <div class="stat-card"><div class="value">37</div><div class="label">Active mandates</div></div>
          <div class="stat-card"><div class="value" style="color:var(--accent)">100%</div><div class="label">Linked to Opera</div></div>
        </div>
        <ul class="feature-list">
          <li><span class="icon icon-green">&#10003;</span> Auto-links mandates to Opera customer accounts</li>
          <li><span class="icon icon-blue">&#10003;</span> Create payment requests against invoices</li>
          <li><span class="icon icon-purple">&#10003;</span> Manage subscriptions with recurring schedules</li>
          <li><span class="icon icon-green">&#10003;</span> Track pending, active, and cancelled status</li>
          <li><span class="icon icon-amber">&#10003;</span> Sync mandates on demand from GoCardless</li>
        </ul>
      </div>
    </div>
    """)

    # 8. Subscriptions
    slides.append("""
    <div class="content-slide">
      <div class="content-left">
        <div class="step-badge"><span class="dot"></span> Subscriptions</div>
        <h2>Recurring Payment Automation</h2>
        <p>Set up recurring payment schedules tied to GoCardless mandates. Monthly, weekly, or custom frequencies — the system handles collection automatically, leaving you with nothing to chase.</p>
      </div>
      <div class="content-right">
        <table class="mock-table">
          <tr><th>Customer</th><th>Amount</th><th>Frequency</th><th>Status</th></tr>
          <tr><td>Harris Foods</td><td class="amount">£1,250.00</td><td>Monthly</td><td><span class="tag tag-green">Active</span></td></tr>
          <tr><td>Vertec Systems</td><td class="amount">£847.50</td><td>Monthly</td><td><span class="tag tag-green">Active</span></td></tr>
          <tr><td>St Anton Ltd</td><td class="amount">£2,000.00</td><td>Monthly</td><td><span class="tag tag-green">Active</span></td></tr>
          <tr><td>Physique Mgmt</td><td class="amount">£175.00</td><td>Monthly</td><td><span class="tag tag-green">Active</span></td></tr>
        </table>
        <div style="margin-top:12px;padding:10px;background:rgba(139,92,246,0.1);border-radius:8px;border:1px solid rgba(139,92,246,0.2)">
          <div style="font-size:0.78rem;color:var(--accent3)">Subscriptions auto-collect — no monthly intervention needed</div>
        </div>
      </div>
    </div>
    """)

    # 9. Benefits summary
    slides.append("""
    <div class="content-slide">
      <div class="content-left">
        <div class="step-badge"><span class="dot"></span> Benefits</div>
        <h2>Transform Your Cash Collection</h2>
        <p>From hours of manual work to seconds of automated precision. Every payment accounted for, every fee tracked, every invoice allocated, every balance updated — instantly.</p>
      </div>
      <div class="content-right">
        <div class="stat-row">
          <div class="stat-card"><div class="value">95%</div><div class="label">Time saved</div></div>
          <div class="stat-card"><div class="value">0</div><div class="label">Keying errors</div></div>
        </div>
        <ul class="feature-list">
          <li><span class="icon icon-green">&#10003;</span> <strong>Zero manual data entry</strong> — payments auto-post</li>
          <li><span class="icon icon-green">&#10003;</span> <strong>Auto-allocation</strong> — receipts matched to invoices automatically</li>
          <li><span class="icon icon-green">&#10003;</span> <strong>VAT automatically tracked</strong> — fees reclaimed on your return</li>
          <li><span class="icon icon-blue">&#10003;</span> <strong>Secure AI environment</strong> — your data never leaves your infrastructure</li>
          <li><span class="icon icon-blue">&#10003;</span> <strong>Full audit trail</strong> — every import logged and traceable</li>
          <li><span class="icon icon-purple">&#10003;</span> <strong>Works with Opera SE &amp; Opera 3</strong> — same workflow, any version</li>
        </ul>
      </div>
    </div>
    """)

    # 10. CTA
    slides.append(f"""
    <div class="cta-slide">
      <div style="margin-bottom:6px;font-size:0.75rem;text-transform:uppercase;letter-spacing:3px;color:var(--accent2);font-weight:600">crakd.ai — Automating the Accounting Function</div>
      <div style="margin-bottom:12px;font-size:0.6rem;text-transform:uppercase;letter-spacing:2px;color:var(--dim);font-weight:500">AI Solution for Opera</div>
      <h2>Ready to Automate Your Direct Debits?</h2>
      <p>Part of our AI Solution for Opera — a secure, on-premise AI environment purpose-built for accounting automation. Your financial data stays protected within your own infrastructure while intelligent automation handles the heavy lifting.</p>
      <span class="cta-badge">Book a Demo Today</span>
      <div class="logos">
        <img src="data:image/png;base64,{SE_LOGO}" alt="Opera SE" />
        <span class="plus">+</span>
        <img src="data:image/png;base64,{O3_LOGO}" alt="Opera 3" />
      </div>
    </div>
    """)

    return slides


# ─────────────────────────────────────────────────────────
# BANK RECONCILIATION DEMO SLIDES
# ─────────────────────────────────────────────────────────

def bank_reconciliation_slides() -> tuple:
    slides = []

    # 1. Title
    slides.append(f"""
    <div class="title-slide">
      <div class="logos">
        <img src="data:image/png;base64,{SE_LOGO}" alt="Opera SE" />
        <div class="divider"></div>
        <img src="data:image/png;base64,{O3_LOGO}" alt="Opera 3" />
      </div>
      <div style="margin-bottom:6px;font-size:0.7rem;text-transform:uppercase;letter-spacing:3px;color:var(--accent2);font-weight:600">crakd.ai — Automating the Accounting Function</div>
      <div style="margin-bottom:8px;font-size:0.6rem;text-transform:uppercase;letter-spacing:2px;color:var(--dim);font-weight:500">AI Solution for Opera</div>
      <h1>Automated Bank Reconciliation</h1>
      <p class="subtitle">Import bank statements, auto-match and post missing entries, allocate receipts to invoices, and reconcile your cashbook — all from one seamless workflow, powered by a secure AI environment that keeps your data protected.</p>
      <span class="badge">Works with Opera SQL SE &amp; Opera 3</span>
    </div>
    """)

    # 2. The Problem
    slides.append("""
    <div class="content-slide">
      <div class="content-left">
        <div class="step-badge"><span class="dot"></span> The Challenge</div>
        <h2>Bank Reconciliation Shouldn't Take All Day</h2>
        <p>Manually comparing bank statements to Opera cashbook entries is tedious. Missing entries need keying in separately, and the whole process ties up your finance team for hours every month.</p>
      </div>
      <div class="content-right">
        <div class="stat-row">
          <div class="stat-card"><div class="value" style="color:#f87171">3-4 hrs</div><div class="label">Per bank per month</div></div>
          <div class="stat-card"><div class="value" style="color:var(--amber)">20+</div><div class="label">Items to key manually</div></div>
        </div>
        <ul class="feature-list">
          <li><span class="icon icon-amber">!</span> Printing statements and ticking off entries by hand</li>
          <li><span class="icon icon-amber">!</span> Switching between bank PDF and Opera to key missing entries</li>
          <li><span class="icon icon-amber">!</span> Re-keying customer names and references manually</li>
          <li><span class="icon icon-amber">!</span> Errors discovered weeks later at month-end close</li>
        </ul>
      </div>
    </div>
    """)

    # 3. Statement Import
    slides.append("""
    <div class="content-slide">
      <div class="content-left">
        <div class="step-badge"><span class="dot"></span> Step 1 — Import</div>
        <h2>Scan Your Inbox or Upload a PDF</h2>
        <p>The system scans your email inbox for bank statement PDFs across all your Opera bank accounts. Alternatively, upload a PDF directly. AI extracts every transaction automatically — no typing required.</p>
      </div>
      <div class="content-right">
        <div class="flow-step"><div class="flow-num">1</div><div class="flow-text"><strong>Scan All Banks</strong> — finds statements in email automatically</div></div>
        <div class="flow-step"><div class="flow-num">2</div><div class="flow-text"><strong>Balance validation</strong> — checks opening balance matches Opera</div></div>
        <div class="flow-step"><div class="flow-num">3</div><div class="flow-text"><strong>AI extraction</strong> — reads every line from the PDF</div></div>
        <div class="flow-step"><div class="flow-num">4</div><div class="flow-text"><strong>Sequential check</strong> — ensures statements imported in order</div></div>
        <div style="margin-top:12px;padding:10px;background:rgba(16,185,129,0.1);border-radius:8px;border:1px solid rgba(16,185,129,0.2)">
          <div style="font-size:0.78rem;color:var(--green)">Supports Barclays, Lloyds, HSBC, NatWest, Monzo, Tide, and more</div>
        </div>
      </div>
    </div>
    """)

    # 4. Smart Matching
    slides.append("""
    <div class="content-slide">
      <div class="content-left">
        <div class="step-badge"><span class="dot"></span> Step 2 — Match</div>
        <h2>Intelligent Auto-Matching</h2>
        <p>Each statement line is matched against your Opera cashbook. Already in Opera? Automatically ticked. Not in Opera? The system identifies the customer, supplier, or nominal account — ready to post in one click.</p>
      </div>
      <div class="content-right">
        <table class="mock-table">
          <tr><th>Description</th><th>Amount</th><th>Match</th></tr>
          <tr><td>Harris Foods Ltd</td><td class="amount">£1,250.00</td><td><span class="tag tag-green">Auto-matched</span></td></tr>
          <tr><td>BT Group PLC</td><td class="amount neg">-£89.99</td><td><span class="tag tag-green">Auto-matched</span></td></tr>
          <tr><td>Transfer to Deposit</td><td class="amount neg">-£5,000.00</td><td><span class="tag tag-blue">Bank Transfer</span></td></tr>
          <tr><td>Vertec Systems</td><td class="amount">£2,100.00</td><td><span class="tag tag-green">Customer: 95%</span></td></tr>
          <tr><td>HMRC VAT</td><td class="amount neg">-£4,320.00</td><td><span class="tag tag-amber">Nominal</span></td></tr>
        </table>
        <ul class="feature-list" style="margin-top:10px">
          <li><span class="icon icon-green">&#10003;</span> Recurring entries detected and matched automatically</li>
          <li><span class="icon icon-blue">&#10003;</span> Bank transfers identified by sort code / account number</li>
          <li><span class="icon icon-purple">&#10003;</span> Pattern learning — gets smarter with every import</li>
        </ul>
      </div>
    </div>
    """)

    # 5. Post to Opera
    slides.append("""
    <div class="content-slide">
      <div class="content-left">
        <div class="step-badge"><span class="dot"></span> Step 3 — Post</div>
        <h2>Post Everything to Opera in One Click</h2>
        <p>Unmatched items are posted as full double-entry transactions: cashbook, ledger, and nominal. Customer receipts auto-allocate to matching invoices. Supplier payments, bank transfers, refunds — all handled. No separate data entry screen needed.</p>
      </div>
      <div class="content-right">
        <table class="mock-table">
          <tr><th>Transaction</th><th>Type</th><th>Auto-Allocate</th></tr>
          <tr><td>Sales Receipt</td><td><span class="tag tag-green">Receipt</span></td><td><span class="tag tag-green">Invoices matched</span></td></tr>
          <tr><td>Purchase Payment</td><td><span class="tag tag-blue">Payment</span></td><td><span class="tag tag-blue">Invoices matched</span></td></tr>
          <tr><td>Bank Transfer</td><td><span class="tag tag-purple">Transfer</span></td><td><span class="tag tag-purple">Both banks</span></td></tr>
          <tr><td>Nominal Entry</td><td><span class="tag tag-amber">Journal</span></td><td>—</td></tr>
          <tr><td>Sales Refund</td><td><span class="tag tag-amber">Credit</span></td><td><span class="tag tag-green">Credit note linked</span></td></tr>
        </table>
        <div style="margin-top:12px;padding:10px;background:rgba(16,185,129,0.1);border-radius:8px;border:1px solid rgba(16,185,129,0.2)">
          <div style="font-size:0.78rem;color:var(--green)">Full double-entry with auto-allocation — receipts matched to outstanding invoices, balances updated instantly</div>
        </div>
      </div>
    </div>
    """)

    # 6. Reconcile
    slides.append("""
    <div class="content-slide">
      <div class="content-left">
        <div class="step-badge"><span class="dot"></span> Step 4 — Reconcile</div>
        <h2>Watch the Difference Hit Zero</h2>
        <p>The reconciliation view mirrors your bank statement exactly — same order, same running balance. Tick matched entries and watch the difference reduce to zero. Statement line numbers are assigned automatically on import.</p>
      </div>
      <div class="content-right">
        <div style="padding:14px;background:var(--surface);border-radius:10px;border:1px solid var(--border);margin-bottom:12px">
          <div style="display:flex;justify-content:space-between;margin-bottom:8px">
            <span style="font-size:0.8rem;color:var(--dim)">Opening Balance</span>
            <span style="font-size:0.95rem;font-weight:600;color:var(--text)">£24,580.00</span>
          </div>
          <div style="display:flex;justify-content:space-between;margin-bottom:8px">
            <span style="font-size:0.8rem;color:var(--dim)">Receipts</span>
            <span style="font-size:0.95rem;font-weight:600;color:var(--green)">+£12,450.00</span>
          </div>
          <div style="display:flex;justify-content:space-between;margin-bottom:8px">
            <span style="font-size:0.8rem;color:var(--dim)">Payments</span>
            <span style="font-size:0.95rem;font-weight:600;color:#f87171">-£9,820.00</span>
          </div>
          <div style="display:flex;justify-content:space-between;border-top:1px solid var(--border);padding-top:8px">
            <span style="font-size:0.8rem;color:var(--dim)">Closing Balance</span>
            <span style="font-size:0.95rem;font-weight:700;color:var(--text)">£27,210.00</span>
          </div>
          <div style="display:flex;justify-content:space-between;margin-top:8px;padding-top:8px;border-top:1px dashed var(--border)">
            <span style="font-size:0.85rem;font-weight:600;color:var(--green)">Difference</span>
            <span style="font-size:1.1rem;font-weight:700;color:var(--green)">£0.00</span>
          </div>
        </div>
        <div style="font-size:0.78rem;color:var(--muted);text-align:center">Statement auto-archived on completion</div>
      </div>
    </div>
    """)

    # 7. Multi-bank
    slides.append("""
    <div class="content-slide">
      <div class="content-left">
        <div class="step-badge"><span class="dot"></span> Multi-Bank</div>
        <h2>All Your Banks, One Dashboard</h2>
        <p>Scan All Banks gives you a single view across every Opera bank account. See which statements are waiting, which are in progress, and which are complete. Process them in sequence — the system ensures correct order.</p>
      </div>
      <div class="content-right">
        <div class="flow-step"><div class="flow-num" style="background:var(--green)">&#10003;</div><div class="flow-text"><strong>Barclays Current</strong> — Reconciled to 28 Feb</div></div>
        <div class="flow-step"><div class="flow-num" style="background:var(--amber)">2</div><div class="flow-text"><strong>Lloyds Business</strong> — 2 statements pending</div></div>
        <div class="flow-step"><div class="flow-num" style="background:var(--accent)">1</div><div class="flow-text"><strong>NatWest Deposit</strong> — 1 statement ready</div></div>
        <div class="flow-step"><div class="flow-num" style="background:var(--green)">&#10003;</div><div class="flow-text"><strong>Monzo Current</strong> — Up to date</div></div>
        <ul class="feature-list" style="margin-top:12px">
          <li><span class="icon icon-green">&#10003;</span> Statements auto-identified by sort code &amp; account number</li>
          <li><span class="icon icon-green">&#10003;</span> Balance validation prevents out-of-order imports</li>
          <li><span class="icon icon-blue">&#10003;</span> Archive old statements with one click</li>
        </ul>
      </div>
    </div>
    """)

    # 8. Smart Features
    slides.append("""
    <div class="content-slide">
      <div class="content-left">
        <div class="step-badge"><span class="dot"></span> Smart Features</div>
        <h2>Intelligent &amp; Secure</h2>
        <p>Powered by a secure, on-premise AI environment that keeps your financial data protected. The system learns from every import — recurring entries, aliases, and patterns are remembered. Each reconciliation gets faster and more accurate.</p>
      </div>
      <div class="content-right">
        <ul class="feature-list">
          <li><span class="icon icon-green">&#10003;</span> <strong>Auto-Allocation</strong> — receipts matched to invoices automatically</li>
          <li><span class="icon icon-green">&#10003;</span> <strong>Pattern Learning</strong> — remembers how you categorised items</li>
          <li><span class="icon icon-blue">&#10003;</span> <strong>Alias Memory</strong> — maps bank names to Opera accounts</li>
          <li><span class="icon icon-purple">&#10003;</span> <strong>Recurring Detection</strong> — standing orders auto-matched</li>
          <li><span class="icon icon-amber">&#10003;</span> <strong>Refund Detection</strong> — identifies credit notes in Opera</li>
          <li><span class="icon icon-green">&#10003;</span> <strong>Duplicate Prevention</strong> — never double-posts</li>
          <li><span class="icon icon-blue">&#10003;</span> <strong>Secure AI</strong> — data never leaves your infrastructure</li>
          <li><span class="icon icon-purple">&#10003;</span> <strong>Partial Reconcile</strong> — reconcile in stages if needed</li>
        </ul>
      </div>
    </div>
    """)

    # 9. Benefits
    slides.append("""
    <div class="content-slide">
      <div class="content-left">
        <div class="step-badge"><span class="dot"></span> Benefits</div>
        <h2>Reconciliation, Reinvented</h2>
        <p>What used to take your finance team hours now happens in minutes. Fewer errors, faster month-end, auto-allocation of receipts to invoices, and a complete audit trail — all completely integrated to Opera.</p>
      </div>
      <div class="content-right">
        <div class="stat-row">
          <div class="stat-card"><div class="value">90%</div><div class="label">Time saved</div></div>
          <div class="stat-card"><div class="value">100%</div><div class="label">Audit trail</div></div>
        </div>
        <div class="stat-row">
          <div class="stat-card"><div class="value" style="color:var(--accent)">5 min</div><div class="label">Average per statement</div></div>
          <div class="stat-card"><div class="value">0</div><div class="label">Missing entries</div></div>
        </div>
        <ul class="feature-list" style="margin-top:8px">
          <li><span class="icon icon-green">&#10003;</span> <strong>Auto-allocate</strong> — receipts matched to invoices on import</li>
          <li><span class="icon icon-green">&#10003;</span> <strong>No separate data entry</strong> — post from the reconcile screen</li>
          <li><span class="icon icon-green">&#10003;</span> <strong>Bank references preserved</strong> — Opera mirrors the statement</li>
          <li><span class="icon icon-blue">&#10003;</span> <strong>Secure AI</strong> — data stays within your infrastructure</li>
          <li><span class="icon icon-purple">&#10003;</span> <strong>Works with Opera SE &amp; Opera 3</strong> — identical workflow</li>
        </ul>
      </div>
    </div>
    """)

    # 10. CTA
    slides.append(f"""
    <div class="cta-slide">
      <div style="margin-bottom:6px;font-size:0.75rem;text-transform:uppercase;letter-spacing:3px;color:var(--accent2);font-weight:600">crakd.ai — Automating the Accounting Function</div>
      <div style="margin-bottom:12px;font-size:0.6rem;text-transform:uppercase;letter-spacing:2px;color:var(--dim);font-weight:500">AI Solution for Opera</div>
      <h2>Ready to Simplify Bank Reconciliation?</h2>
      <p>Part of our AI Solution for Opera — a secure, on-premise AI environment completely integrated to Opera. Import, match, auto-allocate, post, and reconcile — all from one screen, with your financial data protected within your own infrastructure.</p>
      <span class="cta-badge">Book a Demo Today</span>
      <div class="logos">
        <img src="data:image/png;base64,{SE_LOGO}" alt="Opera SE" />
        <span class="plus">+</span>
        <img src="data:image/png;base64,{O3_LOGO}" alt="Opera 3" />
      </div>
    </div>
    """)

    return slides


def generate_demo(title: str, slides: list, filename: str):
    slides_html = "\n".join(
        f'<div class="slide{" active" if i == 0 else ""}">{s}</div>'
        for i, s in enumerate(slides)
    )
    html = wrap_html(title, slides_html, len(slides))
    path = DEMOS_DIR / filename
    path.write_text(html, encoding="utf-8")
    size_kb = path.stat().st_size // 1024
    print(f"  Generated {filename} ({size_kb} KB, {len(slides)} slides)")
    return path


def main():
    print("Generating sales demos...")

    # GoCardless
    gc_slides = gocardless_slides()
    generate_demo(
        "GoCardless Direct Debit Automation — Opera Integration",
        gc_slides,
        "gocardless-automation-demo.html"
    )

    # Bank Reconciliation
    br_slides = bank_reconciliation_slides()
    generate_demo(
        "Automated Bank Reconciliation — Opera Integration",
        br_slides,
        "bank-reconciliation-demo-v2.html"
    )

    print("\nDone! Demos generated in /demos/")


if __name__ == "__main__":
    main()
