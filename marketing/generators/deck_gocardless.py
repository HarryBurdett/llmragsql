#!/usr/bin/env python3
"""Content for the GoCardless commercial deck. Built by build-decks.py.

Every capability claimed here was located in /Users/maccb/gocardless at 2.0.71:
  collections planning   collectable-invoices.ts, due-invoices.ts, eligible-customers.ts
  payment requests       request-payment.ts (invoice_refs), payment-requests.ts
  mandates + onboarding  mandates.ts, mandate-setups.ts (email setup links)
  subscriptions          subscriptions.ts, subscription-frequency.ts, repeat-documents.ts
  the import             fetch-api-payouts.ts, parser.ts, match-customers.ts, suggest-match.ts
  posting + allocation   batch-posting-executor.ts, opera-write allocateReceipt
  controls               duplicate-detection.ts, period-posting-decision.ts,
                         import-idempotency.ts, import-lock.ts, session-lock.ts,
                         session-write-gate.ts, revalidate-batches.ts, unposted-payments.ts
  recovery + ops         import-history.ts, restore-recovery.ts, health-check.ts, Cleardown.tsx
  both editions          opera-write/se/* and opera-write/opera3/* behind one facade

Customer names are invented. No ROI figure is asserted: the money slide is a
calculator driven entirely by the prospect's own inputs.
"""

COMPUTE = """function (num, put, gbp) {
    var invoices = num('c-inv');          // collected per month
    var value    = num('c-val');          // average invoice value
    var late     = num('c-late');         // days typically paid late
    var mins     = num('c-mins');         // minutes to key and allocate one receipt
    var rate     = num('c-rate');         // cost per hour

    var hoursPerMonth = (invoices * mins) / 60;
    var staffPerYear  = hoursPerMonth * 12 * rate;
    // Cash tied up in late payment, on their own figures: a month's collections
    // scaled by how many days late they arrive.
    var tiedUp = invoices * value * (late / 30);

    put('o-hours', Math.round(hoursPerMonth).toLocaleString('en-GB'));
    put('o-staff', gbp(staffPerYear));
    put('o-cash', gbp(tiedUp));
  }"""


def slides(a):
    return [
        # 1 ── title
        f"""<div class="stack">
      <img class="wordmark" src="{a['LOGO_CRAKD']}" alt="crakd.ai">
      <span class="pill">AI automation for Pegasus Opera</span>
      <h1>Get paid on the day<br><span class="grad-text">you decide.</span></h1>
      <p class="lede">
        Collect by Direct Debit straight from your Opera sales ledger, then let the receipts,
        the fees, the VAT and the invoice allocation post themselves. No chasing, no keying,
        no wondering when the money lands.
      </p>
      <div class="opera-lockup">
        <img src="{a['LOGO_OPERA']}" alt="Pegasus Opera">
        <span>Now on<br>Opera 3 &amp; Opera SE</span>
      </div>
    </div>""",

        # 2 ── problem
        """<p class="eyebrow">Why cash arrives late</p>
      <h2>You did the work. Now you<br>wait, and ask, and wait.</h2>
      <div class="split">
        <ul class="bars">
          <li><strong>Payment happens when the customer feels like it.</strong> Your terms say 30 days. Their process says whenever the next run is.</li>
          <li><strong>Chasing is a job.</strong> Statements, reminders, phone calls &mdash; all of it spent asking for money you have already earned.</li>
          <li><strong>Then you key it all in.</strong> Every receipt entered by hand, matched to an invoice, allocated, with the fees and their VAT to work out separately.</li>
          <li><strong>And the ledger lags.</strong> Until someone posts the batch, Opera does not know the money arrived.</li>
        </ul>
        <div class="thread">
          <div class="thread-head">The month you actually have</div>
          <div class="thread-body stack-tight">
            <div class="quote"><strong>Day 1</strong><br>Invoice raised. Terms 30 days.</div>
            <div class="quote"><strong>Day 34</strong><br>Statement sent. No reply.</div>
            <div class="quote"><strong>Day 41</strong><br>Phone call. &ldquo;It&rsquo;s in the next run.&rdquo;</div>
            <div class="quote"><strong>Day 48</strong><br>Paid. Then keyed in by hand.</div>
          </div>
        </div>
      </div>""",

        # 3 ── the cashflow case
        """<p class="eyebrow">The cashflow case</p>
      <h2>Direct Debit moves the<br>decision to your side.</h2>
      <div class="grid-3">
        <div class="panel panel-lead">
          <h3>A date you set</h3>
          <p>You choose the collection date when you raise the request. The money moves then &mdash; not when someone else gets round to it.</p>
        </div>
        <div class="panel panel-lead">
          <h3>Debtor days stop drifting</h3>
          <p>Late payment becomes the exception you chase, not the default you budget around. Your aged debtors stop carrying the same names every month.</p>
        </div>
        <div class="panel panel-lead">
          <h3>Forecastable income</h3>
          <p>Subscriptions collect on a schedule, so a known amount arrives on a known day. That is a forecast you can actually plan against.</p>
        </div>
      </div>
      <div class="panel-quiet" style="margin-top:1.2rem;">
        <h3>And the ledger keeps up</h3>
        <p>Collection is only half of it. Every payout is posted into Opera as receipts against the right customers, with the GoCardless fees and their VAT handled, and each receipt allocated to the invoices it settles &mdash; so the sales ledger is right the same day the cash arrives.</p>
      </div>""",

        # 4 ── calculator
        """<p class="eyebrow">Your numbers, not ours</p>
      <h2>What is late payment<br>costing you?</h2>
      <p class="lede" style="margin-bottom:1.5rem;">
        We are not going to quote you somebody else&rsquo;s savings. Put your own figures in.
      </p>
      <div class="calc">
        <div><label for="c-inv">Invoices collected per month</label><input id="c-inv" type="number" min="0" step="1" value="120" inputmode="numeric"></div>
        <div><label for="c-val">Average invoice (&pound;)</label><input id="c-val" type="number" min="0" step="10" value="850" inputmode="numeric"></div>
        <div><label for="c-late">Days typically paid late</label><input id="c-late" type="number" min="0" step="1" value="14" inputmode="numeric"></div>
        <div><label for="c-mins">Minutes to key &amp; allocate one receipt</label><input id="c-mins" type="number" min="0" step="1" value="3" inputmode="numeric"></div>
        <div><label for="c-rate">Cost per hour (&pound;)</label><input id="c-rate" type="number" min="0" step="1" value="22" inputmode="numeric"></div>
      </div>
      <div class="readout">
        <div class="fig"><div class="fig-n" id="o-hours">6</div><div class="fig-l">hours a month keying receipts</div></div>
        <div class="fig"><div class="fig-n" id="o-staff">&pound;1,584</div><div class="fig-l">a year of that handling time</div></div>
        <div class="fig"><div class="fig-n hero" id="o-cash">&pound;47,600</div><div class="fig-l">working capital sitting in late payment</div></div>
      </div>
      <p class="note" style="margin-top:1rem;">
        All three are arithmetic on the figures above &mdash; your collections, your lateness, your
        hourly cost. The last one is the cash that late payment keeps out of your bank; Direct Debit
        is how you stop lending it.
      </p>""",

        # 5 ── what it does
        """<p class="eyebrow">What it does</p>
      <h2>Collect, post, allocate.</h2>
      <div class="grid-3">
        <div class="panel panel-lead">
          <h3>Collect from the ledger</h3>
          <p>Pick the customers and the invoices from Opera itself, choose a collection date, and raise the requests. One-off, or on a repeating schedule.</p>
        </div>
        <div class="panel panel-lead">
          <h3>Post the payout</h3>
          <p>When GoCardless pays out, each payment becomes a Sales Receipt against the right customer, with the fees posted to your nominal and their VAT recorded for the return.</p>
        </div>
        <div class="panel panel-lead">
          <h3>Allocate to the invoice</h3>
          <p>Because the request knows which invoices it was raised against, the receipt is allocated to exactly those &mdash; not left on account for someone to sort out later.</p>
        </div>
      </div>
      <div class="figs" style="margin-top:2rem;">
        <div class="fig"><div class="fig-n">2</div><div class="fig-l">ways in &mdash; the GoCardless API, or the payout emails</div></div>
        <div class="fig"><div class="fig-n">2</div><div class="fig-l">editions &mdash; Opera 3 and Opera SE, one product</div></div>
        <div class="fig"><div class="fig-n">5</div><div class="fig-l">frequencies for subscriptions &mdash; weekly to annually</div></div>
        <div class="fig"><div class="fig-n">1</div><div class="fig-l">place the ledger is written &mdash; the shared posting engine</div></div>
      </div>""",

        # 6 ── workflow: collecting
        """<p class="eyebrow">Workflow one</p>
      <h2>Raising the collection.</h2>
      <div class="split">
        <div class="flow">
          <div class="flow-step"><div class="flow-num">01</div><div><h3>See who is collectable</h3><p>Customers with a live mandate, and the invoices actually due. Straight from Opera, no list to maintain.</p></div></div>
          <div class="flow-step"><div class="flow-num">02</div><div><h3>Onboard by email</h3><p>No mandate yet? Send a setup link. The customer signs up themselves, and the mandate comes back linked to their Opera account.</p></div></div>
          <div class="flow-step"><div class="flow-num">03</div><div><h3>Raise the request</h3><p>Tick the invoices, choose the charge date, add your own reference for their bank statement. The invoices it covers are recorded with it.</p></div></div>
          <div class="flow-step"><div class="flow-num">04</div><div><h3>Or set it and forget it</h3><p>Put the customer on a subscription instead &mdash; weekly, monthly, quarterly, half-yearly or annual &mdash; and it collects to schedule, with a repeat document raised in Opera each time.</p></div></div>
          <div class="flow-step"><div class="flow-num">05</div><div><h3>Watch it land</h3><p>Pending collections, what is due next, and anything that failed &mdash; all in one view, before the money moves.</p></div></div>
        </div>
        <div class="stack-tight">
          <div class="panel">
            <h3>Mandates are the safety catch</h3>
            <p>A mandate is checked against the customer it belongs to. If the mandate on a payment points at a different Opera account, the import stops rather than posting money to the wrong ledger.</p>
          </div>
          <div class="panel">
            <h3>Nothing is invented</h3>
            <p>Customers, invoices, outstanding balances and nominal codes are read from Opera. The app never keeps its own shadow copy of your ledger.</p>
          </div>
        </div>
      </div>""",

        # 7 ── workflow: the import
        """<p class="eyebrow">Workflow two</p>
      <h2>The payout arrives.</h2>
      <div class="split">
        <div class="stack-tight">
          <div class="thread">
            <div class="thread-head">Payout &nbsp;&middot;&nbsp; 18 Aug 2026</div>
            <div class="thread-body">
              <div class="quote">Gross <strong>&pound;14,280.00</strong> &nbsp;&middot;&nbsp; fees <strong>&pound;71.40</strong> &nbsp;&middot;&nbsp; net <strong>&pound;14,208.60</strong><br>34 payments</div>
            </div>
          </div>
          <div class="thread">
            <div class="thread-head">Posted to Opera</div>
            <div class="thread-body stack-tight">
              <div class="quote"><strong>34 sales receipts</strong> &mdash; one per customer, on the bank you nominate</div>
              <div class="quote"><strong>Fees + VAT</strong> &mdash; to your nominal, input VAT recorded for the return</div>
              <div class="quote"><strong>31 allocated</strong> &mdash; matched to the invoices each request was raised against</div>
              <div class="quote"><strong>3 held</strong> &mdash; amounts that did not agree, left for a person</div>
            </div>
          </div>
        </div>
        <div class="stack">
          <div class="panel-quiet">
            <h3>Customers are matched, then confirmed</h3>
            <p>Each payment is matched to an Opera customer &mdash; by mandate first, because that is the reliable signal &mdash; and anything uncertain is shown to you with a suggestion rather than guessed at.</p>
          </div>
          <div class="panel-quiet">
            <h3>A clearing bank if you use one</h3>
            <p>Post into a GoCardless control bank and transfer to your current account when the payout settles, so the cashbook mirrors what the bank statement will actually show.</p>
          </div>
          <div class="panel-quiet">
            <h3>The screen tells you what will post</h3>
            <p>You see every row, every warning and every amount before anything is written. What the screen says is what posts.</p>
          </div>
        </div>
      </div>""",

        # 8 ── automation
        """<p class="eyebrow">What you stop doing</p>
      <h2>The work that disappears.</h2>
      <div class="ladder">
        <div class="rung"><p><strong>Keying receipts.</strong> Thirty-four payments used to be thirty-four trips through the cashbook. Now it is one import you check and approve.</p><div class="rung-tag">posting</div></div>
        <div class="rung"><p><strong>Allocating by hand.</strong> The receipt goes against the invoices the request was raised for. Multi-invoice collections included &mdash; and if the money only covers part of it, the oldest are settled first and the rest stays on account.</p><div class="rung-tag">allocation</div></div>
        <div class="rung"><p><strong>Working out the fees.</strong> The GoCardless fee is posted to your nominal with its VAT recorded, every payout, without anyone reaching for a calculator.</p><div class="rung-tag">fees &amp; vat</div></div>
        <div class="rung"><p><strong>Raising the repeat invoice.</strong> A subscription collection can raise its repeat document in Opera as it goes, tagged so you can find them.</p><div class="rung-tag">documents</div></div>
        <div class="rung"><p><strong>Chasing mandates.</strong> Send a setup link and the customer does it themselves; the mandate arrives already linked to their account.</p><div class="rung-tag">onboarding</div></div>
      </div>""",

        # 9 ── controls
        """<p class="eyebrow">Why finance can trust it</p>
      <h2>It would rather stop<br>than guess.</h2>
      <div class="grid-2">
        <div class="panel">
          <h3>Nothing posts twice</h3>
          <p>Payouts and payments are recognised on their own identifiers, so re-importing the same payout, or re-running after a restore, cannot duplicate a receipt.</p>
        </div>
        <div class="panel">
          <h3>Closed periods are respected</h3>
          <p>The posting date is checked against Opera&rsquo;s own period rules before anything is written, on both editions.</p>
        </div>
        <div class="panel">
          <h3>Wrong-account posting is blocked</h3>
          <p>A mandate linked to a different customer stops the import. Mismatched mandates are the number-one cause of misposted payments, so this one is a block, not a warning.</p>
        </div>
        <div class="panel">
          <h3>One writer at a time</h3>
          <p>Per-company locks and a central write gate mean two people, or two sessions, cannot post the same batch at once.</p>
        </div>
        <div class="panel">
          <h3>Recoverable</h3>
          <p>Every import is recorded with what it posted. After an Opera restore, the app re-checks its own history against the ledger and tells you what is genuinely missing.</p>
        </div>
        <div class="panel">
          <h3>Checkable</h3>
          <p>A built-in health check tests the API connection, the mandates, the settings and the ledger reads, so a problem surfaces before a posting run rather than during one.</p>
        </div>
      </div>""",

        # 10 ── both editions
        f"""<p class="eyebrow">New</p>
      <h2>Now on Opera 3<br>as well as Opera SE.</h2>
      <div class="split">
        <div class="stack">
          <div class="opera-lockup">
            <img src="{a['LOGO_OPERA']}" alt="Pegasus Opera">
            <span>One product<br>both editions</span>
          </div>
          <p class="lede">
            The same app, the same screens and the same posting rules, whether your Opera is SQL or
            FoxPro. Not a second product, and not a cut-down version.
          </p>
        </div>
        <div class="stack-tight">
          <div class="panel">
            <h3>One posting engine</h3>
            <p>Both editions post through the same shared library, so a rule proven on one is the rule applied on the other. Where the two databases genuinely differ, the difference is handled in one place.</p>
          </div>
          <div class="panel">
            <h3>Opera 3 writes through a local agent</h3>
            <p>A small service alongside your Opera 3 data does the writing, so posting respects Opera&rsquo;s own record locking instead of going near the files directly.</p>
          </div>
          <div class="panel">
            <h3>Multi-company throughout</h3>
            <p>Every record is scoped to the company it belongs to, and each company carries its own settings, banks and nominal codes.</p>
          </div>
        </div>
      </div>""",

        # 11 ── fit
        """<p class="eyebrow">How it fits</p>
      <h2>Nothing changes in Opera.</h2>
      <div class="grid-2">
        <div class="panel"><h3>Your Opera stays your Opera</h3><p>We read your ledger and write the transactions you approve. No migration, no new chart of accounts, no change to how your team works in Opera.</p></div>
        <div class="panel"><h3>Your GoCardless account</h3><p>You keep the merchant relationship and the money. Connect the account, or run sandbox first to see it work end to end.</p></div>
        <div class="panel"><h3>Cloud or on-premise</h3><p>Installed and updated like your other crakd.ai apps, with its own database and its own permissions.</p></div>
        <div class="panel"><h3>Try it on demo data</h3><p>A demo mode serves worked payouts so you can walk the whole cycle &mdash; collect, import, post, allocate &mdash; before touching a live ledger.</p></div>
      </div>
      <p class="note" style="margin-top:1.4rem;">
        Requirements: Pegasus Opera 3 or Opera SE, a GoCardless account, and the sales ledger you
        already keep. Direct Debit collection is subject to GoCardless&rsquo;s own scheme rules.
      </p>""",

        # 12 ── close
        f"""<p class="eyebrow">Where to start</p>
      <h2>Collect one batch and<br>watch the ledger.</h2>
      <div class="flow" style="max-width:70ch;">
        <div class="flow-step"><div class="flow-num">01</div><div><h3>Connect and configure</h3><p>Your GoCardless account, the bank to post to, the nominal for fees. A wizard walks it.</p></div></div>
        <div class="flow-step"><div class="flow-num">02</div><div><h3>Onboard a few customers</h3><p>Send setup links to the ones who pay late. They sign up; the mandates come back linked.</p></div></div>
        <div class="flow-step"><div class="flow-num">03</div><div><h3>Collect one run</h3><p>Raise requests against real invoices, then import the payout and check every row before it posts.</p></div></div>
        <div class="flow-step"><div class="flow-num">04</div><div><h3>Then put the repeaters on subscriptions</h3><p>Anyone who pays you the same amount on a cycle should never be invoiced-and-chased again.</p></div></div>
      </div>
      <hr class="rule">
      <img class="wordmark" src="{a['LOGO_CRAKD']}" alt="crakd.ai" style="height:26px;">
      __LEGAL__
      <div class="footer-line"><span>crakd.ai</span><span>GoCardless for Opera</span><span>Opera 3 &amp; Opera SE</span></div>""",
    ]


NARRATION = [
    "Get paid on the day you decide. This collects by Direct Debit straight from your Opera sales ledger, then posts the receipts, the fees, the VAT and the invoice allocation for you. And it now runs on Opera 3 as well as Opera SE.",
    "You did the work, and then you wait. Your terms say thirty days; the customer's process says whenever the next payment run is. So you chase, with statements and reminders and phone calls, asking for money you have already earned. Then when it finally arrives, somebody keys every receipt in by hand.",
    "Direct Debit moves the decision to your side. You choose the collection date when you raise the request, so the money moves then rather than whenever somebody else gets round to it. Debtor days stop drifting, subscriptions give you income you can actually forecast, and the ledger keeps up, because every payout is posted and allocated the same day the cash arrives.",
    "We are not going to quote you somebody else's savings. Put in how many invoices you collect a month, what they average, how late they usually arrive, and what it costs you to key a receipt. The last figure is the working capital that late payment keeps out of your bank. Direct Debit is how you stop lending it.",
    "There are three jobs. Collect, choosing customers and invoices from Opera itself. Post, turning each payment in the payout into a sales receipt with the fees and their VAT handled. And allocate, so the receipt settles exactly the invoices the request was raised against, instead of sitting on account for somebody to sort out later.",
    "Raising a collection starts from your own ledger: the customers with a live mandate and the invoices actually due. If a customer has no mandate, send them a setup link and they onboard themselves. Tick the invoices, choose the charge date, and the invoices it covers are recorded with the request. Or put them on a subscription and it collects to schedule.",
    "When the payout arrives, every payment becomes a sales receipt against the right customer, the fees go to your nominal with the VAT recorded, and each receipt is allocated to its invoices. Anything that does not agree is held back for a person. Customers are matched by mandate first, because that is the reliable signal, and anything uncertain is shown to you rather than guessed at.",
    "This is the work that disappears. Keying receipts one at a time. Allocating them by hand, including the multi-invoice ones. Working out the fee and its VAT every payout. Raising the repeat invoice for a subscription. And chasing customers for mandates, when a setup link does it for you.",
    "It would rather stop than guess. Nothing posts twice, because payouts are recognised on their own identifiers. Closed periods are respected on both editions. A mandate pointing at a different customer blocks the import rather than warning you, because that is the number one cause of misposted payments. Only one session can post at a time. And every import is recorded, so after a restore the app can tell you what is genuinely missing.",
    "This is new: it now runs on Opera 3 as well as Opera SE. The same app, the same screens, the same posting rules, whether your Opera is SQL or FoxPro. Both editions post through one shared engine, so a rule proven on one is the rule applied on the other. On Opera 3 the writing is done by a small local service, so posting respects Opera's own record locking.",
    "Nothing changes in Opera. We read your ledger and write the transactions you approve: no migration, no new chart of accounts, no change to how your team works. You keep your own GoCardless account and your own money. And there is a demo mode, so you can walk the whole cycle before touching a live ledger.",
    "Start by collecting one batch and watching the ledger. Connect the account and set the bank and the fee nominal. Send setup links to the customers who pay late. Raise requests against real invoices, then import the payout and check every row before it posts. Then move everyone who pays you on a cycle onto a subscription, and stop invoicing and chasing them entirely.",
]

DECK = {
    "title": "GoCardless for Opera",
    "file": "gocardless-commercial-presentation.html",
    "provenance": (
        "Content verified against /Users/maccb/gocardless at 2.0.71 (2026-08-18). "
        "Replaces the 16 April 2026 deck, which predates 23 releases of the app "
        "and the crakd.ai identity. Customer names invented; no ROI asserted."
    ),
    "slides": slides,
    "narration": NARRATION,
    "compute": COMPUTE,
}
