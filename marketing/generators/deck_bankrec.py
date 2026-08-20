#!/usr/bin/env python3
"""Content for the Bank Reconciliation commercial deck. Built by build-decks.py.

Every capability claimed here was located in /Users/maccb/bank-rec at 2.8.76:
  statement ingest      ingest-statement.ts, default-multiformat-parser.ts, format-detect.ts
  PDF extraction        claude-pdf-extractor.ts, gemini-pdf-extractor.ts, pick-vision-extractor.ts
  bank sync             BankSync.tsx, bank-sync (open banking ingest)
  matching              bank-matcher.ts, match-statement.ts, match-transaction.ts,
                        outstanding-invoice-index.ts, enrich-unmatched.ts, suggest/alias layers
  learning              bank-pattern-learner.ts, bank-aliases.ts, alias-corrections.ts
  recurring             check-recurring-entries.ts, post-recurring-entry.ts, check-repeat-entry.ts
  posting               import-posting-executor.ts, auto-allocate.ts, opera-write
  reconciling           reconcile-bank.ts, mark-reconciled.ts, complete-reconciliation.ts,
                        period-reconciliation.ts, reverse-rec
  controls              duplicate-detection.ts, pre-posting-duplicate-check.ts,
                        already-reconciled-decision.ts, file-already-reconciled.ts,
                        claimed-entries.ts, deferred-items.ts, period-posting-decision.ts,
                        import-lock.ts, session locks, health-check.ts
  recovery + ops        import-history.ts, detect-restore-point.ts, guided-restore.ts,
                        ResetWizard.tsx, Cleardown.tsx, heal-posted-stamps.ts
  both editions         opera-write/se/* and opera-write/opera3/* behind one facade

Images are the existing house workflow captures (bank-reconcile-1..5), which are
Playwright renders of hand-built HTML in the Finance Hub style — the established
method in generate_screenshots.py, not photographs of the live app. Captioned as
illustrations. No ROI figure is asserted: the money slide is a calculator driven
by the prospect's own inputs.
"""

COMPUTE = """function (num, put, gbp) {
    var stmts = num('c-stmts');   // statements a month
    var lines = num('c-lines');   // lines per statement
    var mins  = num('c-mins');    // minutes per line, manually
    var rate  = num('c-rate');    // cost per hour
    var auto  = num('c-auto');    // % matched automatically

    var rows = stmts * lines;
    var hoursPerMonth = (rows * mins) / 60;
    var savedHours = hoursPerMonth * (auto / 100);

    put('o-rows', Math.round(rows).toLocaleString('en-GB'));
    put('o-hours', Math.round(hoursPerMonth).toLocaleString('en-GB'));
    put('o-saved', gbp(savedHours * 12 * rate));
  }"""


def slides(a):
    from build_decks_shots import SHOT  # lazily resolved image data URIs
    return [
        # 1 ── title
        f"""<div class="stack">
      <img class="wordmark" src="{a['LOGO_CRAKD']}" alt="crakd.ai">
      <span class="pill">AI automation for Pegasus Opera</span>
      <h1>Reconcile the bank<br><span class="grad-text">before lunch.</span></h1>
      <p class="lede">
        Statements in, matched against Opera, posted and reconciled &mdash; with the duplicates,
        the closed periods and the recurring entries all watched for you. What used to be a
        two-day job at month-end becomes a short one you do whenever you like.
      </p>
      <div class="opera-lockup">
        <img src="{a['LOGO_OPERA']}" alt="Pegasus Opera">
        <span>Now on<br>Opera 3 &amp; Opera SE</span>
      </div>
    </div>""",

        # 2 ── problem
        """<p class="eyebrow">The month-end tax</p>
      <h2>Ticking a statement against<br>a ledger, one line at a time.</h2>
      <div class="split">
        <ul class="bars">
          <li><strong>It is all manual, and all identical.</strong> Find the line, find the entry, tick it, move on. Hundreds of times, every month, on every account.</li>
          <li><strong>The hard part is what is missing.</strong> Anything on the bank that is not in Opera has to be keyed &mdash; and worked out first.</li>
          <li><strong>Mistakes are expensive and quiet.</strong> A payment posted twice, a receipt on the wrong customer, an entry into a closed period. Each one takes far longer to unpick than it took to make.</li>
          <li><strong>Until it is done, you do not know your cash.</strong> The bank balance is a fact; your ledger is an opinion until the two agree.</li>
        </ul>
        <div class="thread">
          <div class="thread-head">A normal statement</div>
          <div class="thread-body stack-tight">
            <div class="quote"><strong>142 lines</strong><br>Receipts, payments, transfers, charges</div>
            <div class="quote"><strong>~40 not in Opera yet</strong><br>Direct debits, card fees, standing orders</div>
            <div class="quote"><strong>3 look familiar</strong><br>Already posted? Or genuinely twice?</div>
            <div class="quote"><strong>2 days later</strong><br>Reconciled. Then next month, again.</div>
          </div>
        </div>
      </div>""",

        # 3 ── the benefit
        """<p class="eyebrow">What you get back</p>
      <h2>Most of the ticking,<br>done before you look.</h2>
      <div class="grid-3">
        <div class="panel panel-lead">
          <h3>Time, every month</h3>
          <p>The statement is matched against the ledger automatically. You review decisions and handle the exceptions, instead of doing the whole thing by hand.</p>
        </div>
        <div class="panel panel-lead">
          <h3>Cash you can see</h3>
          <p>Reconcile weekly instead of monthly and your Opera cashbook tracks the real bank position, so the number you plan from is the number you have.</p>
        </div>
        <div class="panel panel-lead">
          <h3>Month-end that does not spike</h3>
          <p>Reconciling little and often removes the cliff, and the deadline stops depending on one person being available.</p>
        </div>
      </div>
      <div class="panel-quiet" style="margin-top:1.2rem;">
        <h3>And it gets better with use</h3>
        <p>Every correction you make is remembered. Payees you name, the aliases your bank prints, the way a particular customer's reference appears &mdash; all learned, so next month more of the statement is matched before you open it.</p>
      </div>""",

        # 4 ── calculator
        """<p class="eyebrow">Your numbers, not ours</p>
      <h2>What is reconciling<br>costing you now?</h2>
      <p class="lede" style="margin-bottom:1.5rem;">
        We are not going to quote you somebody else&rsquo;s savings. Put your own figures in.
      </p>
      <div class="calc">
        <div><label for="c-stmts">Statements a month</label><input id="c-stmts" type="number" min="0" step="1" value="6" inputmode="numeric"></div>
        <div><label for="c-lines">Lines per statement</label><input id="c-lines" type="number" min="0" step="10" value="140" inputmode="numeric"></div>
        <div><label for="c-mins">Minutes per line, by hand</label><input id="c-mins" type="number" min="0" step="0.25" value="0.75" inputmode="decimal"></div>
        <div><label for="c-rate">Cost per hour (&pound;)</label><input id="c-rate" type="number" min="0" step="1" value="22" inputmode="numeric"></div>
        <div><label for="c-auto">Matched automatically (%)</label><input id="c-auto" type="number" min="0" max="100" step="5" value="80" inputmode="numeric"></div>
      </div>
      <div class="readout">
        <div class="fig"><div class="fig-n" id="o-rows">840</div><div class="fig-l">lines a month to account for</div></div>
        <div class="fig"><div class="fig-n" id="o-hours">11</div><div class="fig-l">hours a month reconciling</div></div>
        <div class="fig"><div class="fig-n hero" id="o-saved">&pound;2,217</div><div class="fig-l">a year on the share matched for you</div></div>
      </div>
      <p class="note" style="margin-top:1rem;">
        The match rate is your estimate, not our claim &mdash; and it is the honest lever, because
        what the app matches depends on your banks, your references and your ledger. The exceptions
        stay with your team either way.
      </p>""",

        # 5 ── what it does
        """<p class="eyebrow">What it does</p>
      <h2>Four steps, one screen each.</h2>
      <div class="grid-2">
        <div class="panel panel-lead">
          <h3>1 &nbsp; Get the statement in</h3>
          <p>PDF, CSV or spreadsheet, from a file, from an email, or straight from the bank through open banking. The format is detected; the layout does not have to be one we have seen before.</p>
        </div>
        <div class="panel panel-lead">
          <h3>2 &nbsp; Match against Opera</h3>
          <p>Every line is matched against outstanding entries &mdash; receipts, payments, transfers, recurring items &mdash; with a reason given for each decision and a suggestion where it is not certain.</p>
        </div>
        <div class="panel panel-lead">
          <h3>3 &nbsp; Post what is missing</h3>
          <p>Bank charges, direct debits, standing orders and unrecorded receipts are posted for you, coded and VAT-treated the way you tell it, and allocated to invoices where they settle them.</p>
        </div>
        <div class="panel panel-lead">
          <h3>4 &nbsp; Reconcile</h3>
          <p>Mark the statement reconciled in Opera itself, with the cashbook balance agreeing to the statement. Reversible if you need to unwind it.</p>
        </div>
      </div>""",

        # 6 ── the workflow, with images
        f"""<p class="eyebrow">The workflow</p>
      <h2>Select, review, post, reconcile.</h2>
      <div class="shots">
        <figure>
          <img src="{SHOT['select']}" alt="Selecting a bank and statement to reconcile, with the statement period and balances shown.">
          <figcaption>Pick the bank and the statement</figcaption>
        </figure>
        <figure>
          <img src="{SHOT['review']}" alt="Reviewing matched statement lines against Opera entries, with match reasons and unmatched rows flagged.">
          <figcaption>Review what matched, and why</figcaption>
        </figure>
        <figure>
          <img src="{SHOT['complete']}" alt="A completed reconciliation showing the cashbook balance agreeing to the statement closing balance.">
          <figcaption>Reconcile, balances agreeing</figcaption>
        </figure>
      </div>
      <p class="note" style="margin-top:1.2rem;">
        Interface illustrations. You are never more than one screen from the decision the app made
        and the reason it made it &mdash; and nothing is written to Opera until you approve the run.
      </p>""",

        # 7 ── automation
        """<p class="eyebrow">What you stop doing</p>
      <h2>The work that disappears.</h2>
      <div class="ladder">
        <div class="rung"><p><strong>Typing the statement in.</strong> Reading a PDF statement is done for you, whatever the layout &mdash; and with open banking the statement arrives on its own.</p><div class="rung-tag">ingest</div></div>
        <div class="rung"><p><strong>Hunting for the matching entry.</strong> Lines are matched on amount, date, reference and payee, against the entries actually outstanding in Opera.</p><div class="rung-tag">matching</div></div>
        <div class="rung"><p><strong>Remembering the recurring ones.</strong> Standing orders, direct debits and repeating charges are recognised and offered, so the ones you post every month are already waiting.</p><div class="rung-tag">recurring</div></div>
        <div class="rung"><p><strong>Coding the same charge again.</strong> Nominal, VAT and analysis are remembered per payee, and the VAT is populated from the code you set rather than typed each time.</p><div class="rung-tag">coding</div></div>
        <div class="rung"><p><strong>Allocating receipts by hand.</strong> A customer receipt is matched to the invoices it settles and allocated on posting, not left on account.</p><div class="rung-tag">allocation</div></div>
        <div class="rung"><p><strong>Waiting for the import to finish.</strong> Long imports run in the background with a progress bar, so a big statement does not tie up the screen.</p><div class="rung-tag">throughput</div></div>
      </div>""",

        # 8 ── controls
        """<p class="eyebrow">Why finance can trust it</p>
      <h2>Built to refuse<br>rather than guess.</h2>
      <div class="grid-2">
        <div class="panel">
          <h3>Duplicates are caught twice</h3>
          <p>Once when the statement is read, and again immediately before posting &mdash; because the ledger can change between the two. A row already in Opera is held, not posted again.</p>
        </div>
        <div class="panel">
          <h3>Already-reconciled is respected</h3>
          <p>The app checks Opera itself for what is already reconciled, rather than trusting its own record, so re-running a statement cannot double-reconcile an entry.</p>
        </div>
        <div class="panel">
          <h3>Closed periods are respected</h3>
          <p>Posting dates are checked against Opera&rsquo;s own period rules before anything is written, on both editions.</p>
        </div>
        <div class="panel">
          <h3>Entries claimed elsewhere are left alone</h3>
          <p>If another reconciliation or another company has claimed an entry, this one will not take it.</p>
        </div>
        <div class="panel">
          <h3>The screen is the contract</h3>
          <p>What the preview says will post is what posts &mdash; the selection is rebuilt from what you can actually see, and a partial run is recorded honestly.</p>
        </div>
        <div class="panel">
          <h3>Reversible, and checkable</h3>
          <p>A completed reconciliation can be reversed as a batch. A built-in health check tests the connection, the banks and the ledger reads before a run rather than during one.</p>
        </div>
      </div>""",

        # 9 ── both editions
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
            <p>Both editions post through the same shared library, so a rule proven on one is the rule applied on the other, and the period and duplicate guards behave identically.</p>
          </div>
          <div class="panel">
            <h3>Opera 3 writes through a local agent</h3>
            <p>A small service alongside your Opera 3 data does the writing, so posting respects Opera&rsquo;s own record locking instead of going near the files directly.</p>
          </div>
          <div class="panel">
            <h3>Proven on both</h3>
            <p>Full reconciliation cycles &mdash; import, post, recurring entries, reconcile &mdash; have been run end to end against live Opera 3 and Opera SE company data.</p>
          </div>
        </div>
      </div>""",

        # 10 ── fit
        """<p class="eyebrow">How it fits</p>
      <h2>Nothing changes in Opera.</h2>
      <div class="grid-2">
        <div class="panel"><h3>Your Opera stays your Opera</h3><p>We read your cashbook and ledgers and write the transactions you approve. No migration, no new chart of accounts, no change to how your team works in Opera.</p></div>
        <div class="panel"><h3>Every bank, every company</h3><p>Multi-company throughout, with per-bank settings, and foreign-currency banks handled in their own currency.</p></div>
        <div class="panel"><h3>Cloud or on-premise</h3><p>Installed and updated like your other crakd.ai apps, with its own database and its own permissions.</p></div>
        <div class="panel"><h3>Restore-aware</h3><p>If Opera is restored from backup, the app detects it, works out what its own history says should exist, and guides putting the two back in step.</p></div>
      </div>
      <p class="note" style="margin-top:1.4rem;">
        Requirements: Pegasus Opera 3 or Opera SE, and the bank accounts you already keep in the
        cashbook. Open-banking statement feeds are optional &mdash; files and email work without them.
      </p>""",

        # 11 ── close
        f"""<p class="eyebrow">Where to start</p>
      <h2>Reconcile one account<br>and time it.</h2>
      <div class="flow" style="max-width:70ch;">
        <div class="flow-step"><div class="flow-num">01</div><div><h3>Point it at one bank</h3><p>One account, one statement you have already done by hand. The fair comparison.</p></div></div>
        <div class="flow-step"><div class="flow-num">02</div><div><h3>Look at the matches, not the total</h3><p>Check the decisions and the reasons. This is where you find out how much of your statement it can actually take.</p></div></div>
        <div class="flow-step"><div class="flow-num">03</div><div><h3>Post the exceptions</h3><p>Code the handful it could not match. Those codings are remembered for next month.</p></div></div>
        <div class="flow-step"><div class="flow-num">04</div><div><h3>Then do the rest of the banks</h3><p>And stop saving it all up for month-end.</p></div></div>
      </div>
      <hr class="rule">
      <img class="wordmark" src="{a['LOGO_CRAKD']}" alt="crakd.ai" style="height:26px;">
      __LEGAL__
      <div class="footer-line"><span>crakd.ai</span><span>Bank Reconciliation for Opera</span><span>Opera 3 &amp; Opera SE</span></div>""",
    ]


NARRATION = [
    "Reconcile the bank before lunch. Statements come in, get matched against Opera, posted and reconciled, with the duplicates, the closed periods and the recurring entries all watched for you. And it now runs on Opera 3 as well as Opera SE.",
    "Reconciling is the month-end tax: find the line, find the entry, tick it, move on, hundreds of times, on every account. The hard part is what is missing, because anything on the bank that is not in Opera has to be worked out and keyed. And mistakes are quiet and expensive. Until it is done, your ledger is an opinion.",
    "What you get back is most of the ticking, done before you look. You review decisions and handle exceptions instead of doing the whole thing by hand. Reconcile weekly rather than monthly and your cashbook tracks the real bank position, so the number you plan from is the number you have. And it improves with use, because every correction you make is remembered.",
    "We are not going to quote you somebody else's savings. Put in your statements a month, the lines on them, and what a line costs you by hand. The match rate is your estimate, not our claim, because what the app can match depends on your banks and your references. The exceptions stay with your team either way.",
    "There are four steps, one screen each. Get the statement in, from a file, an email or straight from the bank. Match it against Opera, with a reason for every decision. Post what is missing, coded and VAT-treated the way you tell it. Then reconcile, in Opera itself, with the cashbook agreeing to the statement.",
    "You are never more than one screen away from the decision the app made and the reason it made it. Pick the bank and the statement, review what matched and why, then reconcile with the balances agreeing. Nothing is written to Opera until you approve the run.",
    "This is the work that disappears. Typing statements in. Hunting for the matching entry. Remembering the standing orders and direct debits you post every month. Coding the same charge again, because the nominal and the VAT are remembered per payee. Allocating customer receipts by hand. And waiting around, because long imports run in the background.",
    "It is built to refuse rather than guess. Duplicates are caught when the statement is read and again just before posting, because the ledger can change in between. Already-reconciled is checked in Opera itself rather than trusted from our own record. Closed periods are respected. Entries claimed by another reconciliation are left alone. What the preview says will post is what posts. And a finished reconciliation can be reversed.",
    "This is new: it now runs on Opera 3 as well as Opera SE. The same app, the same screens, the same posting rules, whether your Opera is SQL or FoxPro. Both editions post through one shared engine, so the period and duplicate guards behave identically, and on Opera 3 the writing is done by a small local service that respects Opera's own record locking. Full cycles have been run end to end on both.",
    "Nothing changes in Opera. We read your cashbook and ledgers and write the transactions you approve: no migration, no new chart of accounts, no change to how your team works. Multi-company throughout, with foreign-currency banks handled in their own currency. And if Opera is ever restored from backup, the app spots it and helps you get back in step.",
    "Start by reconciling one account and timing it. Pick a statement you have already done by hand, so the comparison is fair. Then look at the matches rather than the total, because that is where you find out how much of your statement it can take. Code the handful it could not match, and those codings are remembered. Then do the rest of your banks, and stop saving it all up for month-end.",
]

DECK = {
    "title": "Bank Reconciliation for Opera",
    "file": "bank-reconciliation-commercial-presentation.html",
    "provenance": (
        "Content verified against /Users/maccb/bank-rec at 2.8.76 (2026-08-18). "
        "Replaces the 24-25 March 2026 demos, which predate 47 releases of the app, "
        "the crakd.ai identity, and Opera 3 support. The old per-edition demos "
        "(opera-se / opera3) are superseded: one product now covers both. "
        "Workflow images are the existing house illustrations, captioned as such."
    ),
    "slides": slides,
    "narration": NARRATION,
    "compute": COMPUTE,
}
