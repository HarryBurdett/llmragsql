# Opera 3 transaction-trace tooling

`scripts/snapshot_opera3.py` is the Opera 3 (FoxPro) counterpart to
`scripts/snapshot_opera.py` (Opera SE / SQL Server). Both capture
"before" and "after" DBF/SQL state around a transaction, diff them,
and produce the JSON that goes into the central transaction-library
at `~/opera-knowledge-ref/packages/opera-knowledge/transaction-library/`.

The transaction-library already has ~70 SE traces (cashbook, sales,
purchase, nominal, GoCardless batch, SOP, stock, etc.). It has zero
Opera 3 traces today. Closing that gap is what this tooling exists
for.

## Why this matters

75% of Opera users run Opera 3, not SE. The SAM rewrite has business
logic ported from SE only — same Knex, same table names, same field
names — but **runtime behaviour against Opera 3 is unverified**.
The before/after diff is the cheapest way to verify each transaction
posts to the same tables in the same shape on both engines, and to
catch the subtle differences (default values, lock fields, allocation
cascading, type coercion) that schema inspection alone won't surface.

## CLI

```bash
# Take before snapshot — point at the company's data folder
python scripts/snapshot_opera3.py before --data-path /path/to/install/DATA

# Perform the transaction in Opera 3 (post receipt, allocate, etc.)

# Take after snapshot — no need to repeat the path
python scripts/snapshot_opera3.py after
```

Single-installation assumption. No multi-company resolution, no
seqco lookup, no `companies/*.json` integration. Just point at the
one DBF folder.

The data path is saved inside `scripts/opera3_snapshot.json`. `after`
reads it back so you don't retype it; if you pass a different
`--data-path` to `after`, the script errors out (catches accidentally
diffing different companies).

State files (both git-ignored, in `scripts/`):

| File | Written by | Purpose |
|---|---|---|
| `opera3_snapshot.json` | `before` | Before snapshot — overwritten each `before` run |
| `opera3_comparison_result.json` | `after` | Full `before + after + changes` for offline review |

Diff output mirrors `snapshot_opera.py`:

- Per-table `NEW RECORDS: N` with all non-null fields
- Per-table `MODIFIED RECORDS: N` with field-level before→after
- `SUMMARY` line per table

## What to capture

The canonical list of transactions and the JSON shape live in the
central knowledge:

> `~/opera-knowledge-ref/packages/opera-knowledge/transaction-library/INDEX.md`

Don't duplicate the list here — the central INDEX is the source of
truth. It tracks which engine each transaction has been captured
against. Run a before/after pair for any transaction marked "Opera 3:
pending" and add the result to the library.

## Adding a captured trace to the central library

After `after` completes, the result is in
`scripts/opera3_comparison_result.json`. Wrap it as a transaction
library entry:

```bash
# Pick the transaction name from the central INDEX
TX=cashbook_sales_receipt
ENGINE=opera_3
TS=$(date +%Y%m%d_%H%M%S)

jq --arg name "Sales Receipt" \
   --arg source "$ENGINE" \
   --arg description "..." \
   '{
     name: $name,
     source: $source,
     description: $description,
     before_timestamp: .before.timestamp,
     after_timestamp: .after.timestamp,
     recorded_at: now | todate,
     tables_changed: (.changes | length),
     tables_checked: (.after.tables | length),
     changes: .changes,
     classification: null
   }' \
   scripts/opera3_comparison_result.json \
   > ~/opera-knowledge-ref/packages/opera-knowledge/transaction-library/${TX}_${ENGINE}_${TS}.json

cd ~/opera-knowledge-ref
python scripts/regenerate_field_reference.py    # rebuild the markdown rollup
git add packages/opera-knowledge/transaction-library/
git commit -m "trace: opera_3 ${TX} — first capture"
git push
```

The classification block (auto-detected type, tables-with-new-rows,
amount conventions, etc.) gets filled in when reviewing — see the
existing SE entries for the pattern.

## Reading the diff

The output is "what changed in the database when the transaction
ran". Compare it against the matching SE trace already in the
library to spot any divergences:

- Same tables touched? Same rows-added vs rows-modified pattern?
- Same fields updated? Any field present in one engine but not the
  other?
- Same amount conventions (pence vs pounds)?
- Same ID-allocation pattern (`nextid` vs `atype.ay_entry`)?
- Same locking/audit metadata (`zlock`, `datemodified`, `ae_tmpstat`)?

Anything that differs is a candidate for a version-specific code path
in the SAM rewrite. Most of the time the answer should be "identical"
— that's the whole point of confirming.

## When something looks wrong

If a DBF read errors or returns garbage:

- Fix `Opera3FieldParser` in `sql_rag/opera3_foxpro.py` (shared with
  the runtime). The snapshot tool uses the same parser, so any fix
  benefits production code too.
- If a particular field's type coercion is broken, add a parser
  override in `Opera3FieldParser` rather than working around it in
  the snapshot tool.

## Cross-engine schema sanity check (one-shot)

Separate from transaction tracing, you can also do a one-shot schema
comparison once you have an Opera 3 snapshot:

```bash
diff <(jq -S 'del(.timestamp) | del(.data_path) | del(.engine)' \
        scripts/opera_snapshot.json) \
     <(jq -S 'del(.timestamp) | del(.data_path) | del(.engine)' \
        scripts/opera3_snapshot.json) | less
```

Expected outcome: tables and field names are identical, only the
storage engine differs. If you see anything else, the diff is the
punch list.
