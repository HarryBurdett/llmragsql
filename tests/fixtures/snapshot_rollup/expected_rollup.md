# Opera Transaction Posting — Complete Field Reference

Generated from transaction snapshot library by `scripts/regenerate_field_reference.py`.
Every field value from real Opera postings — added AND modified rows.
**Use as definitive reference when writing transactions back to Opera.**

---

## Cashbook Transactions

### Purchase Payment (fixture)

**Source:** opera_se
**Recorded:** 2026-01-02T13:00:00

Test fixture for the rollup regenerator unit tests.

**Tables Updated:**

| Database | Table | Rows Added | Rows Modified | Fields Changed |
|----------|-------|-----------|--------------|----------------|
| opera_se | aentry | 1 | 0 |  |

**aentry — New rows:**

```json
{
  "ae_entry": "P0001",
  "ae_value": -10000
}
```

### Sales Receipt (fixture)

**Source:** opera_se
**Recorded:** 2026-01-01T12:00:00

Test fixture.

**Tables Updated:**

| Database | Table | Rows Added | Rows Modified | Fields Changed |
|----------|-------|-----------|--------------|----------------|
| opera_se | aentry | 1 | 0 |  |

**aentry — New rows:**

```json
{
  "ae_entry": "R0001",
  "ae_value": 5000
}
```

## Sales Ledger Transactions

### Sales Invoice (fixture)

**Source:** opera_se
**Recorded:** 2026-01-03T14:00:00

Test fixture for sales ledger module.

**Tables Updated:**

| Database | Table | Rows Added | Rows Modified | Fields Changed |
|----------|-------|-----------|--------------|----------------|
| opera_se | stran | 1 | 0 |  |

**stran — New rows:**

```json
{
  "st_account": "C001",
  "st_trvalue": 100.0
}
```
