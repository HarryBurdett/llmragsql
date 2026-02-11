# Migration Strategy: Opera to Independent System

**Goal**: Build a modern, independent system that initially runs on Opera's database, then migrates to its own schema while preserving all data and functionality.

## Strategic Phases

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Phase 1: BUILD ON OPERA                                                     │
│ ┌─────────────┐     ┌─────────────┐     ┌─────────────┐                    │
│ │ Modern UI   │────►│   API       │────►│ Opera SQL   │                    │
│ │ (React)     │     │  (FastAPI)  │     │  Tables     │                    │
│ └─────────────┘     └─────────────┘     └─────────────┘                    │
│                                                                             │
│ Deliverable: Working Stock/SOP/POP/BOM modules on Opera database           │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Phase 2: ABSTRACT THE DATA LAYER                                            │
│ ┌─────────────┐     ┌─────────────┐     ┌─────────────┐                    │
│ │ Modern UI   │────►│   API       │────►│ Repository  │                    │
│ │ (React)     │     │  (FastAPI)  │     │  Interface  │                    │
│ └─────────────┘     └─────────────┘     └──────┬──────┘                    │
│                                                │                            │
│                                    ┌───────────┴───────────┐               │
│                                    ▼                       ▼               │
│                              ┌──────────┐           ┌──────────┐           │
│                              │  Opera   │           │   New    │           │
│                              │  Adapter │           │  Adapter │           │
│                              └──────────┘           └──────────┘           │
│                                                                             │
│ Deliverable: Same API, switchable backend (Opera or New)                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Phase 3: DESIGN NEW SCHEMA                                                  │
│                                                                             │
│ Take best of Opera + modern practices:                                      │
│ - Proper foreign keys (Opera has implicit relationships)                    │
│ - UUID primary keys (not sequential strings)                                │
│ - Timestamps with timezone                                                  │
│ - Audit tables (who changed what, when)                                     │
│ - JSON fields for extensibility                                             │
│ - Consistent naming (no more 2-letter prefixes)                             │
│                                                                             │
│ Deliverable: New database schema design document                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Phase 4: PARALLEL RUNNING                                                   │
│                                                                             │
│ ┌─────────────┐     ┌─────────────┐     ┌─────────────┐                    │
│ │   New UI    │────►│   API       │────►│ Repository  │                    │
│ └─────────────┘     └─────────────┘     └──────┬──────┘                    │
│                                                │                            │
│                                    ┌───────────┴───────────┐               │
│                                    ▼                       ▼               │
│                              ┌──────────┐           ┌──────────┐           │
│                              │  Opera   │◄─────────►│   New    │           │
│                              │  (Read)  │   Sync    │  (Write) │           │
│                              └──────────┘           └──────────┘           │
│                                                                             │
│ Options:                                                                    │
│ - Write to both, read from new                                              │
│ - Migrate in sections (Stock first, then SOP, etc.)                         │
│ - Big bang cutover (riskier but simpler)                                    │
│                                                                             │
│ Deliverable: Data migration scripts, parallel running capability            │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Phase 5: FULL INDEPENDENCE                                                  │
│                                                                             │
│ ┌─────────────┐     ┌─────────────┐     ┌─────────────┐                    │
│ │   New UI    │────►│   API       │────►│  New DB     │                    │
│ └─────────────┘     └─────────────┘     └─────────────┘                    │
│                                                                             │
│ - Opera decommissioned                                                      │
│ - Full control over schema evolution                                        │
│ - No license dependencies                                                   │
│ - Modern tooling (migrations, ORMs, etc.)                                   │
│                                                                             │
│ Deliverable: Fully independent system                                       │
└─────────────────────────────────────────────────────────────────────────────┘
```

## What We Keep from Opera

Opera's database design has 30+ years of real-world refinement. We should preserve:

### Good Patterns to Keep
| Pattern | Opera Example | Why It Works |
|---------|---------------|--------------|
| Double-entry accounting | ntran for all nominal posts | Audit trail, balancing |
| Separate transaction tables | stran, ptran, atran | Clean separation of concerns |
| Period-based posting | pt_period, pt_year | Easy reporting, period close |
| Control accounts | Debtors/Creditors control | Reconciliation capability |
| VAT tracking | zvtran, nvat | HMRC compliance |
| Document numbering | T000001, S000001 | Unique, sortable references |
| Warehouse-level stock | cstwh per warehouse | Multi-location support |
| Allocation vs physical | CS_ALLOC vs CS_INSTOCK | Proper stock management |

### Patterns to Improve
| Opera Pattern | Problem | New Approach |
|---------------|---------|--------------|
| 2-letter field prefixes (pt_, st_) | Verbose, inconsistent | Descriptive names |
| Implicit relationships | No FK constraints | Proper foreign keys |
| String primary keys | Inefficient | UUID or integer PKs |
| No created/modified tracking | Limited audit | Full audit columns |
| Pence vs Pounds inconsistency | Error-prone | Consistent decimal handling |
| No soft deletes | Data loss risk | Soft delete with restore |
| Limited extensibility | Hard to add fields | JSON metadata fields |

## Repository Interface Design

The key to migration is abstracting the data layer:

```python
# Abstract interface - doesn't care about underlying database
class StockRepository(ABC):
    @abstractmethod
    async def get_product(self, ref: str) -> Product: ...

    @abstractmethod
    async def get_stock_levels(self, ref: str) -> List[StockLevel]: ...

    @abstractmethod
    async def create_adjustment(self, adjustment: StockAdjustment) -> str: ...

    @abstractmethod
    async def create_transfer(self, transfer: StockTransfer) -> str: ...

# Opera implementation
class OperaStockRepository(StockRepository):
    async def get_product(self, ref: str) -> Product:
        # Reads from cname, cstwh, cfact, etc.
        ...

# New database implementation
class NewStockRepository(StockRepository):
    async def get_product(self, ref: str) -> Product:
        # Reads from products, stock_levels, etc.
        ...
```

## Data Migration Approach

### Option A: Big Bang Migration
- Export all Opera data
- Transform to new schema
- Import to new database
- Switch over on a weekend

**Pros**: Clean cut, no sync complexity
**Cons**: High risk, needs thorough testing

### Option B: Gradual Migration by Module
1. Migrate Stock master data (read-only in Opera)
2. Migrate SOP (new orders to new system)
3. Migrate POP (new POs to new system)
4. Historical data migrated in background

**Pros**: Lower risk, can rollback per module
**Cons**: Complex sync during transition

### Option C: Shadow Writing
- Write to both databases during transition
- Compare results to ensure consistency
- Gradually shift reads to new system

**Pros**: Highest confidence, can verify before cutover
**Cons**: Most complex implementation

## Recommended Approach

1. **Start with Phase 1** - Build working modules on Opera
   - Proves the UI and workflows work
   - Users get immediate value
   - We learn Opera's edge cases

2. **Design repository interface early** (Phase 2)
   - Even if only Opera adapter exists initially
   - Makes future migration much easier
   - Forces clean separation

3. **Design new schema in parallel** (Phase 3)
   - Don't rush this - get it right
   - Review with stakeholders
   - Consider future needs

4. **Choose migration approach based on business needs**
   - How much downtime is acceptable?
   - How critical is the system?
   - What's the risk tolerance?

## Timeline Considerations

| Phase | Estimated Duration | Dependencies |
|-------|-------------------|--------------|
| Phase 1 | 6-12 months | UI screens, table documentation |
| Phase 2 | 2-3 months | Phase 1 working |
| Phase 3 | 2-3 months | Can run parallel to Phase 1 |
| Phase 4 | 3-6 months | Phases 2 & 3 complete |
| Phase 5 | 1-2 months | Successful parallel running |

*Note: These are rough estimates - actual duration depends on scope and resources*

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Opera edge cases not understood | Data integrity issues | Extensive testing against Opera behavior |
| New schema missing critical data | Feature gaps | Review Opera tables thoroughly before design |
| Migration data loss | Business disruption | Full backups, dry-run migrations, rollback plan |
| User resistance | Adoption failure | Involve users early, make new UI clearly better |
| Performance issues | Unusable system | Load testing, performance benchmarks |

## Success Criteria

### Phase 1 Complete When:
- [ ] Stock, SOP, POP, BOM modules working on Opera
- [ ] Users can do daily work without Opera UI
- [ ] All posting rules followed correctly

### Phase 2 Complete When:
- [ ] Repository interface defined for all modules
- [ ] Opera adapter passes all tests
- [ ] New adapter skeleton exists

### Phase 3 Complete When:
- [ ] New schema documented
- [ ] Migration scripts drafted
- [ ] Schema reviewed and approved

### Phase 4 Complete When:
- [ ] Data migrated successfully
- [ ] Parallel running for 1+ month
- [ ] No data discrepancies

### Phase 5 Complete When:
- [ ] Opera disconnected
- [ ] All users on new system
- [ ] Opera data archived

## Next Steps

1. Complete Phase 1 module documentation (in progress)
2. Gather UI screenshots from Opera
3. Begin Stock module read-only implementation
4. Start thinking about repository interface patterns
