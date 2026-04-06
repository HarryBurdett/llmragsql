# Opera Transaction Posting — Complete Field Reference

Generated from transaction snapshot library.
Every field value from real Opera postings — added AND modified rows.
**Use as definitive reference when writing transactions back to Opera.**

---

## Cashbook Transactions

### Bank Rec update

#### aentry

**12 row(s) modified:**

*Row id=11045.0:*
| Field | Before | After |
|-------|--------|-------|
| `ae_tmpstat` | `0.0` | `20.0` |

*Row id=11046.0:*
| Field | Before | After |
|-------|--------|-------|
| `ae_tmpstat` | `0.0` | `30.0` |

*Row id=11047.0:*
| Field | Before | After |
|-------|--------|-------|
| `ae_tmpstat` | `0.0` | `40.0` |

*Row id=11049.0:*
| Field | Before | After |
|-------|--------|-------|
| `ae_tmpstat` | `0.0` | `50.0` |

*Row id=11050.0:*
| Field | Before | After |
|-------|--------|-------|
| `ae_tmpstat` | `0.0` | `60.0` |


#### nbank

**7 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `nk_reccfwd` | `0.0` | `2345400.0` |
| `nk_recldte` | `None` | `2024-05-01T00:00:00` |
| `nk_reclnum` | `0.0` | `336.0` |
| `nk_recstdt` | `None` | `2024-05-01T00:00:00` |
| `nk_recstfr` | `0.0` | `428.0` |
| `nk_recstto` | `0.0` | `428.0` |

*Row id=2.0:*
| Field | Before | After |
|-------|--------|-------|
| `nk_recldte` | `None` | `NaT` |
| `nk_recstdt` | `None` | `NaT` |

*Row id=3.0:*
| Field | Before | After |
|-------|--------|-------|
| `nk_recldte` | `None` | `NaT` |
| `nk_recstdt` | `None` | `NaT` |

*Row id=4.0:*
| Field | Before | After |
|-------|--------|-------|
| `nk_recldte` | `None` | `NaT` |
| `nk_recstdt` | `None` | `NaT` |

*Row id=5.0:*
| Field | Before | After |
|-------|--------|-------|
| `nk_recldte` | `None` | `NaT` |
| `nk_recstdt` | `None` | `NaT` |


---

### Nominal Payment

#### aentry

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `ae_acnt` | `C310` |
| `ae_batchid` | `0.0` |
| `ae_brwptr` | `` |
| `ae_cbtype` | `P1` |
| `ae_cntr` | `` |
| `ae_comment` | `test` |
| `ae_complet` | `1.0` |
| `ae_entref` | `test` |
| `ae_entry` | `P100000768` |
| `ae_frstat` | `0.0` |
| `ae_lstdate` | `2024-05-01T00:00:00` |
| `ae_payid` | `0.0` |
| `ae_postgrp` | `0.0` |
| `ae_recbal` | `0.0` |
| `ae_reclnum` | `0.0` |
| `ae_remove` | `0.0` |
| `ae_statln` | `0.0` |
| `ae_tmpstat` | `0.0` |
| `ae_tostat` | `0.0` |
| `ae_value` | `-10000.0` |
| `sq_amtime` | `` |
| `sq_amuser` | `` |
| `sq_crdate` | `2024-05-01T00:00:00` |
| `sq_crtime` | `13:51:57` |
| `sq_cruser` | `TEST` |


#### anoml

**3 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `ax_comment` | `test` |
| `ax_date` | `2024-05-01T00:00:00` |
| `ax_done` | `` |
| `ax_fcdec` | `2.0` |
| `ax_fcmult` | `0.0` |
| `ax_fcrate` | `1.0` |
| `ax_fcurr` | `` |
| `ax_fvalue` | `8333.0` |
| `ax_job` | `SM` |
| `ax_jrnl` | `0.0` |
| `ax_nacnt` | `S120` |
| `ax_ncntr` | `ADM` |
| `ax_nlpdate` | `2024-05-01T00:00:00` |
| `ax_project` | `` |
| `ax_source` | `A` |
| `ax_srcco` | `Z` |
| `ax_tref` | `test` |
| `ax_unique` | `_7FL0TPWZX` |
| `ax_value` | `83.33` |

*Row 2:*
| Field | Value |
|-------|-------|
| `ax_comment` | `test` |
| `ax_date` | `2024-05-01T00:00:00` |
| `ax_done` | `` |
| `ax_fcdec` | `2.0` |
| `ax_fcmult` | `0.0` |
| `ax_fcrate` | `1.0` |
| `ax_fcurr` | `` |
| `ax_fvalue` | `1667.0` |
| `ax_job` | `` |
| `ax_jrnl` | `0.0` |
| `ax_nacnt` | `E225` |
| `ax_ncntr` | `` |
| `ax_nlpdate` | `2024-05-01T00:00:00` |
| `ax_project` | `` |
| `ax_source` | `A` |
| `ax_srcco` | `Z` |
| `ax_tref` | `test` |
| `ax_unique` | `_7FL0TPWZX` |
| `ax_value` | `16.67` |

*Row 3:*
| Field | Value |
|-------|-------|
| `ax_comment` | `test` |
| `ax_date` | `2024-05-01T00:00:00` |
| `ax_done` | `` |
| `ax_fcdec` | `2.0` |
| `ax_fcmult` | `0.0` |
| `ax_fcrate` | `1.0` |
| `ax_fcurr` | `` |
| `ax_fvalue` | `-10000.0` |
| `ax_job` | `` |
| `ax_jrnl` | `0.0` |
| `ax_nacnt` | `C310` |
| `ax_ncntr` | `` |
| `ax_nlpdate` | `2024-05-01T00:00:00` |
| `ax_project` | `` |
| `ax_source` | `A` |
| `ax_srcco` | `Z` |
| `ax_tref` | `test` |
| `ax_unique` | `_7FL0TPWZX` |
| `ax_value` | `-100.0` |


#### atran

**2 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `at_account` | `S120    ADM` |
| `at_acnt` | `C310` |
| `at_atpycd` | `` |
| `at_bacprn` | `0.0` |
| `at_bic` | `` |
| `at_bsname` | `` |
| `at_bsref` | `` |
| `at_cash` | `0.0` |
| `at_cbtype` | `P1` |
| `at_ccauth` | `` |
| `at_ccdno` | `` |
| `at_ccdprn` | `0.0` |
| `at_chqlst` | `0.0` |
| `at_chqprn` | `0.0` |
| `at_cntr` | `` |
| `at_comment` | `test` |
| `at_disc` | `0.0` |
| `at_ecb` | `0.0` |
| `at_ecbpayd` | `2024-05-01T00:00:00` |
| `at_ecbtype` | `` |
| `at_entry` | `P100000768` |
| `at_fcdec` | `2.0` |
| `at_fcexch` | `1.0` |
| `at_fcmult` | `0.0` |
| `at_fcurr` | `` |
| `at_iban` | `` |
| `at_inputby` | `TEST` |
| `at_job` | `SM` |
| `at_memo` | `` |
| `at_name` | `Travel Expenses / Subsistence` |
| `at_number` | `` |
| `at_payee` | `` |
| `at_payname` | `` |
| `at_payslp` | `0.0` |
| `at_postgrp` | `0.0` |
| `at_project` | `` |
| `at_pstdate` | `2024-05-01T00:00:00` |
| `at_pysprn` | `0.0` |
| `at_refer` | `test` |
| `at_remit` | `0.0` |
| `at_remove` | `0.0` |
| `at_sort` | `` |
| `at_srcco` | `Z` |
| `at_sysdate` | `2024-05-01T00:00:00` |
| `at_tperiod` | `1.0` |
| `at_type` | `1.0` |
| `at_unique` | `_7FL0TPWZX` |
| `at_value` | `-8333.0` |
| `at_vattycd` | `` |

*Row 2:*
| Field | Value |
|-------|-------|
| `at_account` | `E225` |
| `at_acnt` | `C310` |
| `at_atpycd` | `` |
| `at_bacprn` | `0.0` |
| `at_bic` | `` |
| `at_bsname` | `` |
| `at_bsref` | `` |
| `at_cash` | `0.0` |
| `at_cbtype` | `P1` |
| `at_ccauth` | `` |
| `at_ccdno` | `` |
| `at_ccdprn` | `0.0` |
| `at_chqlst` | `0.0` |
| `at_chqprn` | `0.0` |
| `at_cntr` | `` |
| `at_comment` | `test` |
| `at_disc` | `0.0` |
| `at_ecb` | `0.0` |
| `at_ecbpayd` | `2024-05-01T00:00:00` |
| `at_ecbtype` | `` |
| `at_entry` | `P100000768` |
| `at_fcdec` | `2.0` |
| `at_fcexch` | `1.0` |
| `at_fcmult` | `0.0` |
| `at_fcurr` | `` |
| `at_iban` | `` |
| `at_inputby` | `TEST` |
| `at_job` | `SM` |
| `at_memo` | `` |
| `at_name` | `VAT (Input - Purchases)` |
| `at_number` | `` |
| `at_payee` | `` |
| `at_payname` | `` |
| `at_payslp` | `0.0` |
| `at_postgrp` | `0.0` |
| `at_project` | `` |
| `at_pstdate` | `2024-05-01T00:00:00` |
| `at_pysprn` | `0.0` |
| `at_refer` | `test` |
| `at_remit` | `0.0` |
| `at_remove` | `0.0` |
| `at_sort` | `` |
| `at_srcco` | `Z` |
| `at_sysdate` | `2024-05-01T00:00:00` |
| `at_tperiod` | `1.0` |
| `at_type` | `1.0` |
| `at_unique` | `_7FL0TPWZX` |
| `at_value` | `-1667.0` |
| `at_vattycd` | `` |


#### atype

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `ay_entry` | `P100000768` | `P100000769` |


#### nbank

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `nk_curbal` | `4798026.0` | `4788026.0` |


#### nextid

**4 row(s) modified:**

*Row id=10.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `11058.0` | `11059.0` |

*Row id=13.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `26118.0` | `26121.0` |

*Row id=21.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `17743.0` | `17745.0` |

*Row id=246.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `1.0` | `2.0` |


#### zlock

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|


---

### Nominal Receipt

#### aentry

**1 row(s) modified:**

*Row id=11044.0:*
| Field | Before | After |
|-------|--------|-------|
| `ae_comment` | `` | `test` |
| `ae_entref` | `PAY` | `test` |
| `ae_postgrp` | `0.0` | `1.0` |
| `ae_value` | `225000.0` | `230500.0` |
| `sq_amdate` | `NaT` | `2024-05-01T00:00:00` |
| `sq_amtime` | `` | `14:13:01` |
| `sq_amuser` | `` | `TEST` |


#### anoml

**2 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `ax_comment` | `test` |
| `ax_date` | `2024-05-01T00:00:00` |
| `ax_done` | `Y` |
| `ax_fcdec` | `2.0` |
| `ax_fcmult` | `0.0` |
| `ax_fcrate` | `1.0` |
| `ax_fcurr` | `` |
| `ax_fvalue` | `5500.0` |
| `ax_job` | `` |
| `ax_jrnl` | `3446.0` |
| `ax_nacnt` | `C310` |
| `ax_ncntr` | `` |
| `ax_nlpdate` | `2024-05-01T00:00:00` |
| `ax_project` | `` |
| `ax_source` | `A` |
| `ax_srcco` | `Z` |
| `ax_tref` | `test` |
| `ax_unique` | `_7FL0UH01D` |
| `ax_value` | `55.0` |

*Row 2:*
| Field | Value |
|-------|-------|
| `ax_comment` | `test` |
| `ax_date` | `2024-05-01T00:00:00` |
| `ax_done` | `Y` |
| `ax_fcdec` | `2.0` |
| `ax_fcmult` | `0.0` |
| `ax_fcrate` | `1.0` |
| `ax_fcurr` | `` |
| `ax_fvalue` | `-5500.0` |
| `ax_job` | `` |
| `ax_jrnl` | `3446.0` |
| `ax_nacnt` | `S120` |
| `ax_ncntr` | `ADM` |
| `ax_nlpdate` | `2024-05-01T00:00:00` |
| `ax_project` | `` |
| `ax_source` | `A` |
| `ax_srcco` | `Z` |
| `ax_tref` | `test` |
| `ax_unique` | `_7FL0UH01D` |
| `ax_value` | `-55.0` |


#### atran

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `at_account` | `S120    ADM` |
| `at_acnt` | `C310` |
| `at_atpycd` | `` |
| `at_bacprn` | `0.0` |
| `at_bic` | `` |
| `at_bsname` | `` |
| `at_bsref` | `` |
| `at_cash` | `0.0` |
| `at_cbtype` | `R1` |
| `at_ccauth` | `` |
| `at_ccdno` | `` |
| `at_ccdprn` | `0.0` |
| `at_chqlst` | `0.0` |
| `at_chqprn` | `0.0` |
| `at_cntr` | `` |
| `at_comment` | `test` |
| `at_disc` | `0.0` |
| `at_ecb` | `0.0` |
| `at_ecbpayd` | `2024-05-01T00:00:00` |
| `at_ecbtype` | `` |
| `at_entry` | `R100000232` |
| `at_fcdec` | `2.0` |
| `at_fcexch` | `1.0` |
| `at_fcmult` | `0.0` |
| `at_fcurr` | `` |
| `at_iban` | `` |
| `at_inputby` | `TEST` |
| `at_job` | `` |
| `at_memo` | `` |
| `at_name` | `Travel Expenses / Subsistence` |
| `at_number` | `` |
| `at_payee` | `` |
| `at_payname` | `` |
| `at_payslp` | `0.0` |
| `at_postgrp` | `1.0` |
| `at_project` | `` |
| `at_pstdate` | `2024-05-01T00:00:00` |
| `at_pysprn` | `1.0` |
| `at_refer` | `test` |
| `at_remit` | `0.0` |
| `at_remove` | `0.0` |
| `at_sort` | `` |
| `at_srcco` | `Z` |
| `at_sysdate` | `2024-05-01T00:00:00` |
| `at_tperiod` | `1.0` |
| `at_type` | `2.0` |
| `at_unique` | `_7FL0UH01D` |
| `at_value` | `5500.0` |
| `at_vattycd` | `` |


#### idtab

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `id_numericid` | `3446.0` | `3447.0` |


#### nacnt

**2 row(s) modified:**

*Row id=16.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `-27387.95` | `-27332.95` |
| `na_ptddr` | `0.0` | `55.0` |
| `na_ytddr` | `272086.37` | `272141.37` |

*Row id=137.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `83.33` | `28.33` |
| `na_ptdcr` | `0.0` | `55.0` |
| `na_ytdcr` | `0.0` | `55.0` |


#### nbank

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `nk_curbal` | `4788026.0` | `4793526.0` |


#### ndetail

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `nt_acnt` | `C310` |
| `nt_cdesc` | `` |
| `nt_cmnt` | `test` |
| `nt_cntr` | `` |
| `nt_consol` | `0.0` |
| `nt_distrib` | `0.0` |
| `nt_entr` | `2024-05-01T00:00:00` |
| `nt_fcdec` | `0.0` |
| `nt_fcmult` | `0.0` |
| `nt_fcrate` | `0.0` |
| `nt_fcurr` | `` |
| `nt_fvalue` | `0.0` |
| `nt_inp` | `TEST` |
| `nt_job` | `` |
| `nt_jrnl` | `3446.0` |
| `nt_period` | `5.0` |
| `nt_perpost` | `0.0` |
| `nt_posttyp` | `A` |
| `nt_prevyr` | `0.0` |
| `nt_project` | `` |
| `nt_pstgrp` | `1.0` |
| `nt_pstid` | `_7FL0UH0Q3` |
| `nt_recjrnl` | `0.0` |
| `nt_rectify` | `0.0` |
| `nt_recurr` | `0.0` |
| `nt_ref` | `` |
| `nt_rvrse` | `0.0` |
| `nt_srcco` | `Z` |
| `nt_subt` | `03` |
| `nt_trnref` | `test` |
| `nt_trtype` | `A` |
| `nt_type` | `10` |
| `nt_value` | `55.0` |
| `nt_vatanal` | `0.0` |
| `nt_year` | `2024.0` |


#### nextid

**6 row(s) modified:**

*Row id=13.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `26121.0` | `26123.0` |

*Row id=21.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `17745.0` | `17746.0` |

*Row id=229.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `6894.0` | `6895.0` |

*Row id=234.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `159919.0` | `159920.0` |

*Row id=235.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `210.0` | `211.0` |


#### nhist

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `nh_bal` | `-55.0` |
| `nh_budg` | `0.0` |
| `nh_fbal` | `0.0` |
| `nh_job` | `` |
| `nh_nacnt` | `S120` |
| `nh_ncntr` | `ADM` |
| `nh_nsubt` | `03` |
| `nh_ntype` | `45` |
| `nh_period` | `5.0` |
| `nh_project` | `` |
| `nh_ptdcr` | `-55.0` |
| `nh_ptddr` | `0.0` |
| `nh_rbudg` | `0.0` |
| `nh_rectype` | `1.0` |
| `nh_year` | `2024.0` |

**1 row(s) modified:**

*Row id=159882.0:*
| Field | Before | After |
|-------|--------|-------|
| `nh_bal` | `-27387.95` | `-27332.95` |
| `nh_ptddr` | `0.0` | `55.0` |


#### njmemo

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `nj_binrep` | `0.0` |
| `nj_image` | `` |
| `nj_journal` | `3446.0` |
| `nj_memo` | `ÿ<<JOURNAL_DATA_ONLY>>ÿ` |
| `nj_txtrep` | `Cashbook Ledger Transfer` |


#### nsubt

**2 row(s) modified:**

*Row id=8.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `2770848.35` | `2770903.35` |

*Row id=23.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `83.33` | `28.33` |


#### ntype

**2 row(s) modified:**

*Row id=3.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `5115790.67` | `5115845.67` |

*Row id=10.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `374276.8` | `374221.8` |


#### zlock

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|


---

### Recurring entry posting

#### aentry

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `ae_acnt` | `C310` |
| `ae_batchid` | `0.0` |
| `ae_brwptr` | `` |
| `ae_cbtype` | `P1` |
| `ae_cntr` | `` |
| `ae_comment` | `` |
| `ae_complet` | `1.0` |
| `ae_entref` | `test` |
| `ae_entry` | `P100000769` |
| `ae_frstat` | `0.0` |
| `ae_lstdate` | `2024-05-01T00:00:00` |
| `ae_payid` | `0.0` |
| `ae_postgrp` | `0.0` |
| `ae_recbal` | `0.0` |
| `ae_reclnum` | `0.0` |
| `ae_remove` | `0.0` |
| `ae_statln` | `0.0` |
| `ae_tmpstat` | `0.0` |
| `ae_tostat` | `0.0` |
| `ae_value` | `-10000.0` |
| `sq_amtime` | `` |
| `sq_amuser` | `` |
| `sq_crdate` | `2024-05-01T00:00:00` |
| `sq_crtime` | `14:46:39` |
| `sq_cruser` | `TEST` |


#### anoml

**3 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `ax_comment` | `` |
| `ax_date` | `2024-05-01T00:00:00` |
| `ax_done` | `Y` |
| `ax_fcdec` | `2.0` |
| `ax_fcmult` | `0.0` |
| `ax_fcrate` | `1.0` |
| `ax_fcurr` | `` |
| `ax_fvalue` | `-10000.0` |
| `ax_job` | `` |
| `ax_jrnl` | `3452.0` |
| `ax_nacnt` | `C310` |
| `ax_ncntr` | `` |
| `ax_nlpdate` | `2024-05-01T00:00:00` |
| `ax_project` | `` |
| `ax_source` | `A` |
| `ax_srcco` | `Z` |
| `ax_tref` | `test` |
| `ax_unique` | `_7FL0VO92J` |
| `ax_value` | `-100.0` |

*Row 2:*
| Field | Value |
|-------|-------|
| `ax_comment` | `` |
| `ax_date` | `2024-05-01T00:00:00` |
| `ax_done` | `Y` |
| `ax_fcdec` | `2.0` |
| `ax_fcmult` | `0.0` |
| `ax_fcrate` | `1.0` |
| `ax_fcurr` | `` |
| `ax_fvalue` | `1667.0` |
| `ax_job` | `` |
| `ax_jrnl` | `3452.0` |
| `ax_nacnt` | `E225` |
| `ax_ncntr` | `` |
| `ax_nlpdate` | `2024-05-01T00:00:00` |
| `ax_project` | `` |
| `ax_source` | `A` |
| `ax_srcco` | `Z` |
| `ax_tref` | `test` |
| `ax_unique` | `_7FL0VO92J` |
| `ax_value` | `16.67` |

*Row 3:*
| Field | Value |
|-------|-------|
| `ax_comment` | `` |
| `ax_date` | `2024-05-01T00:00:00` |
| `ax_done` | `Y` |
| `ax_fcdec` | `2.0` |
| `ax_fcmult` | `0.0` |
| `ax_fcrate` | `1.0` |
| `ax_fcurr` | `` |
| `ax_fvalue` | `8333.0` |
| `ax_job` | `` |
| `ax_jrnl` | `3452.0` |
| `ax_nacnt` | `S120` |
| `ax_ncntr` | `ADM` |
| `ax_nlpdate` | `2024-05-01T00:00:00` |
| `ax_project` | `` |
| `ax_source` | `A` |
| `ax_srcco` | `Z` |
| `ax_tref` | `test` |
| `ax_unique` | `_7FL0VO92J` |
| `ax_value` | `83.33` |


#### aparm

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `ap_nextrec` | `REC0000026` | `REC0000027` |


#### arhead

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `ae_acnt` | `C310` |
| `ae_cntr` | `` |
| `ae_desc` | `test` |
| `ae_entry` | `REC0000026` |
| `ae_every` | `1.0` |
| `ae_freq` | `M` |
| `ae_inputby` | `TEST` |
| `ae_lstpost` | `2024-05-01T00:00:00` |
| `ae_nxtpost` | `2024-06-01T00:00:00` |
| `ae_posted` | `1.0` |
| `ae_srcco` | `Z` |
| `ae_topost` | `1.0` |
| `ae_type` | `1.0` |
| `ae_vatanal` | `1.0` |
| `sq_amtime` | `` |
| `sq_amuser` | `` |
| `sq_crdate` | `2024-05-01T00:00:00` |
| `sq_crtime` | `14:46:16` |
| `sq_cruser` | `TEST` |
| `sq_memo` | `` |


#### arline

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `at_account` | `S120    ADM` |
| `at_acnt` | `C310` |
| `at_atpycd` | `` |
| `at_bacprn` | `0.0` |
| `at_bic` | `` |
| `at_cbtype` | `P1` |
| `at_ccdno` | `` |
| `at_chqprn` | `0.0` |
| `at_cntr` | `` |
| `at_comment` | `` |
| `at_disc` | `0.0` |
| `at_discnl` | `` |
| `at_ecb` | `0.0` |
| `at_ecbtype` | `` |
| `at_entref` | `test` |
| `at_entry` | `REC0000026` |
| `at_fcdec` | `2.0` |
| `at_fcurr` | `` |
| `at_iban` | `` |
| `at_job` | `` |
| `at_line` | `1.0` |
| `at_number` | `` |
| `at_payee` | `` |
| `at_payname` | `` |
| `at_project` | `` |
| `at_ref2` | `` |
| `at_remit` | `0.0` |
| `at_sort` | `` |
| `at_srcco` | `` |
| `at_unique` | `` |
| `at_value` | `-10000.0` |
| `at_vatcde` | `1` |
| `at_vattyp` | `P` |
| `at_vatval` | `-1667.0` |
| `sq_amtime` | `` |
| `sq_amuser` | `` |
| `sq_crdate` | `2024-05-01T00:00:00` |
| `sq_crtime` | `14:46:12` |
| `sq_cruser` | `TEST` |
| `sq_memo` | `` |


#### atran

**2 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `at_account` | `S120    ADM` |
| `at_acnt` | `C310` |
| `at_atpycd` | `` |
| `at_bacprn` | `0.0` |
| `at_bic` | `` |
| `at_bsname` | `` |
| `at_bsref` | `` |
| `at_cash` | `0.0` |
| `at_cbtype` | `P1` |
| `at_ccauth` | `` |
| `at_ccdno` | `` |
| `at_ccdprn` | `0.0` |
| `at_chqlst` | `0.0` |
| `at_chqprn` | `0.0` |
| `at_cntr` | `` |
| `at_comment` | `` |
| `at_disc` | `0.0` |
| `at_ecb` | `0.0` |
| `at_ecbpayd` | `2024-05-01T00:00:00` |
| `at_ecbtype` | `` |
| `at_entry` | `P100000769` |
| `at_fcdec` | `2.0` |
| `at_fcexch` | `1.0` |
| `at_fcmult` | `0.0` |
| `at_fcurr` | `` |
| `at_iban` | `` |
| `at_inputby` | `TEST` |
| `at_job` | `` |
| `at_memo` | `` |
| `at_name` | `Travel Expenses / Subsistence` |
| `at_number` | `` |
| `at_payee` | `` |
| `at_payname` | `` |
| `at_payslp` | `0.0` |
| `at_postgrp` | `0.0` |
| `at_project` | `` |
| `at_pstdate` | `2024-05-01T00:00:00` |
| `at_pysprn` | `0.0` |
| `at_refer` | `test` |
| `at_remit` | `0.0` |
| `at_remove` | `0.0` |
| `at_sort` | `` |
| `at_srcco` | `Z` |
| `at_sysdate` | `2024-05-01T00:00:00` |
| `at_tperiod` | `1.0` |
| `at_type` | `1.0` |
| `at_unique` | `_7FL0VO97M` |
| `at_value` | `-8333.0` |
| `at_vattycd` | `` |

*Row 2:*
| Field | Value |
|-------|-------|
| `at_account` | `E225` |
| `at_acnt` | `C310` |
| `at_atpycd` | `` |
| `at_bacprn` | `0.0` |
| `at_bic` | `` |
| `at_bsname` | `` |
| `at_bsref` | `` |
| `at_cash` | `0.0` |
| `at_cbtype` | `P1` |
| `at_ccauth` | `` |
| `at_ccdno` | `` |
| `at_ccdprn` | `0.0` |
| `at_chqlst` | `0.0` |
| `at_chqprn` | `0.0` |
| `at_cntr` | `` |
| `at_comment` | `` |
| `at_disc` | `0.0` |
| `at_ecb` | `0.0` |
| `at_ecbpayd` | `2024-05-01T00:00:00` |
| `at_ecbtype` | `` |
| `at_entry` | `P100000769` |
| `at_fcdec` | `2.0` |
| `at_fcexch` | `1.0` |
| `at_fcmult` | `0.0` |
| `at_fcurr` | `` |
| `at_iban` | `` |
| `at_inputby` | `TEST` |
| `at_job` | `` |
| `at_memo` | `` |
| `at_name` | `VAT (Input - Purchases)` |
| `at_number` | `` |
| `at_payee` | `` |
| `at_payname` | `` |
| `at_payslp` | `0.0` |
| `at_postgrp` | `0.0` |
| `at_project` | `` |
| `at_pstdate` | `2024-05-01T00:00:00` |
| `at_pysprn` | `0.0` |
| `at_refer` | `test` |
| `at_remit` | `0.0` |
| `at_remove` | `0.0` |
| `at_sort` | `` |
| `at_srcco` | `Z` |
| `at_sysdate` | `2024-05-01T00:00:00` |
| `at_tperiod` | `1.0` |
| `at_type` | `1.0` |
| `at_unique` | `_7FL0VO97M` |
| `at_value` | `-1667.0` |
| `at_vattycd` | `` |


#### atype

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `ay_entry` | `P100000769` | `P100000770` |


#### idtab

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `id_numericid` | `3452.0` | `3453.0` |


#### nacnt

**3 row(s) modified:**

*Row id=16.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `-27431.95` | `-27531.95` |
| `na_ptdcr` | `27986.95` | `28086.95` |
| `na_ytdcr` | `224805.11` | `224905.11` |

*Row id=137.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `111.66` | `194.99` |
| `na_ptddr` | `166.66` | `249.99` |
| `na_ytddr` | `166.66` | `249.99` |

*Row id=252.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `75010.03` | `75026.7` |
| `na_ptddr` | `75010.03` | `75026.7` |
| `na_ytddr` | `750797.72` | `750814.39` |


#### nbank

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `nk_curbal` | `4783626.0` | `4773626.0` |
| `nk_reclock` | `` | `TEST` |


#### ndetail

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `nt_acnt` | `C310` |
| `nt_cdesc` | `` |
| `nt_cmnt` | `test` |
| `nt_cntr` | `` |
| `nt_consol` | `0.0` |
| `nt_distrib` | `0.0` |
| `nt_entr` | `2024-05-01T00:00:00` |
| `nt_fcdec` | `0.0` |
| `nt_fcmult` | `0.0` |
| `nt_fcrate` | `0.0` |
| `nt_fcurr` | `` |
| `nt_fvalue` | `0.0` |
| `nt_inp` | `TEST` |
| `nt_job` | `` |
| `nt_jrnl` | `3452.0` |
| `nt_period` | `5.0` |
| `nt_perpost` | `0.0` |
| `nt_posttyp` | `A` |
| `nt_prevyr` | `0.0` |
| `nt_project` | `` |
| `nt_pstgrp` | `1.0` |
| `nt_pstid` | `_7FL0VOOTP` |
| `nt_recjrnl` | `0.0` |
| `nt_rectify` | `0.0` |
| `nt_recurr` | `0.0` |
| `nt_ref` | `` |
| `nt_rvrse` | `0.0` |
| `nt_srcco` | `Z` |
| `nt_subt` | `03` |
| `nt_trnref` | `` |
| `nt_trtype` | `A` |
| `nt_type` | `10` |
| `nt_value` | `-100.0` |
| `nt_vatanal` | `0.0` |
| `nt_year` | `2024.0` |


#### nextid

**10 row(s) modified:**

*Row id=10.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `11060.0` | `11061.0` |

*Row id=13.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `26127.0` | `26130.0` |

*Row id=16.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `6.0` | `7.0` |

*Row id=17.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `9.0` | `10.0` |

*Row id=21.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `17748.0` | `17750.0` |


#### nhist

**3 row(s) modified:**

*Row id=159882.0:*
| Field | Before | After |
|-------|--------|-------|
| `nh_bal` | `-27431.95` | `-27531.95` |
| `nh_ptdcr` | `-27986.95` | `-28086.95` |

*Row id=159888.0:*
| Field | Before | After |
|-------|--------|-------|
| `nh_bal` | `75010.03` | `75026.7` |
| `nh_ptddr` | `75010.03` | `75026.7` |

*Row id=159919.0:*
| Field | Before | After |
|-------|--------|-------|
| `nh_bal` | `28.33` | `111.66` |
| `nh_ptddr` | `83.33` | `166.66` |


#### njmemo

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `nj_binrep` | `0.0` |
| `nj_image` | `` |
| `nj_journal` | `3452.0` |
| `nj_memo` | `ÿ<<JOURNAL_DATA_ONLY>>ÿ` |
| `nj_txtrep` | `Cashbook Ledger Transfer` |


#### nsubt

**3 row(s) modified:**

*Row id=8.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `2770804.35` | `2770704.35` |

*Row id=11.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `-54940.12` | `-54923.45` |

*Row id=23.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `111.66` | `194.99` |


#### ntype

**3 row(s) modified:**

*Row id=3.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `5115126.67` | `5115026.67` |

*Row id=4.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `-284709.08` | `-284692.41` |

*Row id=10.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `374305.13` | `374388.46` |


#### nvat

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `nv_acnt` | `S120` |
| `nv_advance` | `0.0` |
| `nv_cntr` | `ADM` |
| `nv_comment` | `` |
| `nv_crdate` | `2024-05-01T00:00:00` |
| `nv_date` | `2024-05-01T00:00:00` |
| `nv_ref` | `test` |
| `nv_taxdate` | `2024-05-01T00:00:00` |
| `nv_type` | `I` |
| `nv_value` | `100.0` |
| `nv_vatcode` | `1` |
| `nv_vatctry` | `H` |
| `nv_vatrate` | `20.0` |
| `nv_vattype` | `P` |
| `nv_vatval` | `16.67` |


#### zpool

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `sp_cby` | `TEST` |
| `sp_cdate` | `2026-04-01T00:00:00` |
| `sp_ctime` | `14:46` |
| `sp_desc` | `dfg` |
| `sp_file` | `TESTSDGV` |
| `sp_origin` | `` |
| `sp_pby` | `` |
| `sp_platfrm` | `32BIT` |
| `sp_printer` | `PDF:` |
| `sp_ptime` | `` |
| `sp_rephite` | `0.0` |
| `sp_repwide` | `0.0` |


---

### Sales Receipt — BACS

#### aentry

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `ae_acnt` | `C310` |
| `ae_batchid` | `0.0` |
| `ae_brwptr` | `` |
| `ae_cbtype` | `R2` |
| `ae_cntr` | `` |
| `ae_comment` | `` |
| `ae_complet` | `1.0` |
| `ae_entref` | `rec` |
| `ae_entry` | `R200000718` |
| `ae_frstat` | `0.0` |
| `ae_lstdate` | `2024-05-01T00:00:00` |
| `ae_payid` | `0.0` |
| `ae_postgrp` | `0.0` |
| `ae_recbal` | `0.0` |
| `ae_reclnum` | `0.0` |
| `ae_remove` | `0.0` |
| `ae_statln` | `0.0` |
| `ae_tmpstat` | `0.0` |
| `ae_tostat` | `0.0` |
| `ae_value` | `2399.0` |
| `sq_amtime` | `` |
| `sq_amuser` | `` |
| `sq_crdate` | `2026-04-01T00:00:00` |
| `sq_crtime` | `12:46:51` |
| `sq_cruser` | `TEST` |


#### anoml

**2 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `ax_comment` | `Adams Light Engineering Ltd   BACS` |
| `ax_date` | `2024-05-01T00:00:00` |
| `ax_done` | `` |
| `ax_fcdec` | `0.0` |
| `ax_fcmult` | `0.0` |
| `ax_fcrate` | `0.0` |
| `ax_fcurr` | `` |
| `ax_fvalue` | `0.0` |
| `ax_job` | `` |
| `ax_jrnl` | `0.0` |
| `ax_nacnt` | `C310` |
| `ax_ncntr` | `` |
| `ax_nlpdate` | `2024-05-01T00:00:00` |
| `ax_project` | `` |
| `ax_source` | `S` |
| `ax_srcco` | `Z` |
| `ax_tref` | `rec` |
| `ax_unique` | `_7FL0RDZ8K` |
| `ax_value` | `23.99` |

*Row 2:*
| Field | Value |
|-------|-------|
| `ax_comment` | `Adams Light Engineering Ltd   BACS` |
| `ax_date` | `2024-05-01T00:00:00` |
| `ax_done` | `` |
| `ax_fcdec` | `0.0` |
| `ax_fcmult` | `0.0` |
| `ax_fcrate` | `0.0` |
| `ax_fcurr` | `` |
| `ax_fvalue` | `0.0` |
| `ax_job` | `` |
| `ax_jrnl` | `0.0` |
| `ax_nacnt` | `C110` |
| `ax_ncntr` | `` |
| `ax_nlpdate` | `2024-05-01T00:00:00` |
| `ax_project` | `` |
| `ax_source` | `S` |
| `ax_srcco` | `Z` |
| `ax_tref` | `rec` |
| `ax_unique` | `_7FL0RDZ8K` |
| `ax_value` | `-23.99` |


#### atran

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `at_account` | `ADA0001` |
| `at_acnt` | `C310` |
| `at_atpycd` | `` |
| `at_bacprn` | `0.0` |
| `at_bic` | `` |
| `at_bsname` | `` |
| `at_bsref` | `` |
| `at_cash` | `0.0` |
| `at_cbtype` | `R2` |
| `at_ccauth` | `0` |
| `at_ccdno` | `` |
| `at_ccdprn` | `0.0` |
| `at_chqlst` | `0.0` |
| `at_chqprn` | `0.0` |
| `at_cntr` | `` |
| `at_comment` | `` |
| `at_disc` | `0.0` |
| `at_ecb` | `0.0` |
| `at_ecbtype` | `` |
| `at_entry` | `R200000718` |
| `at_fcdec` | `2.0` |
| `at_fcexch` | `1.0` |
| `at_fcmult` | `0.0` |
| `at_fcurr` | `` |
| `at_iban` | `` |
| `at_inputby` | `TEST` |
| `at_job` | `` |
| `at_memo` | `` |
| `at_name` | `Adams Light Engineering Ltd` |
| `at_number` | `` |
| `at_payee` | `` |
| `at_payname` | `` |
| `at_payslp` | `0.0` |
| `at_postgrp` | `0.0` |
| `at_project` | `` |
| `at_pstdate` | `2024-05-01T00:00:00` |
| `at_pysprn` | `0.0` |
| `at_refer` | `rec` |
| `at_remit` | `0.0` |
| `at_remove` | `0.0` |
| `at_sort` | `` |
| `at_srcco` | `Z` |
| `at_sysdate` | `2024-05-01T00:00:00` |
| `at_tperiod` | `1.0` |
| `at_type` | `4.0` |
| `at_unique` | `_7FL0RDZ8K` |
| `at_value` | `2399.0` |
| `at_vattycd` | `` |


#### atype

**1 row(s) modified:**

*Row id=10.0:*
| Field | Before | After |
|-------|--------|-------|
| `ay_entry` | `R200000718` | `R200000719` |


#### nbank

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `nk_curbal` | `4812026.0` | `4814425.0` |


#### nextid

**5 row(s) modified:**

*Row id=10.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `11046.0` | `11047.0` |

*Row id=13.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `26096.0` | `26098.0` |

*Row id=21.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `17730.0` | `17731.0` |

*Row id=269.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `1746.0` | `1748.0` |

*Row id=286.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `9281.0` | `9282.0` |


#### salloc

**2 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `al_account` | `ADA0001` |
| `al_acnt` | `C310` |
| `al_adjsv` | `0.0` |
| `al_advind` | `0.0` |
| `al_cntr` | `` |
| `al_date` | `2024-04-19T00:00:00` |
| `al_fcurr` | `` |
| `al_fdec` | `0.0` |
| `al_fval` | `0.0` |
| `al_payday` | `2024-05-01T00:00:00` |
| `al_payflag` | `91.0` |
| `al_payind` | `A` |
| `al_preprd` | `0.0` |
| `al_ref1` | `INV05188` |
| `al_ref2` | `AHL-CONT-0722/AX001` |
| `al_type` | `I` |
| `al_unique` | `9213.0` |
| `al_val` | `23.99` |

*Row 2:*
| Field | Value |
|-------|-------|
| `al_account` | `ADA0001` |
| `al_acnt` | `C310` |
| `al_adjsv` | `0.0` |
| `al_advind` | `0.0` |
| `al_cntr` | `` |
| `al_date` | `2024-05-01T00:00:00` |
| `al_fcurr` | `` |
| `al_fdec` | `0.0` |
| `al_fval` | `0.0` |
| `al_payday` | `2024-05-01T00:00:00` |
| `al_payflag` | `91.0` |
| `al_payind` | `A` |
| `al_preprd` | `0.0` |
| `al_ref1` | `rec` |
| `al_ref2` | `BACS` |
| `al_type` | `R` |
| `al_unique` | `9281.0` |
| `al_val` | `-23.99` |


#### sname

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `sn_currbal` | `16756.17` | `16732.18` |
| `sn_nextpay` | `91.0` | `92.0` |


#### stran

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `jxrenewal` | `0.0` |
| `jxservid` | `0.0` |
| `st_account` | `ADA0001` |
| `st_adjsv` | `0.0` |
| `st_advallc` | `0.0` |
| `st_advance` | `N` |
| `st_binrep` | `0.0` |
| `st_cash` | `0.0` |
| `st_cbtype` | `R2` |
| `st_crdate` | `2024-05-01T00:00:00` |
| `st_custref` | `BACS` |
| `st_delacc` | `` |
| `st_dispute` | `0.0` |
| `st_edi` | `0.0` |
| `st_editx` | `0.0` |
| `st_edivn` | `0.0` |
| `st_entry` | `R200000718` |
| `st_eurind` | `` |
| `st_euro` | `0.0` |
| `st_exttime` | `` |
| `st_fadval` | `0.0` |
| `st_fcbal` | `0.0` |
| `st_fcdec` | `0.0` |
| `st_fcmult` | `0.0` |
| `st_fcrate` | `0.0` |
| `st_fcurr` | `` |
| `st_fcval` | `0.0` |
| `st_fcvat` | `0.0` |
| `st_fullamt` | `0.0` |
| `st_fullcb` | `` |
| `st_fullnar` | `` |
| `st_gateid` | `0.0` |
| `st_gatetr` | `0.0` |
| `st_luptime` | `` |
| `st_memo` | `Analysis of Receipt rec                   Amount        23.99  Dated 01/05/20...` |
| `st_nlpdate` | `2024-05-01T00:00:00` |
| `st_origcur` | `` |
| `st_paid` | `A` |
| `st_payadvl` | `0.0` |
| `st_payday` | `2024-05-01T00:00:00` |
| `st_payflag` | `91.0` |
| `st_rcode` | `` |
| `st_region` | `` |
| `st_revchrg` | `0.0` |
| `st_ruser` | `` |
| `st_set1` | `0.0` |
| `st_set1day` | `0.0` |
| `st_set2` | `0.0` |
| `st_set2day` | `0.0` |
| `st_terr` | `` |
| `st_trbal` | `0.0` |
| `st_trdate` | `2024-05-01T00:00:00` |
| `st_trref` | `rec` |
| `st_trtype` | `R` |
| `st_trvalue` | `-23.99` |
| `st_txtrep` | `` |
| `st_type` | `` |
| `st_unique` | `_7FL0RDZ8K` |
| `st_vatval` | `0.0` |

**1 row(s) modified:**

*Row id=9213.0:*
| Field | Before | After |
|-------|--------|-------|
| `st_lastrec` | `NaT` | `2024-05-01T00:00:00` |
| `st_paid` | `` | `P` |
| `st_payday` | `NaT` | `2024-05-01T00:00:00` |
| `st_payflag` | `0.0` | `91.0` |
| `st_trbal` | `23.99` | `0.0` |


#### zlock

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|


---

### Sales Receipt — Cheque

#### aentry

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `ae_acnt` | `C310` |
| `ae_batchid` | `0.0` |
| `ae_brwptr` | `` |
| `ae_cbtype` | `R2` |
| `ae_cntr` | `` |
| `ae_comment` | `` |
| `ae_complet` | `1.0` |
| `ae_entref` | `pay` |
| `ae_entry` | `R200000717` |
| `ae_frstat` | `0.0` |
| `ae_lstdate` | `2024-05-01T00:00:00` |
| `ae_payid` | `0.0` |
| `ae_postgrp` | `0.0` |
| `ae_recbal` | `0.0` |
| `ae_reclnum` | `0.0` |
| `ae_remove` | `0.0` |
| `ae_statln` | `0.0` |
| `ae_tmpstat` | `0.0` |
| `ae_tostat` | `0.0` |
| `ae_value` | `2399.0` |
| `sq_amtime` | `` |
| `sq_amuser` | `` |
| `sq_crdate` | `2026-04-01T00:00:00` |
| `sq_crtime` | `12:41:12` |
| `sq_cruser` | `TEST` |


#### anoml

**2 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `ax_comment` | `Adams Light Engineering Ltd   BACS` |
| `ax_date` | `2024-05-01T00:00:00` |
| `ax_done` | `` |
| `ax_fcdec` | `0.0` |
| `ax_fcmult` | `0.0` |
| `ax_fcrate` | `0.0` |
| `ax_fcurr` | `` |
| `ax_fvalue` | `0.0` |
| `ax_job` | `` |
| `ax_jrnl` | `0.0` |
| `ax_nacnt` | `C310` |
| `ax_ncntr` | `` |
| `ax_nlpdate` | `2024-05-01T00:00:00` |
| `ax_project` | `` |
| `ax_source` | `S` |
| `ax_srcco` | `Z` |
| `ax_tref` | `pay` |
| `ax_unique` | `_7FL0R6FLA` |
| `ax_value` | `23.99` |

*Row 2:*
| Field | Value |
|-------|-------|
| `ax_comment` | `Adams Light Engineering Ltd   BACS` |
| `ax_date` | `2024-05-01T00:00:00` |
| `ax_done` | `` |
| `ax_fcdec` | `0.0` |
| `ax_fcmult` | `0.0` |
| `ax_fcrate` | `0.0` |
| `ax_fcurr` | `` |
| `ax_fvalue` | `0.0` |
| `ax_job` | `` |
| `ax_jrnl` | `0.0` |
| `ax_nacnt` | `C110` |
| `ax_ncntr` | `` |
| `ax_nlpdate` | `2024-05-01T00:00:00` |
| `ax_project` | `` |
| `ax_source` | `S` |
| `ax_srcco` | `Z` |
| `ax_tref` | `pay` |
| `ax_unique` | `_7FL0R6FLA` |
| `ax_value` | `-23.99` |


#### atran

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `at_account` | `ADA0001` |
| `at_acnt` | `C310` |
| `at_atpycd` | `` |
| `at_bacprn` | `0.0` |
| `at_bic` | `` |
| `at_bsname` | `` |
| `at_bsref` | `` |
| `at_cash` | `0.0` |
| `at_cbtype` | `R2` |
| `at_ccauth` | `0` |
| `at_ccdno` | `` |
| `at_ccdprn` | `0.0` |
| `at_chqlst` | `0.0` |
| `at_chqprn` | `0.0` |
| `at_cntr` | `` |
| `at_comment` | `` |
| `at_disc` | `0.0` |
| `at_ecb` | `0.0` |
| `at_ecbtype` | `` |
| `at_entry` | `R200000717` |
| `at_fcdec` | `2.0` |
| `at_fcexch` | `1.0` |
| `at_fcmult` | `0.0` |
| `at_fcurr` | `` |
| `at_iban` | `` |
| `at_inputby` | `TEST` |
| `at_job` | `` |
| `at_memo` | `` |
| `at_name` | `Adams Light Engineering Ltd` |
| `at_number` | `` |
| `at_payee` | `` |
| `at_payname` | `` |
| `at_payslp` | `0.0` |
| `at_postgrp` | `0.0` |
| `at_project` | `` |
| `at_pstdate` | `2024-05-01T00:00:00` |
| `at_pysprn` | `0.0` |
| `at_refer` | `pay` |
| `at_remit` | `0.0` |
| `at_remove` | `0.0` |
| `at_sort` | `` |
| `at_srcco` | `Z` |
| `at_sysdate` | `2024-05-01T00:00:00` |
| `at_tperiod` | `1.0` |
| `at_type` | `4.0` |
| `at_unique` | `_7FL0R6FLA` |
| `at_value` | `2399.0` |
| `at_vattycd` | `` |


#### atype

**1 row(s) modified:**

*Row id=10.0:*
| Field | Before | After |
|-------|--------|-------|
| `ay_entry` | `R200000717` | `R200000718` |


#### nbank

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `nk_curbal` | `4809627.0` | `4812026.0` |


#### nextid

**5 row(s) modified:**

*Row id=10.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `11045.0` | `11046.0` |

*Row id=13.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `26094.0` | `26096.0` |

*Row id=21.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `17729.0` | `17730.0` |

*Row id=269.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `1744.0` | `1746.0` |

*Row id=286.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `9280.0` | `9281.0` |


#### salloc

**2 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `al_account` | `ADA0001` |
| `al_acnt` | `C310` |
| `al_adjsv` | `0.0` |
| `al_advind` | `0.0` |
| `al_cntr` | `` |
| `al_date` | `2024-05-17T00:00:00` |
| `al_fcurr` | `` |
| `al_fdec` | `0.0` |
| `al_fval` | `0.0` |
| `al_payday` | `2024-05-01T00:00:00` |
| `al_payflag` | `90.0` |
| `al_payind` | `A` |
| `al_preprd` | `0.0` |
| `al_ref1` | `INV05214` |
| `al_ref2` | `AHL-CONT-0722/AX001` |
| `al_type` | `I` |
| `al_unique` | `9264.0` |
| `al_val` | `23.99` |

*Row 2:*
| Field | Value |
|-------|-------|
| `al_account` | `ADA0001` |
| `al_acnt` | `C310` |
| `al_adjsv` | `0.0` |
| `al_advind` | `0.0` |
| `al_cntr` | `` |
| `al_date` | `2024-05-01T00:00:00` |
| `al_fcurr` | `` |
| `al_fdec` | `0.0` |
| `al_fval` | `0.0` |
| `al_payday` | `2024-05-01T00:00:00` |
| `al_payflag` | `90.0` |
| `al_payind` | `A` |
| `al_preprd` | `0.0` |
| `al_ref1` | `pay` |
| `al_ref2` | `BACS` |
| `al_type` | `R` |
| `al_unique` | `9280.0` |
| `al_val` | `-23.99` |


#### sname

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `sn_currbal` | `16780.16` | `16756.17` |
| `sn_lastrec` | `2024-01-31T00:00:00` | `2024-05-01T00:00:00` |
| `sn_nextpay` | `90.0` | `91.0` |


#### stran

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `jxrenewal` | `0.0` |
| `jxservid` | `0.0` |
| `st_account` | `ADA0001` |
| `st_adjsv` | `0.0` |
| `st_advallc` | `0.0` |
| `st_advance` | `N` |
| `st_binrep` | `0.0` |
| `st_cash` | `0.0` |
| `st_cbtype` | `R2` |
| `st_crdate` | `2024-05-01T00:00:00` |
| `st_custref` | `BACS` |
| `st_delacc` | `` |
| `st_dispute` | `0.0` |
| `st_edi` | `0.0` |
| `st_editx` | `0.0` |
| `st_edivn` | `0.0` |
| `st_entry` | `R200000717` |
| `st_eurind` | `` |
| `st_euro` | `0.0` |
| `st_exttime` | `` |
| `st_fadval` | `0.0` |
| `st_fcbal` | `0.0` |
| `st_fcdec` | `0.0` |
| `st_fcmult` | `0.0` |
| `st_fcrate` | `0.0` |
| `st_fcurr` | `` |
| `st_fcval` | `0.0` |
| `st_fcvat` | `0.0` |
| `st_fullamt` | `0.0` |
| `st_fullcb` | `` |
| `st_fullnar` | `` |
| `st_gateid` | `0.0` |
| `st_gatetr` | `0.0` |
| `st_luptime` | `` |
| `st_memo` | `Analysis of Receipt pay                   Amount        23.99  Dated 01/05/20...` |
| `st_nlpdate` | `2024-05-01T00:00:00` |
| `st_origcur` | `` |
| `st_paid` | `A` |
| `st_payadvl` | `0.0` |
| `st_payday` | `2024-05-01T00:00:00` |
| `st_payflag` | `90.0` |
| `st_rcode` | `` |
| `st_region` | `` |
| `st_revchrg` | `0.0` |
| `st_ruser` | `` |
| `st_set1` | `0.0` |
| `st_set1day` | `0.0` |
| `st_set2` | `0.0` |
| `st_set2day` | `0.0` |
| `st_terr` | `` |
| `st_trbal` | `0.0` |
| `st_trdate` | `2024-05-01T00:00:00` |
| `st_trref` | `pay` |
| `st_trtype` | `R` |
| `st_trvalue` | `-23.99` |
| `st_txtrep` | `` |
| `st_type` | `` |
| `st_unique` | `_7FL0R6FLA` |
| `st_vatval` | `0.0` |

**1 row(s) modified:**

*Row id=9264.0:*
| Field | Before | After |
|-------|--------|-------|
| `st_lastrec` | `NaT` | `2024-05-17T00:00:00` |
| `st_paid` | `` | `P` |
| `st_payday` | `NaT` | `2024-05-01T00:00:00` |
| `st_payflag` | `0.0` | `90.0` |
| `st_trbal` | `23.99` | `0.0` |


#### zlock

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|


---

### Sales Refund

#### aentry

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `ae_acnt` | `C310` |
| `ae_batchid` | `0.0` |
| `ae_brwptr` | `` |
| `ae_cbtype` | `P6` |
| `ae_cntr` | `` |
| `ae_comment` | `` |
| `ae_complet` | `1.0` |
| `ae_entref` | `test` |
| `ae_entry` | `P600000039` |
| `ae_frstat` | `0.0` |
| `ae_lstdate` | `2024-05-01T00:00:00` |
| `ae_payid` | `0.0` |
| `ae_postgrp` | `0.0` |
| `ae_recbal` | `0.0` |
| `ae_reclnum` | `0.0` |
| `ae_remove` | `0.0` |
| `ae_statln` | `0.0` |
| `ae_tmpstat` | `0.0` |
| `ae_tostat` | `0.0` |
| `ae_value` | `-2399.0` |
| `sq_amtime` | `` |
| `sq_amuser` | `` |
| `sq_crdate` | `2026-04-01T00:00:00` |
| `sq_crtime` | `12:50:07` |
| `sq_cruser` | `TEST` |


#### anoml

**2 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `ax_comment` | `Anderson Car Factors Ltd      Refund` |
| `ax_date` | `2024-05-01T00:00:00` |
| `ax_done` | `` |
| `ax_fcdec` | `0.0` |
| `ax_fcmult` | `0.0` |
| `ax_fcrate` | `0.0` |
| `ax_fcurr` | `` |
| `ax_fvalue` | `0.0` |
| `ax_job` | `` |
| `ax_jrnl` | `0.0` |
| `ax_nacnt` | `C310` |
| `ax_ncntr` | `` |
| `ax_nlpdate` | `2024-05-01T00:00:00` |
| `ax_project` | `` |
| `ax_source` | `S` |
| `ax_srcco` | `Z` |
| `ax_tref` | `test` |
| `ax_unique` | `_7FL0RIDYH` |
| `ax_value` | `-23.99` |

*Row 2:*
| Field | Value |
|-------|-------|
| `ax_comment` | `Anderson Car Factors Ltd      Refund` |
| `ax_date` | `2024-05-01T00:00:00` |
| `ax_done` | `` |
| `ax_fcdec` | `0.0` |
| `ax_fcmult` | `0.0` |
| `ax_fcrate` | `0.0` |
| `ax_fcurr` | `` |
| `ax_fvalue` | `0.0` |
| `ax_job` | `` |
| `ax_jrnl` | `0.0` |
| `ax_nacnt` | `C110` |
| `ax_ncntr` | `` |
| `ax_nlpdate` | `2024-05-01T00:00:00` |
| `ax_project` | `` |
| `ax_source` | `S` |
| `ax_srcco` | `Z` |
| `ax_tref` | `test` |
| `ax_unique` | `_7FL0RIDYH` |
| `ax_value` | `23.99` |


#### atran

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `at_account` | `AND0001` |
| `at_acnt` | `C310` |
| `at_atpycd` | `` |
| `at_bacprn` | `0.0` |
| `at_bic` | `` |
| `at_bsname` | `` |
| `at_bsref` | `` |
| `at_cash` | `0.0` |
| `at_cbtype` | `P6` |
| `at_ccauth` | `` |
| `at_ccdno` | `` |
| `at_ccdprn` | `0.0` |
| `at_chqlst` | `0.0` |
| `at_chqprn` | `0.0` |
| `at_cntr` | `` |
| `at_comment` | `` |
| `at_disc` | `0.0` |
| `at_ecb` | `0.0` |
| `at_ecbtype` | `` |
| `at_entry` | `P600000039` |
| `at_fcdec` | `2.0` |
| `at_fcexch` | `1.0` |
| `at_fcmult` | `0.0` |
| `at_fcurr` | `` |
| `at_iban` | `` |
| `at_inputby` | `TEST` |
| `at_job` | `` |
| `at_memo` | `` |
| `at_name` | `Anderson Car Factors Ltd` |
| `at_number` | `` |
| `at_payee` | `` |
| `at_payname` | `` |
| `at_payslp` | `0.0` |
| `at_postgrp` | `0.0` |
| `at_project` | `` |
| `at_pstdate` | `2024-05-01T00:00:00` |
| `at_pysprn` | `0.0` |
| `at_refer` | `test` |
| `at_remit` | `0.0` |
| `at_remove` | `0.0` |
| `at_sort` | `` |
| `at_srcco` | `Z` |
| `at_sysdate` | `2024-05-01T00:00:00` |
| `at_tperiod` | `1.0` |
| `at_type` | `3.0` |
| `at_unique` | `_7FL0RIDYH` |
| `at_value` | `-2399.0` |
| `at_vattycd` | `` |


#### atype

**1 row(s) modified:**

*Row id=6.0:*
| Field | Before | After |
|-------|--------|-------|
| `ay_entry` | `P600000039` | `P600000040` |


#### nbank

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `nk_curbal` | `4814425.0` | `4812026.0` |


#### nextid

**4 row(s) modified:**

*Row id=10.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `11047.0` | `11048.0` |

*Row id=13.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `26098.0` | `26100.0` |

*Row id=21.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `17731.0` | `17732.0` |

*Row id=286.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `9282.0` | `9283.0` |


#### sname

**1 row(s) modified:**

*Row id=22.0:*
| Field | Before | After |
|-------|--------|-------|
| `sn_currbal` | `0.0` | `23.99` |


#### stran

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `jxrenewal` | `0.0` |
| `jxservid` | `0.0` |
| `st_account` | `AND0001` |
| `st_adjsv` | `0.0` |
| `st_advallc` | `0.0` |
| `st_advance` | `N` |
| `st_binrep` | `0.0` |
| `st_cash` | `0.0` |
| `st_cbtype` | `P6` |
| `st_crdate` | `2024-05-01T00:00:00` |
| `st_custref` | `Refund` |
| `st_delacc` | `` |
| `st_dispute` | `0.0` |
| `st_edi` | `0.0` |
| `st_editx` | `0.0` |
| `st_edivn` | `0.0` |
| `st_entry` | `P600000039` |
| `st_eurind` | `` |
| `st_euro` | `0.0` |
| `st_exttime` | `` |
| `st_fadval` | `0.0` |
| `st_fcbal` | `0.0` |
| `st_fcdec` | `0.0` |
| `st_fcmult` | `0.0` |
| `st_fcrate` | `0.0` |
| `st_fcurr` | `` |
| `st_fcval` | `0.0` |
| `st_fcvat` | `0.0` |
| `st_fullamt` | `0.0` |
| `st_fullcb` | `` |
| `st_fullnar` | `` |
| `st_gateid` | `0.0` |
| `st_gatetr` | `0.0` |
| `st_luptime` | `` |
| `st_memo` | `` |
| `st_nlpdate` | `2024-05-01T00:00:00` |
| `st_origcur` | `` |
| `st_paid` | `` |
| `st_payadvl` | `0.0` |
| `st_payflag` | `0.0` |
| `st_rcode` | `` |
| `st_region` | `` |
| `st_revchrg` | `0.0` |
| `st_ruser` | `` |
| `st_set1` | `0.0` |
| `st_set1day` | `0.0` |
| `st_set2` | `0.0` |
| `st_set2day` | `0.0` |
| `st_terr` | `` |
| `st_trbal` | `23.99` |
| `st_trdate` | `2024-05-01T00:00:00` |
| `st_trref` | `test` |
| `st_trtype` | `F` |
| `st_trvalue` | `23.99` |
| `st_txtrep` | `` |
| `st_type` | `` |
| `st_unique` | `_7FL0RIDYH` |
| `st_vatval` | `0.0` |


#### zlock

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|


---

### Transfer

#### aentry

**2 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `ae_acnt` | `C310` |
| `ae_batchid` | `0.0` |
| `ae_brwptr` | `` |
| `ae_cbtype` | `T1` |
| `ae_cntr` | `` |
| `ae_comment` | `` |
| `ae_complet` | `1.0` |
| `ae_entref` | `test` |
| `ae_entry` | `T100000104` |
| `ae_frstat` | `0.0` |
| `ae_lstdate` | `2024-05-01T00:00:00` |
| `ae_payid` | `0.0` |
| `ae_postgrp` | `0.0` |
| `ae_recbal` | `0.0` |
| `ae_reclnum` | `0.0` |
| `ae_remove` | `0.0` |
| `ae_statln` | `0.0` |
| `ae_tmpstat` | `0.0` |
| `ae_tostat` | `0.0` |
| `ae_value` | `-4000.0` |
| `sq_amtime` | `` |
| `sq_amuser` | `` |
| `sq_crdate` | `2024-05-01T00:00:00` |
| `sq_crtime` | `13:46:10` |
| `sq_cruser` | `TEST` |

*Row 2:*
| Field | Value |
|-------|-------|
| `ae_acnt` | `C315` |
| `ae_batchid` | `0.0` |
| `ae_brwptr` | `` |
| `ae_cbtype` | `T1` |
| `ae_cntr` | `` |
| `ae_comment` | `` |
| `ae_complet` | `1.0` |
| `ae_entref` | `test` |
| `ae_entry` | `T100000105` |
| `ae_frstat` | `0.0` |
| `ae_lstdate` | `2024-05-01T00:00:00` |
| `ae_payid` | `0.0` |
| `ae_postgrp` | `0.0` |
| `ae_recbal` | `0.0` |
| `ae_reclnum` | `0.0` |
| `ae_remove` | `0.0` |
| `ae_statln` | `0.0` |
| `ae_tmpstat` | `0.0` |
| `ae_tostat` | `0.0` |
| `ae_value` | `4000.0` |
| `sq_amtime` | `` |
| `sq_amuser` | `` |
| `sq_crdate` | `2024-05-01T00:00:00` |
| `sq_crtime` | `13:46:10` |
| `sq_cruser` | `TEST` |


#### anoml

**2 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `ax_comment` | `test` |
| `ax_date` | `2024-05-01T00:00:00` |
| `ax_done` | `` |
| `ax_fcdec` | `2.0` |
| `ax_fcmult` | `0.0` |
| `ax_fcrate` | `1.0` |
| `ax_fcurr` | `` |
| `ax_fvalue` | `4000.0` |
| `ax_job` | `` |
| `ax_jrnl` | `0.0` |
| `ax_nacnt` | `C315` |
| `ax_ncntr` | `` |
| `ax_nlpdate` | `2024-05-01T00:00:00` |
| `ax_project` | `` |
| `ax_source` | `A` |
| `ax_srcco` | `Z` |
| `ax_tref` | `test` |
| `ax_unique` | `_7FL0TIH1C` |
| `ax_value` | `40.0` |

*Row 2:*
| Field | Value |
|-------|-------|
| `ax_comment` | `test` |
| `ax_date` | `2024-05-01T00:00:00` |
| `ax_done` | `` |
| `ax_fcdec` | `2.0` |
| `ax_fcmult` | `0.0` |
| `ax_fcrate` | `1.0` |
| `ax_fcurr` | `` |
| `ax_fvalue` | `-4000.0` |
| `ax_job` | `` |
| `ax_jrnl` | `0.0` |
| `ax_nacnt` | `C310` |
| `ax_ncntr` | `` |
| `ax_nlpdate` | `2024-05-01T00:00:00` |
| `ax_project` | `` |
| `ax_source` | `A` |
| `ax_srcco` | `Z` |
| `ax_tref` | `test` |
| `ax_unique` | `_7FL0TIH1C` |
| `ax_value` | `-40.0` |


#### atran

**2 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `at_account` | `C315` |
| `at_acnt` | `C310` |
| `at_atpycd` | `` |
| `at_bacprn` | `0.0` |
| `at_bic` | `` |
| `at_bsname` | `` |
| `at_bsref` | `` |
| `at_cash` | `0.0` |
| `at_cbtype` | `T1` |
| `at_ccauth` | `` |
| `at_ccdno` | `` |
| `at_ccdprn` | `0.0` |
| `at_chqlst` | `0.0` |
| `at_chqprn` | `0.0` |
| `at_cntr` | `` |
| `at_comment` | `test` |
| `at_disc` | `0.0` |
| `at_ecb` | `0.0` |
| `at_ecbpayd` | `2024-05-01T00:00:00` |
| `at_ecbtype` | `` |
| `at_entry` | `T100000104` |
| `at_fcdec` | `2.0` |
| `at_fcexch` | `1.0` |
| `at_fcmult` | `0.0` |
| `at_fcurr` | `` |
| `at_iban` | `` |
| `at_inputby` | `TEST` |
| `at_job` | `` |
| `at_memo` | `` |
| `at_name` | `Second Bank Current Account` |
| `at_number` | `32873945` |
| `at_payee` | `` |
| `at_payname` | `` |
| `at_payslp` | `0.0` |
| `at_postgrp` | `0.0` |
| `at_project` | `` |
| `at_pstdate` | `2024-05-01T00:00:00` |
| `at_pysprn` | `0.0` |
| `at_refer` | `test` |
| `at_remit` | `0.0` |
| `at_remove` | `0.0` |
| `at_sort` | `33-44-99` |
| `at_srcco` | `Z` |
| `at_sysdate` | `2024-05-01T00:00:00` |
| `at_tperiod` | `1.0` |
| `at_type` | `8.0` |
| `at_unique` | `_7FL0TIH1C` |
| `at_value` | `-4000.0` |
| `at_vattycd` | `` |

*Row 2:*
| Field | Value |
|-------|-------|
| `at_account` | `C310` |
| `at_acnt` | `C315` |
| `at_atpycd` | `` |
| `at_bacprn` | `0.0` |
| `at_bic` | `` |
| `at_bsname` | `` |
| `at_bsref` | `` |
| `at_cash` | `0.0` |
| `at_cbtype` | `T1` |
| `at_ccauth` | `` |
| `at_ccdno` | `` |
| `at_ccdprn` | `0.0` |
| `at_chqlst` | `0.0` |
| `at_chqprn` | `0.0` |
| `at_cntr` | `` |
| `at_comment` | `test` |
| `at_disc` | `0.0` |
| `at_ecb` | `0.0` |
| `at_ecbpayd` | `2024-05-01T00:00:00` |
| `at_ecbtype` | `` |
| `at_entry` | `T100000105` |
| `at_fcdec` | `2.0` |
| `at_fcexch` | `1.0` |
| `at_fcmult` | `0.0` |
| `at_fcurr` | `` |
| `at_iban` | `` |
| `at_inputby` | `TEST` |
| `at_job` | `` |
| `at_memo` | `` |
| `at_name` | `Main Bank Current Account` |
| `at_number` | `` |
| `at_payee` | `` |
| `at_payname` | `` |
| `at_payslp` | `0.0` |
| `at_postgrp` | `0.0` |
| `at_project` | `` |
| `at_pstdate` | `2024-05-01T00:00:00` |
| `at_pysprn` | `0.0` |
| `at_refer` | `test` |
| `at_remit` | `0.0` |
| `at_remove` | `0.0` |
| `at_sort` | `` |
| `at_srcco` | `Z` |
| `at_sysdate` | `2024-05-01T00:00:00` |
| `at_tperiod` | `1.0` |
| `at_type` | `8.0` |
| `at_unique` | `_7FL0TIH1C` |
| `at_value` | `4000.0` |
| `at_vattycd` | `` |


#### atype

**1 row(s) modified:**

*Row id=14.0:*
| Field | Before | After |
|-------|--------|-------|
| `ay_entry` | `T100000104` | `T100000106` |


#### nbank

**2 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `nk_curbal` | `4802026.0` | `4798026.0` |

*Row id=3.0:*
| Field | Before | After |
|-------|--------|-------|
| `nk_curbal` | `99924475.0` | `99928475.0` |


#### nextid

**3 row(s) modified:**

*Row id=10.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `11056.0` | `11058.0` |

*Row id=13.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `26116.0` | `26118.0` |

*Row id=21.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `17741.0` | `17743.0` |


#### zlock

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|


---

### create foreign currency bank account

#### abatch

**3 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `ab_account` | `FC001` |
| `ab_centre` | `` |
| `ab_complet` | `0.0` |
| `ab_entry` | `P200000429` |
| `ab_type` | `P2` |

*Row 2:*
| Field | Value |
|-------|-------|
| `ab_account` | `FC001` |
| `ab_centre` | `` |
| `ab_complet` | `0.0` |
| `ab_entry` | `P500000731` |
| `ab_type` | `P5` |

*Row 3:*
| Field | Value |
|-------|-------|
| `ab_account` | `FC001` |
| `ab_centre` | `` |
| `ab_complet` | `0.0` |
| `ab_entry` | `R100000232` |
| `ab_type` | `R1` |


#### nacnt

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `na_acnt` | `FC001` |
| `na_allwjob` | `0.0` |
| `na_allwprj` | `0.0` |
| `na_balc01` | `0.0` |
| `na_balc02` | `0.0` |
| `na_balc03` | `0.0` |
| `na_balc04` | `0.0` |
| `na_balc05` | `0.0` |
| `na_balc06` | `0.0` |
| `na_balc07` | `0.0` |
| `na_balc08` | `0.0` |
| `na_balc09` | `0.0` |
| `na_balc10` | `0.0` |
| `na_balc11` | `0.0` |
| `na_balc12` | `0.0` |
| `na_balc13` | `0.0` |
| `na_balc14` | `0.0` |
| `na_balc15` | `0.0` |
| `na_balc16` | `0.0` |
| `na_balc17` | `0.0` |
| `na_balc18` | `0.0` |
| `na_balc19` | `0.0` |
| `na_balc20` | `0.0` |
| `na_balc21` | `0.0` |
| `na_balc22` | `0.0` |
| `na_balc23` | `0.0` |
| `na_balc24` | `0.0` |
| `na_balp01` | `0.0` |
| `na_balp02` | `0.0` |
| `na_balp03` | `0.0` |
| `na_balp04` | `0.0` |
| `na_balp05` | `0.0` |
| `na_balp06` | `0.0` |
| `na_balp07` | `0.0` |
| `na_balp08` | `0.0` |
| `na_balp09` | `0.0` |
| `na_balp10` | `0.0` |
| `na_balp11` | `0.0` |
| `na_balp12` | `0.0` |
| `na_balp13` | `0.0` |
| `na_balp14` | `0.0` |
| `na_balp15` | `0.0` |
| `na_balp16` | `0.0` |
| `na_balp17` | `0.0` |
| `na_balp18` | `0.0` |
| `na_balp19` | `0.0` |
| `na_balp20` | `0.0` |
| `na_balp21` | `0.0` |
| `na_balp22` | `0.0` |
| `na_balp23` | `0.0` |
| `na_balp24` | `0.0` |
| `na_cntr` | `` |
| `na_comm` | `0.0` |
| `na_desc` | `FC Bank` |
| `na_extcode` | `` |
| `na_fbalc01` | `0.0` |
| `na_fbalc02` | `0.0` |
| `na_fbalc03` | `0.0` |
| `na_fbalc04` | `0.0` |
| `na_fbalc05` | `0.0` |
| `na_fbalc06` | `0.0` |
| `na_fbalc07` | `0.0` |
| `na_fbalc08` | `0.0` |
| `na_fbalc09` | `0.0` |
| `na_fbalc10` | `0.0` |
| `na_fbalc11` | `0.0` |
| `na_fbalc12` | `0.0` |
| `na_fbalc13` | `0.0` |
| `na_fbalc14` | `0.0` |
| `na_fbalc15` | `0.0` |
| `na_fbalc16` | `0.0` |
| `na_fbalc17` | `0.0` |
| `na_fbalc18` | `0.0` |
| `na_fbalc19` | `0.0` |
| `na_fbalc20` | `0.0` |
| `na_fbalc21` | `0.0` |
| `na_fbalc22` | `0.0` |
| `na_fbalc23` | `0.0` |
| `na_fbalc24` | `0.0` |
| `na_fbalp01` | `0.0` |
| `na_fbalp02` | `0.0` |
| `na_fbalp03` | `0.0` |
| `na_fbalp04` | `0.0` |
| `na_fbalp05` | `0.0` |
| `na_fbalp06` | `0.0` |
| `na_fbalp07` | `0.0` |
| `na_fbalp08` | `0.0` |
| `na_fbalp09` | `0.0` |
| `na_fbalp10` | `0.0` |
| `na_fbalp11` | `0.0` |
| `na_fbalp12` | `0.0` |
| `na_fbalp13` | `0.0` |
| `na_fbalp14` | `0.0` |
| `na_fbalp15` | `0.0` |
| `na_fbalp16` | `0.0` |
| `na_fbalp17` | `0.0` |
| `na_fbalp18` | `0.0` |
| `na_fbalp19` | `0.0` |
| `na_fbalp20` | `0.0` |
| `na_fbalp21` | `0.0` |
| `na_fbalp22` | `0.0` |
| `na_fbalp23` | `0.0` |
| `na_fbalp24` | `0.0` |
| `na_fcdec` | `2.0` |
| `na_fcmult` | `0.0` |
| `na_fcrate` | `1.190476` |
| `na_fcurr` | `EUR` |
| `na_fprycr` | `0.0` |
| `na_fprydr` | `0.0` |
| `na_fptdcr` | `0.0` |
| `na_fptddr` | `0.0` |
| `na_fytdcr` | `0.0` |
| `na_fytddr` | `0.0` |
| `na_job` | `` |
| `na_key1` | `` |
| `na_key2` | `` |
| `na_key3` | `` |
| `na_key4` | `` |
| `na_memo` | `` |
| `na_open` | `0.0` |
| `na_post` | `0.0` |
| `na_project` | `` |
| `na_prycr` | `0.0` |
| `na_prydr` | `0.0` |
| `na_ptdcr` | `0.0` |
| `na_ptddr` | `0.0` |
| `na_redist` | `0.0` |
| `na_repkey1` | `` |
| `na_repkey2` | `` |
| `na_repkey3` | `` |
| `na_repkey4` | `` |
| `na_repkey5` | `` |
| `na_subt` | `` |
| `na_type` | `` |
| `na_ytdcr` | `0.0` |
| `na_ytddr` | `0.0` |
| `sq_private` | `0.0` |


#### nbank

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `nk_acnt` | `FC001` |
| `nk_addr1` | `bank` |
| `nk_addr2` | `` |
| `nk_addr3` | `` |
| `nk_addr4` | `` |
| `nk_bic` | `` |
| `nk_bkname` | `euro bank` |
| `nk_chqrep` | `` |
| `nk_cntr` | `` |
| `nk_contact` | `` |
| `nk_curbal` | `0.0` |
| `nk_desc` | `FC Bank` |
| `nk_dwnlck` | `` |
| `nk_ecb` | `` |
| `nk_ecbdwn` | `0.0` |
| `nk_ecbpay` | `0.0` |
| `nk_ecbrec` | `0.0` |
| `nk_email` | `` |
| `nk_faxno` | `` |
| `nk_fcdec` | `2.0` |
| `nk_fcurr` | `EUR` |
| `nk_iban` | `` |
| `nk_key1` | `FC` |
| `nk_key2` | `BANK` |
| `nk_key3` | `` |
| `nk_key4` | `` |
| `nk_lstchq` | `1.0` |
| `nk_lstpslp` | `1.0` |
| `nk_lstrecl` | `1.0` |
| `nk_lststno` | `1.0` |
| `nk_matlock` | `` |
| `nk_notice` | `0.0` |
| `nk_number` | `99999999` |
| `nk_ovrdrft` | `0.0` |
| `nk_petty` | `0.0` |
| `nk_private` | `0.0` |
| `nk_pstcode` | `` |
| `nk_recbal` | `0.0` |
| `nk_reccfwd` | `0.0` |
| `nk_reclnum` | `0.0` |
| `nk_reclock` | `` |
| `nk_recstfr` | `0.0` |
| `nk_recstln` | `0.0` |
| `nk_recstto` | `0.0` |
| `nk_sepa` | `0.0` |
| `nk_sepctry` | `` |
| `nk_sort` | `99-99-99` |
| `nk_teleno` | `` |
| `nk_title` | `` |
| `nk_wwwpage` | `` |
| `sq_memo` | `` |


#### nextid

**3 row(s) modified:**

*Row id=2.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `32.0` | `35.0` |

*Row id=223.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `334.0` | `335.0` |

*Row id=224.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `8.0` | `9.0` |


---

### foreign currency payment

#### aentry

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `ae_acnt` | `FC001` |
| `ae_batchid` | `0.0` |
| `ae_brwptr` | `` |
| `ae_cbtype` | `P1` |
| `ae_cntr` | `` |
| `ae_comment` | `test` |
| `ae_complet` | `1.0` |
| `ae_entref` | `test` |
| `ae_entry` | `P100000770` |
| `ae_frstat` | `0.0` |
| `ae_lstdate` | `2024-05-20T00:00:00` |
| `ae_payid` | `0.0` |
| `ae_postgrp` | `0.0` |
| `ae_recbal` | `0.0` |
| `ae_reclnum` | `0.0` |
| `ae_remove` | `0.0` |
| `ae_statln` | `0.0` |
| `ae_tmpstat` | `0.0` |
| `ae_tostat` | `0.0` |
| `ae_value` | `-5000.0` |
| `sq_amtime` | `` |
| `sq_amuser` | `` |
| `sq_crdate` | `2026-03-31T00:00:00` |
| `sq_crtime` | `21:14:06` |
| `sq_cruser` | `TEST` |


#### anoml

**2 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `ax_comment` | `test` |
| `ax_date` | `2024-05-20T00:00:00` |
| `ax_done` | `Y` |
| `ax_fcdec` | `2.0` |
| `ax_fcmult` | `0.0` |
| `ax_fcrate` | `1.190476` |
| `ax_fcurr` | `EUR` |
| `ax_fvalue` | `5000.0` |
| `ax_job` | `` |
| `ax_jrnl` | `3454.0` |
| `ax_nacnt` | `C110` |
| `ax_ncntr` | `` |
| `ax_nlpdate` | `2024-05-20T00:00:00` |
| `ax_project` | `` |
| `ax_source` | `A` |
| `ax_srcco` | `Z` |
| `ax_tref` | `test` |
| `ax_unique` | `_7FQ19II9J` |
| `ax_value` | `42.0` |

*Row 2:*
| Field | Value |
|-------|-------|
| `ax_comment` | `test` |
| `ax_date` | `2024-05-20T00:00:00` |
| `ax_done` | `Y` |
| `ax_fcdec` | `2.0` |
| `ax_fcmult` | `0.0` |
| `ax_fcrate` | `1.190476` |
| `ax_fcurr` | `EUR` |
| `ax_fvalue` | `-5000.0` |
| `ax_job` | `` |
| `ax_jrnl` | `3454.0` |
| `ax_nacnt` | `FC001` |
| `ax_ncntr` | `` |
| `ax_nlpdate` | `2024-05-20T00:00:00` |
| `ax_project` | `` |
| `ax_source` | `A` |
| `ax_srcco` | `Z` |
| `ax_tref` | `test` |
| `ax_unique` | `_7FQ19II9J` |
| `ax_value` | `-42.0` |


#### atran

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `at_account` | `C110` |
| `at_acnt` | `FC001` |
| `at_atpycd` | `` |
| `at_bacprn` | `0.0` |
| `at_bic` | `` |
| `at_bsname` | `` |
| `at_bsref` | `` |
| `at_cash` | `0.0` |
| `at_cbtype` | `P1` |
| `at_ccauth` | `` |
| `at_ccdno` | `` |
| `at_ccdprn` | `0.0` |
| `at_chqlst` | `0.0` |
| `at_chqprn` | `0.0` |
| `at_cntr` | `` |
| `at_comment` | `test` |
| `at_disc` | `0.0` |
| `at_ecb` | `0.0` |
| `at_ecbpayd` | `2024-05-20T00:00:00` |
| `at_ecbtype` | `` |
| `at_entry` | `P100000770` |
| `at_fcdec` | `2.0` |
| `at_fcexch` | `1.190476` |
| `at_fcmult` | `0.0` |
| `at_fcurr` | `EUR` |
| `at_iban` | `` |
| `at_inputby` | `TEST` |
| `at_job` | `` |
| `at_memo` | `` |
| `at_name` | `Trade Debtors` |
| `at_number` | `` |
| `at_payee` | `` |
| `at_payname` | `` |
| `at_payslp` | `0.0` |
| `at_postgrp` | `0.0` |
| `at_project` | `` |
| `at_pstdate` | `2024-05-20T00:00:00` |
| `at_pysprn` | `0.0` |
| `at_refer` | `test` |
| `at_remit` | `0.0` |
| `at_remove` | `0.0` |
| `at_sort` | `` |
| `at_srcco` | `Z` |
| `at_sysdate` | `2026-03-31T00:00:00` |
| `at_tperiod` | `1.0` |
| `at_type` | `1.0` |
| `at_unique` | `_7FQ19II9J` |
| `at_value` | `-5000.0` |
| `at_vattycd` | `` |


#### atype

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `ay_entry` | `P100000770` | `P100000771` |


#### idtab

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `id_numericid` | `3454.0` | `3455.0` |


#### nacnt

**2 row(s) modified:**

*Row id=12.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `20321.73` | `20363.73` |
| `na_ptddr` | `23239.71` | `23281.71` |
| `na_ytddr` | `1995838.49` | `1995880.49` |

*Row id=64.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `0.0` | `-42.0` |
| `na_fbalc05` | `0.0` | `-5000.0` |
| `na_fptdcr` | `0.0` | `5000.0` |
| `na_fytdcr` | `0.0` | `5000.0` |
| `na_ptdcr` | `0.0` | `42.0` |
| `na_ytdcr` | `0.0` | `42.0` |


#### nbank

**1 row(s) modified:**

*Row id=8.0:*
| Field | Before | After |
|-------|--------|-------|
| `nk_curbal` | `10000.0` | `5000.0` |


#### nextid

**6 row(s) modified:**

*Row id=10.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `11062.0` | `11063.0` |

*Row id=13.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `26132.0` | `26134.0` |

*Row id=21.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `17751.0` | `17752.0` |

*Row id=234.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `159925.0` | `159926.0` |

*Row id=235.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `218.0` | `219.0` |


#### nhist

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `nh_bal` | `-42.0` |
| `nh_budg` | `0.0` |
| `nh_fbal` | `0.0` |
| `nh_job` | `` |
| `nh_nacnt` | `K999` |
| `nh_ncntr` | `` |
| `nh_nsubt` | `01` |
| `nh_ntype` | `30` |
| `nh_period` | `5.0` |
| `nh_project` | `` |
| `nh_ptdcr` | `-42.0` |
| `nh_ptddr` | `0.0` |
| `nh_rbudg` | `0.0` |
| `nh_rectype` | `1.0` |
| `nh_year` | `2024.0` |

**1 row(s) modified:**

*Row id=159881.0:*
| Field | Before | After |
|-------|--------|-------|
| `nh_bal` | `20321.73` | `20363.73` |
| `nh_ptddr` | `23239.71` | `23281.71` |


#### njmemo

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `nj_binrep` | `0.0` |
| `nj_image` | `` |
| `nj_journal` | `3454.0` |
| `nj_memo` | `ÿ<<JOURNAL_DATA_ONLY>>ÿ` |
| `nj_txtrep` | `Cashbook Ledger Transfer` |


#### nsubt

**2 row(s) modified:**

*Row id=6.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `432213.47` | `432255.47` |

*Row id=15.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `-12870.02` | `-12912.02` |


#### ntype

**2 row(s) modified:**

*Row id=3.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `5115026.67` | `5115068.67` |

*Row id=7.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `-1462243.81` | `-1462285.81` |


#### zlock

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|


---

### foreign currency receipt

#### aentry

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `ae_acnt` | `FC001` |
| `ae_batchid` | `0.0` |
| `ae_brwptr` | `` |
| `ae_cbtype` | `R1` |
| `ae_cntr` | `` |
| `ae_comment` | `test` |
| `ae_complet` | `0.0` |
| `ae_entref` | `test` |
| `ae_entry` | `R100000233` |
| `ae_frstat` | `0.0` |
| `ae_lstdate` | `2024-05-01T00:00:00` |
| `ae_payid` | `0.0` |
| `ae_postgrp` | `0.0` |
| `ae_recbal` | `0.0` |
| `ae_reclnum` | `0.0` |
| `ae_remove` | `0.0` |
| `ae_statln` | `0.0` |
| `ae_tmpstat` | `0.0` |
| `ae_tostat` | `0.0` |
| `ae_value` | `10000.0` |
| `sq_amtime` | `` |
| `sq_amuser` | `` |
| `sq_crdate` | `2026-03-31T00:00:00` |
| `sq_crtime` | `21:06:48` |
| `sq_cruser` | `TEST` |


#### anoml

**2 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `ax_comment` | `test` |
| `ax_date` | `2024-05-01T00:00:00` |
| `ax_done` | `Y` |
| `ax_fcdec` | `2.0` |
| `ax_fcmult` | `0.0` |
| `ax_fcrate` | `1.190476` |
| `ax_fcurr` | `EUR` |
| `ax_fvalue` | `10000.0` |
| `ax_job` | `` |
| `ax_jrnl` | `3453.0` |
| `ax_nacnt` | `FC001` |
| `ax_ncntr` | `` |
| `ax_nlpdate` | `2024-05-01T00:00:00` |
| `ax_project` | `` |
| `ax_source` | `A` |
| `ax_srcco` | `Z` |
| `ax_tref` | `test` |
| `ax_unique` | `_7FQ1994R2` |
| `ax_value` | `84.0` |

*Row 2:*
| Field | Value |
|-------|-------|
| `ax_comment` | `test` |
| `ax_date` | `2024-05-01T00:00:00` |
| `ax_done` | `Y` |
| `ax_fcdec` | `2.0` |
| `ax_fcmult` | `0.0` |
| `ax_fcrate` | `1.190476` |
| `ax_fcurr` | `EUR` |
| `ax_fvalue` | `-10000.0` |
| `ax_job` | `SM` |
| `ax_jrnl` | `3453.0` |
| `ax_nacnt` | `S120` |
| `ax_ncntr` | `ADM` |
| `ax_nlpdate` | `2024-05-01T00:00:00` |
| `ax_project` | `DVD1` |
| `ax_source` | `A` |
| `ax_srcco` | `Z` |
| `ax_tref` | `test` |
| `ax_unique` | `_7FQ1994R2` |
| `ax_value` | `-84.0` |


#### atran

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `at_account` | `S120    ADM` |
| `at_acnt` | `FC001` |
| `at_atpycd` | `` |
| `at_bacprn` | `0.0` |
| `at_bic` | `` |
| `at_bsname` | `` |
| `at_bsref` | `` |
| `at_cash` | `0.0` |
| `at_cbtype` | `R1` |
| `at_ccauth` | `` |
| `at_ccdno` | `` |
| `at_ccdprn` | `0.0` |
| `at_chqlst` | `0.0` |
| `at_chqprn` | `0.0` |
| `at_cntr` | `` |
| `at_comment` | `test` |
| `at_disc` | `0.0` |
| `at_ecb` | `0.0` |
| `at_ecbpayd` | `2024-05-01T00:00:00` |
| `at_ecbtype` | `` |
| `at_entry` | `R100000233` |
| `at_fcdec` | `2.0` |
| `at_fcexch` | `1.190476` |
| `at_fcmult` | `0.0` |
| `at_fcurr` | `EUR` |
| `at_iban` | `` |
| `at_inputby` | `TEST` |
| `at_job` | `SM` |
| `at_memo` | `` |
| `at_name` | `Travel Expenses / Subsistence` |
| `at_number` | `` |
| `at_payee` | `` |
| `at_payname` | `` |
| `at_payslp` | `0.0` |
| `at_postgrp` | `0.0` |
| `at_project` | `DVD1` |
| `at_pstdate` | `2024-05-01T00:00:00` |
| `at_pysprn` | `1.0` |
| `at_refer` | `test` |
| `at_remit` | `0.0` |
| `at_remove` | `0.0` |
| `at_sort` | `` |
| `at_srcco` | `Z` |
| `at_sysdate` | `2026-03-31T00:00:00` |
| `at_tperiod` | `1.0` |
| `at_type` | `2.0` |
| `at_unique` | `_7FQ1994R2` |
| `at_value` | `10000.0` |
| `at_vattycd` | `` |


#### atype

**1 row(s) modified:**

*Row id=9.0:*
| Field | Before | After |
|-------|--------|-------|
| `ay_entry` | `R100000232` | `R100000233` |


#### idtab

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `id_numericid` | `3453.0` | `3454.0` |


#### nacnt

**2 row(s) modified:**

*Row id=72.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `0.0` | `84.0` |
| `na_fbalc05` | `0.0` | `10000.0` |
| `na_fptddr` | `0.0` | `10000.0` |
| `na_fytddr` | `0.0` | `10000.0` |
| `na_ptddr` | `0.0` | `84.0` |
| `na_ytddr` | `0.0` | `84.0` |

*Row id=137.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `194.99` | `110.99` |
| `na_ptdcr` | `55.0` | `139.0` |
| `na_ytdcr` | `55.0` | `139.0` |


#### nbank

**1 row(s) modified:**

*Row id=8.0:*
| Field | Before | After |
|-------|--------|-------|
| `nk_curbal` | `0.0` | `10000.0` |


#### nextid

**6 row(s) modified:**

*Row id=10.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `11061.0` | `11062.0` |

*Row id=13.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `26130.0` | `26132.0` |

*Row id=21.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `17750.0` | `17751.0` |

*Row id=234.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `159923.0` | `159925.0` |

*Row id=235.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `217.0` | `218.0` |


#### nhist

**2 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `nh_bal` | `84.0` |
| `nh_budg` | `0.0` |
| `nh_fbal` | `0.0` |
| `nh_job` | `` |
| `nh_nacnt` | `M999` |
| `nh_ncntr` | `` |
| `nh_nsubt` | `01` |
| `nh_ntype` | `35` |
| `nh_period` | `5.0` |
| `nh_project` | `` |
| `nh_ptdcr` | `0.0` |
| `nh_ptddr` | `84.0` |
| `nh_rbudg` | `0.0` |
| `nh_rectype` | `1.0` |
| `nh_year` | `2024.0` |

*Row 2:*
| Field | Value |
|-------|-------|
| `nh_bal` | `-84.0` |
| `nh_budg` | `0.0` |
| `nh_fbal` | `0.0` |
| `nh_job` | `SM` |
| `nh_nacnt` | `S120` |
| `nh_ncntr` | `ADM` |
| `nh_nsubt` | `03` |
| `nh_ntype` | `45` |
| `nh_period` | `5.0` |
| `nh_project` | `DVD1` |
| `nh_ptdcr` | `-84.0` |
| `nh_ptddr` | `0.0` |
| `nh_rbudg` | `0.0` |
| `nh_rectype` | `1.0` |
| `nh_year` | `2024.0` |


#### njmemo

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `nj_binrep` | `0.0` |
| `nj_image` | `` |
| `nj_journal` | `3453.0` |
| `nj_memo` | `ÿ<<JOURNAL_DATA_ONLY>>ÿ` |
| `nj_txtrep` | `Cashbook Ledger Transfer` |


#### nsubt

**2 row(s) modified:**

*Row id=18.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `1294135.35` | `1294219.35` |

*Row id=23.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `194.99` | `110.99` |


#### ntype

**2 row(s) modified:**

*Row id=9.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `204770.26` | `204854.26` |

*Row id=10.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `374388.46` | `374304.46` |


#### zlock

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|


---

## Customer Master (sname)

### New Customer

#### nextid

**2 row(s) modified:**

*Row id=148.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `166.0` | `167.0` |

*Row id=203.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `4870.0` | `4872.0` |


#### sname

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `sn_account` | `A1224` |
| `sn_acknow` | `0.0` |
| `sn_addr1` | `Test` |
| `sn_addr2` | `` |
| `sn_addr3` | `` |
| `sn_addr4` | `` |
| `sn_adjsvcd` | `` |
| `sn_analsys` | `ANAL` |
| `sn_atpycd` | `` |
| `sn_bana` | `` |
| `sn_bankac` | `` |
| `sn_banksor` | `` |
| `sn_bic` | `` |
| `sn_branch` | `0.0` |
| `sn_cmgroup` | `` |
| `sn_contac2` | `order cont` |
| `sn_contact` | `ac contact` |
| `sn_cprfl` | `NOSTAT` |
| `sn_crdcrno` | `` |
| `sn_crdnotes` | `` |
| `sn_crdrate` | `0.0` |
| `sn_crdscor` | `` |
| `sn_crlim` | `0.0` |
| `sn_ctry` | `GB` |
| `sn_currbal` | `0.0` |
| `sn_custloc` | `` |
| `sn_custype` | `S` |
| `sn_delinst` | `` |
| `sn_delt` | `` |
| `sn_desp` | `` |
| `sn_dl_flag` | `0.0` |
| `sn_dl_pubid` | `0.0` |
| `sn_dltmail` | `0.0` |
| `sn_docmail` | `0.0` |
| `sn_dormant` | `0.0` |
| `sn_dwar` | `MAIN` |
| `sn_email` | `email` |
| `sn_emailoa` | `0.0` |
| `sn_emailst` | `0.0` |
| `sn_eori` | `` |
| `sn_epasswd` | `` |
| `sn_estore` | `` |
| `sn_extra1` | `` |
| `sn_extra2` | `` |
| `sn_faxno` | `` |
| `sn_fcreate` | `2026-04-06T00:00:00` |
| `sn_frnvat` | `0.0` |
| `sn_iban` | `` |
| `sn_invceac` | `` |
| `sn_job` | `A001` |
| `sn_key1` | `TEST` |
| `sn_key2` | `` |
| `sn_key3` | `` |
| `sn_key4` | `` |
| `sn_luptime` | `` |
| `sn_memo` | `` |
| `sn_model` | `0.0` |
| `sn_mtrn` | `` |
| `sn_name` | `Test` |
| `sn_nextpay` | `0.0` |
| `sn_nrthire` | `0.0` |
| `sn_ntrn` | `` |
| `sn_ordmail` | `` |
| `sn_ordrbal` | `0.0` |
| `sn_ovravmt` | `0.0` |
| `sn_priorty` | `1.0` |
| `sn_project` | `S020` |
| `sn_pstcode` | `SW19 8SE` |
| `sn_rana` | `` |
| `sn_region` | `A` |
| `sn_route` | `` |
| `sn_sana` | `` |
| `sn_sepayee` | `` |
| `sn_sepctry` | `` |
| `sn_seppstc` | `` |
| `sn_sepstnm` | `` |
| `sn_septown` | `` |
| `sn_sgrp` | `` |
| `sn_stmntac` | `` |
| `sn_stop` | `0.0` |
| `sn_teleno` | `` |
| `sn_terrtry` | `001` |
| `sn_tprfl` | `STD` |
| `sn_trnover` | `0.0` |
| `sn_vendor` | `` |
| `sn_vrn` | `` |
| `sn_wwwpage` | `` |


#### zcontacts

**2 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `sq_amtime` | `` |
| `sq_amuser` | `` |
| `sq_date` | `2026-04-06T00:00:00` |
| `sq_time` | `20:46:01` |
| `sq_user` | `TEST` |
| `zc_account` | `A1224` |
| `zc_allwfax` | `1.0` |
| `zc_allwmal` | `1.0` |
| `zc_allwmob` | `1.0` |
| `zc_allwph` | `1.0` |
| `zc_attr1` | `` |
| `zc_attr2` | `` |
| `zc_attr3` | `` |
| `zc_attr4` | `` |
| `zc_attr5` | `` |
| `zc_attr6` | `` |
| `zc_contact` | `ac contact` |
| `zc_email` | `email` |
| `zc_fax` | `` |
| `zc_fornam` | `` |
| `zc_mobile` | `` |
| `zc_module` | `S` |
| `zc_optcont` | `1.0` |
| `zc_phone` | `` |
| `zc_pos` | `` |
| `zc_surname` | `` |
| `zc_title` | `` |

*Row 2:*
| Field | Value |
|-------|-------|
| `sq_amtime` | `` |
| `sq_amuser` | `` |
| `sq_date` | `2026-04-06T00:00:00` |
| `sq_time` | `20:46:01` |
| `sq_user` | `TEST` |
| `zc_account` | `A1224` |
| `zc_allwfax` | `0.0` |
| `zc_allwmal` | `0.0` |
| `zc_allwmob` | `0.0` |
| `zc_allwph` | `0.0` |
| `zc_attr1` | `` |
| `zc_attr2` | `` |
| `zc_attr3` | `` |
| `zc_attr4` | `` |
| `zc_attr5` | `` |
| `zc_attr6` | `` |
| `zc_contact` | `order cont` |
| `zc_email` | `` |
| `zc_fax` | `` |
| `zc_fornam` | `` |
| `zc_mobile` | `` |
| `zc_module` | `S` |
| `zc_optcont` | `2.0` |
| `zc_phone` | `` |
| `zc_pos` | `` |
| `zc_surname` | `` |
| `zc_title` | `` |


---

## Nominal Ledger Journals

### Nominal Journal

#### idtab

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `id_numericid` | `3448.0` | `3449.0` |


#### nacnt

**3 row(s) modified:**

*Row id=137.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `28.33` | `111.66` |
| `na_ptddr` | `83.33` | `166.66` |
| `na_ytddr` | `83.33` | `166.66` |

*Row id=185.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `0.0` | `-100.0` |
| `na_ptdcr` | `0.0` | `100.0` |
| `na_ytdcr` | `0.0` | `100.0` |

*Row id=252.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `74973.36` | `74990.03` |
| `na_ptddr` | `74973.36` | `74990.03` |
| `na_ytddr` | `750761.05` | `750777.72` |


#### ndetl

**2 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `nd_account` | `S120     ADM` |
| `nd_comm` | `test` |
| `nd_cramnt` | `0.0` |
| `nd_dramnt` | `100.0` |
| `nd_fcdec` | `0.0` |
| `nd_fcmult` | `0.0` |
| `nd_fcrate` | `0.0` |
| `nd_fcurr` | `` |
| `nd_fvalue` | `0.0` |
| `nd_job` | `` |
| `nd_project` | `` |
| `nd_recno` | `1.0` |
| `nd_ref` | `JNL0000058` |
| `nd_taxdate` | `2024-05-01T00:00:00` |
| `nd_tfcdec` | `0.0` |
| `nd_tfcmult` | `0.0` |
| `nd_tfcrate` | `0.0` |
| `nd_tfcurr` | `` |
| `nd_tfvalue` | `0.0` |
| `nd_vatcde` | `1` |
| `nd_vattyp` | `P` |
| `nd_vatval` | `16.67` |

*Row 2:*
| Field | Value |
|-------|-------|
| `nd_account` | `E325     TEC` |
| `nd_comm` | `test` |
| `nd_cramnt` | `100.0` |
| `nd_dramnt` | `0.0` |
| `nd_fcdec` | `0.0` |
| `nd_fcmult` | `0.0` |
| `nd_fcrate` | `0.0` |
| `nd_fcurr` | `` |
| `nd_fvalue` | `0.0` |
| `nd_job` | `` |
| `nd_project` | `` |
| `nd_recno` | `2.0` |
| `nd_ref` | `JNL0000058` |
| `nd_tfcdec` | `0.0` |
| `nd_tfcmult` | `0.0` |
| `nd_tfcrate` | `0.0` |
| `nd_tfcurr` | `` |
| `nd_tfvalue` | `0.0` |
| `nd_vatcde` | `N` |
| `nd_vattyp` | `N` |
| `nd_vatval` | `0.0` |

**29 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `nd_taxdate` | `None` | `NaT` |

*Row id=2.0:*
| Field | Before | After |
|-------|--------|-------|
| `nd_taxdate` | `None` | `NaT` |

*Row id=3.0:*
| Field | Before | After |
|-------|--------|-------|
| `nd_taxdate` | `None` | `NaT` |

*Row id=4.0:*
| Field | Before | After |
|-------|--------|-------|
| `nd_taxdate` | `None` | `NaT` |

*Row id=5.0:*
| Field | Before | After |
|-------|--------|-------|
| `nd_taxdate` | `None` | `NaT` |


#### nextid

**7 row(s) modified:**

*Row id=230.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `30.0` | `32.0` |

*Row id=233.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `8.0` | `9.0` |

*Row id=234.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `159921.0` | `159922.0` |

*Row id=235.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `212.0` | `213.0` |

*Row id=244.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `106646.0` | `106649.0` |


#### nhead

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `nh_crtot` | `100.0` |
| `nh_date` | `2024-05-01T00:00:00` |
| `nh_days` | `0.0` |
| `nh_drtot` | `100.0` |
| `nh_fcdec` | `0.0` |
| `nh_fcmult` | `0.0` |
| `nh_fcrate` | `0.0` |
| `nh_fcrtot` | `0.0` |
| `nh_fcurr` | `` |
| `nh_fdrtot` | `0.0` |
| `nh_freq` | `` |
| `nh_inp` | `TEST` |
| `nh_journal` | `3448.0` |
| `nh_ldate` | `2024-05-01T00:00:00` |
| `nh_memo` | `` |
| `nh_narr` | `test` |
| `nh_periods` | `0.0` |
| `nh_plast` | `5.0` |
| `nh_recur` | `0.0` |
| `nh_ref` | `JNL0000058` |
| `nh_retain` | `1.0` |
| `nh_rev` | `1.0` |
| `nh_times` | `1.0` |
| `nh_vatanal` | `1.0` |
| `nh_ylast` | `2024.0` |


#### nhist

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `nh_bal` | `-100.0` |
| `nh_budg` | `0.0` |
| `nh_fbal` | `0.0` |
| `nh_job` | `` |
| `nh_nacnt` | `E325` |
| `nh_ncntr` | `TEC` |
| `nh_nsubt` | `02` |
| `nh_ntype` | `15` |
| `nh_period` | `5.0` |
| `nh_project` | `` |
| `nh_ptdcr` | `-100.0` |
| `nh_ptddr` | `0.0` |
| `nh_rbudg` | `0.0` |
| `nh_rectype` | `1.0` |
| `nh_year` | `2024.0` |

**2 row(s) modified:**

*Row id=159888.0:*
| Field | Before | After |
|-------|--------|-------|
| `nh_bal` | `74973.36` | `74990.03` |
| `nh_ptddr` | `74973.36` | `74990.03` |

*Row id=159919.0:*
| Field | Before | After |
|-------|--------|-------|
| `nh_bal` | `-55.0` | `28.33` |
| `nh_ptddr` | `0.0` | `83.33` |


#### njmemo

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `nj_binrep` | `1.0` |
| `nj_image` | `` |
| `nj_journal` | `3448.0` |
| `nj_memo` | `ÿ<<JOURNAL_DATA_ONLY>>ÿ` |
| `nj_txtrep` | `` |


#### nparm

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `np_nexdoc` | `JNL0000058` | `JNL0000059` |


#### nsubt

**2 row(s) modified:**

*Row id=11.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `-54876.79` | `-54960.12` |

*Row id=23.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `28.33` | `111.66` |


#### ntype

**2 row(s) modified:**

*Row id=4.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `-285124.75` | `-285208.08` |

*Row id=10.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `374221.8` | `374305.13` |


#### nvat

**2 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `nv_acnt` | `S120` |
| `nv_advance` | `0.0` |
| `nv_cntr` | `ADM` |
| `nv_comment` | `test` |
| `nv_crdate` | `2024-05-01T00:00:00` |
| `nv_date` | `2024-05-01T00:00:00` |
| `nv_ref` | `JNL0000058` |
| `nv_taxdate` | `2024-05-01T00:00:00` |
| `nv_type` | `I` |
| `nv_value` | `100.0` |
| `nv_vatcode` | `1` |
| `nv_vatctry` | `H` |
| `nv_vatrate` | `20.0` |
| `nv_vattype` | `P` |
| `nv_vatval` | `16.67` |

*Row 2:*
| Field | Value |
|-------|-------|
| `nv_acnt` | `E325` |
| `nv_advance` | `0.0` |
| `nv_cntr` | `TEC` |
| `nv_comment` | `test` |
| `nv_crdate` | `2024-05-01T00:00:00` |
| `nv_date` | `2024-05-01T00:00:00` |
| `nv_ref` | `JNL0000058` |
| `nv_type` | `I` |
| `nv_value` | `-100.0` |
| `nv_vatcode` | `N` |
| `nv_vatctry` | `H` |
| `nv_vatrate` | `0.0` |
| `nv_vattype` | `N` |
| `nv_vatval` | `0.0` |


#### zpool

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `sp_cby` | `TEST` |
| `sp_cdate` | `2026-04-01T00:00:00` |
| `sp_ctime` | `14:23` |
| `sp_desc` | `sdf` |
| `sp_file` | `SFS` |
| `sp_origin` | `` |
| `sp_pby` | `` |
| `sp_platfrm` | `32BIT` |
| `sp_printer` | `PDF:` |
| `sp_ptime` | `` |
| `sp_rephite` | `0.0` |
| `sp_repwide` | `0.0` |


---

### Nominal Journal - not posted

#### sequser

**1 row(s) modified:**

*Row id=7.0:*
| Field | Before | After |
|-------|--------|-------|


#### ndetl

**2 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `nd_account` | `GA110` |
| `nd_comm` | `test]` |
| `nd_cramnt` | `0.0` |
| `nd_dramnt` | `120.0` |
| `nd_fcdec` | `0.0` |
| `nd_fcmult` | `0.0` |
| `nd_fcrate` | `0.0` |
| `nd_fcurr` | `` |
| `nd_fvalue` | `0.0` |
| `nd_job` | `` |
| `nd_project` | `U999` |
| `nd_recno` | `1.0` |
| `nd_ref` | `INT00522` |
| `nd_taxdate` | `2026-04-06T00:00:00` |
| `nd_tfcdec` | `0.0` |
| `nd_tfcmult` | `0.0` |
| `nd_tfcrate` | `0.0` |
| `nd_tfcurr` | `` |
| `nd_tfvalue` | `0.0` |
| `nd_vatcde` | `2` |
| `nd_vattyp` | `P` |
| `nd_vatval` | `20.0` |

*Row 2:*
| Field | Value |
|-------|-------|
| `nd_account` | `GA045` |
| `nd_comm` | `test]` |
| `nd_cramnt` | `120.0` |
| `nd_dramnt` | `0.0` |
| `nd_fcdec` | `0.0` |
| `nd_fcmult` | `0.0` |
| `nd_fcrate` | `0.0` |
| `nd_fcurr` | `` |
| `nd_fvalue` | `0.0` |
| `nd_job` | `` |
| `nd_project` | `` |
| `nd_recno` | `2.0` |
| `nd_ref` | `INT00522` |
| `nd_tfcdec` | `0.0` |
| `nd_tfcmult` | `0.0` |
| `nd_tfcrate` | `0.0` |
| `nd_tfcurr` | `` |
| `nd_tfvalue` | `0.0` |
| `nd_vatcde` | `N` |
| `nd_vattyp` | `N` |
| `nd_vatval` | `0.0` |


#### nextid

**2 row(s) modified:**

*Row id=107.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `236.0` | `238.0` |

*Row id=109.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `41.0` | `42.0` |


#### nhead

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `nh_crtot` | `120.0` |
| `nh_date` | `2026-04-06T00:00:00` |
| `nh_days` | `0.0` |
| `nh_drtot` | `120.0` |
| `nh_fcdec` | `0.0` |
| `nh_fcmult` | `0.0` |
| `nh_fcrate` | `0.0` |
| `nh_fcrtot` | `0.0` |
| `nh_fcurr` | `` |
| `nh_fdrtot` | `0.0` |
| `nh_freq` | `` |
| `nh_inp` | `TEST` |
| `nh_journal` | `0.0` |
| `nh_memo` | `` |
| `nh_narr` | `test12` |
| `nh_periods` | `0.0` |
| `nh_plast` | `0.0` |
| `nh_recur` | `0.0` |
| `nh_ref` | `INT00522` |
| `nh_retain` | `0.0` |
| `nh_rev` | `0.0` |
| `nh_times` | `0.0` |
| `nh_vatanal` | `1.0` |
| `nh_ylast` | `0.0` |


#### nparm

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `np_nexdoc` | `INT00522` | `INT00523` |


---

## Nominal Account Master (nname/nacnt)

### Cashbook transfer

#### anoml

**101 row(s) modified:**

*Row id=26020.0:*
| Field | Before | After |
|-------|--------|-------|
| `ax_done` | `` | `Y` |
| `ax_jrnl` | `0.0` | `3441.0` |

*Row id=26021.0:*
| Field | Before | After |
|-------|--------|-------|
| `ax_done` | `` | `Y` |
| `ax_jrnl` | `0.0` | `3441.0` |

*Row id=26022.0:*
| Field | Before | After |
|-------|--------|-------|
| `ax_done` | `` | `Y` |
| `ax_jrnl` | `0.0` | `3441.0` |

*Row id=26023.0:*
| Field | Before | After |
|-------|--------|-------|
| `ax_done` | `` | `Y` |
| `ax_jrnl` | `0.0` | `3441.0` |

*Row id=26024.0:*
| Field | Before | After |
|-------|--------|-------|
| `ax_done` | `` | `Y` |
| `ax_jrnl` | `0.0` | `3441.0` |


#### idtab

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `id_numericid` | `3441.0` | `3442.0` |


#### nacnt

**13 row(s) modified:**

*Row id=12.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `0.0` | `-2273.99` |
| `na_ptdcr` | `0.0` | `2297.98` |
| `na_ptddr` | `0.0` | `23.99` |
| `na_ytdcr` | `1560707.04` | `1563005.02` |
| `na_ytddr` | `1972598.78` | `1972622.77` |

*Row id=16.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `0.0` | `-27387.95` |
| `na_ptdcr` | `0.0` | `27387.95` |
| `na_ytdcr` | `196818.16` | `224206.11` |

*Row id=17.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `0.0` | `40.0` |
| `na_ptddr` | `0.0` | `40.0` |
| `na_ytddr` | `1199244.75` | `1199284.75` |

*Row id=18.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `0.0` | `-1096790.68` |
| `na_fbalc05` | `0.0` | `-175500000.0` |
| `na_fptdcr` | `0.0` | `175500000.0` |
| `na_fytdcr` | `26500000.0` | `202000000.0` |
| `na_ptdcr` | `0.0` | `1096790.68` |
| `na_ytdcr` | `165575.47` | `1262366.15` |

*Row id=19.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `0.0` | `-3500.0` |
| `na_ptdcr` | `0.0` | `3500.0` |
| `na_ytdcr` | `186358.58` | `189858.58` |


#### ndetail

**37 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `nt_acnt` | `C310` |
| `nt_cdesc` | `` |
| `nt_cmnt` | `WAGES TRANSFER` |
| `nt_cntr` | `` |
| `nt_consol` | `0.0` |
| `nt_distrib` | `0.0` |
| `nt_entr` | `2024-05-01T00:00:00` |
| `nt_fcdec` | `0.0` |
| `nt_fcmult` | `0.0` |
| `nt_fcrate` | `0.0` |
| `nt_fcurr` | `` |
| `nt_fvalue` | `0.0` |
| `nt_inp` | `TEST` |
| `nt_job` | `` |
| `nt_jrnl` | `3441.0` |
| `nt_period` | `5.0` |
| `nt_perpost` | `0.0` |
| `nt_posttyp` | `W` |
| `nt_prevyr` | `0.0` |
| `nt_project` | `` |
| `nt_pstgrp` | `1.0` |
| `nt_pstid` | `_4J9H1FE00` |
| `nt_recjrnl` | `0.0` |
| `nt_rectify` | `0.0` |
| `nt_recurr` | `0.0` |
| `nt_ref` | `` |
| `nt_rvrse` | `0.0` |
| `nt_srcco` | `Z` |
| `nt_subt` | `03` |
| `nt_trnref` | `` |
| `nt_trtype` | `A` |
| `nt_type` | `10` |
| `nt_value` | `-2381.49` |
| `nt_vatanal` | `0.0` |
| `nt_year` | `2024.0` |

*Row 2:*
| Field | Value |
|-------|-------|
| `nt_acnt` | `C310` |
| `nt_cdesc` | `` |
| `nt_cmnt` | `WAGES TRANSFER` |
| `nt_cntr` | `` |
| `nt_consol` | `0.0` |
| `nt_distrib` | `0.0` |
| `nt_entr` | `2024-05-01T00:00:00` |
| `nt_fcdec` | `0.0` |
| `nt_fcmult` | `0.0` |
| `nt_fcrate` | `0.0` |
| `nt_fcurr` | `` |
| `nt_fvalue` | `0.0` |
| `nt_inp` | `TEST` |
| `nt_job` | `` |
| `nt_jrnl` | `3441.0` |
| `nt_period` | `5.0` |
| `nt_perpost` | `0.0` |
| `nt_posttyp` | `W` |
| `nt_prevyr` | `0.0` |
| `nt_project` | `` |
| `nt_pstgrp` | `2.0` |
| `nt_pstid` | `_4J9H1FE00` |
| `nt_recjrnl` | `0.0` |
| `nt_rectify` | `0.0` |
| `nt_recurr` | `0.0` |
| `nt_ref` | `` |
| `nt_rvrse` | `0.0` |
| `nt_srcco` | `Z` |
| `nt_subt` | `03` |
| `nt_trnref` | `` |
| `nt_trtype` | `A` |
| `nt_type` | `10` |
| `nt_value` | `-1041.24` |
| `nt_vatanal` | `0.0` |
| `nt_year` | `2024.0` |

*Row 3:*
| Field | Value |
|-------|-------|
| `nt_acnt` | `C310` |
| `nt_cdesc` | `` |
| `nt_cmnt` | `WAGES TRANSFER` |
| `nt_cntr` | `` |
| `nt_consol` | `0.0` |
| `nt_distrib` | `0.0` |
| `nt_entr` | `2024-05-01T00:00:00` |
| `nt_fcdec` | `0.0` |
| `nt_fcmult` | `0.0` |
| `nt_fcrate` | `0.0` |
| `nt_fcurr` | `` |
| `nt_fvalue` | `0.0` |
| `nt_inp` | `TEST` |
| `nt_job` | `` |
| `nt_jrnl` | `3441.0` |
| `nt_period` | `5.0` |
| `nt_perpost` | `0.0` |
| `nt_posttyp` | `W` |
| `nt_prevyr` | `0.0` |
| `nt_project` | `` |
| `nt_pstgrp` | `3.0` |
| `nt_pstid` | `_4J9H1FE00` |
| `nt_recjrnl` | `0.0` |
| `nt_rectify` | `0.0` |
| `nt_recurr` | `0.0` |
| `nt_ref` | `` |
| `nt_rvrse` | `0.0` |
| `nt_srcco` | `Z` |
| `nt_subt` | `03` |
| `nt_trnref` | `` |
| `nt_trtype` | `A` |
| `nt_type` | `10` |
| `nt_value` | `-2131.83` |
| `nt_vatanal` | `0.0` |
| `nt_year` | `2024.0` |

*Row 4:*
| Field | Value |
|-------|-------|
| `nt_acnt` | `C310` |
| `nt_cdesc` | `` |
| `nt_cmnt` | `WAGES TRANSFER` |
| `nt_cntr` | `` |
| `nt_consol` | `0.0` |
| `nt_distrib` | `0.0` |
| `nt_entr` | `2024-05-01T00:00:00` |
| `nt_fcdec` | `0.0` |
| `nt_fcmult` | `0.0` |
| `nt_fcrate` | `0.0` |
| `nt_fcurr` | `` |
| `nt_fvalue` | `0.0` |
| `nt_inp` | `TEST` |
| `nt_job` | `` |
| `nt_jrnl` | `3441.0` |
| `nt_period` | `5.0` |
| `nt_perpost` | `0.0` |
| `nt_posttyp` | `W` |
| `nt_prevyr` | `0.0` |
| `nt_project` | `` |
| `nt_pstgrp` | `4.0` |
| `nt_pstid` | `_4J9H1FE00` |
| `nt_recjrnl` | `0.0` |
| `nt_rectify` | `0.0` |
| `nt_recurr` | `0.0` |
| `nt_ref` | `` |
| `nt_rvrse` | `0.0` |
| `nt_srcco` | `Z` |
| `nt_subt` | `03` |
| `nt_trnref` | `` |
| `nt_trtype` | `A` |
| `nt_type` | `10` |
| `nt_value` | `-1015.04` |
| `nt_vatanal` | `0.0` |
| `nt_year` | `2024.0` |

*Row 5:*
| Field | Value |
|-------|-------|
| `nt_acnt` | `C310` |
| `nt_cdesc` | `` |
| `nt_cmnt` | `WAGES TRANSFER` |
| `nt_cntr` | `` |
| `nt_consol` | `0.0` |
| `nt_distrib` | `0.0` |
| `nt_entr` | `2024-05-01T00:00:00` |
| `nt_fcdec` | `0.0` |
| `nt_fcmult` | `0.0` |
| `nt_fcrate` | `0.0` |
| `nt_fcurr` | `` |
| `nt_fvalue` | `0.0` |
| `nt_inp` | `TEST` |
| `nt_job` | `` |
| `nt_jrnl` | `3441.0` |
| `nt_period` | `5.0` |
| `nt_perpost` | `0.0` |
| `nt_posttyp` | `W` |
| `nt_prevyr` | `0.0` |
| `nt_project` | `` |
| `nt_pstgrp` | `5.0` |
| `nt_pstid` | `_4J9H1FE00` |
| `nt_recjrnl` | `0.0` |
| `nt_rectify` | `0.0` |
| `nt_recurr` | `0.0` |
| `nt_ref` | `` |
| `nt_rvrse` | `0.0` |
| `nt_srcco` | `Z` |
| `nt_subt` | `03` |
| `nt_trnref` | `` |
| `nt_trtype` | `A` |
| `nt_type` | `10` |
| `nt_value` | `-2132.03` |
| `nt_vatanal` | `0.0` |
| `nt_year` | `2024.0` |


#### nextid

**4 row(s) modified:**

*Row id=229.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `6857.0` | `6894.0` |

*Row id=234.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `159881.0` | `159894.0` |

*Row id=235.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `205.0` | `206.0` |

*Row id=244.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `106363.0` | `106431.0` |


#### nhist

**13 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `nh_bal` | `-2273.99` |
| `nh_budg` | `0.0` |
| `nh_fbal` | `0.0` |
| `nh_job` | `` |
| `nh_nacnt` | `C110` |
| `nh_ncntr` | `` |
| `nh_nsubt` | `01` |
| `nh_ntype` | `10` |
| `nh_period` | `5.0` |
| `nh_project` | `` |
| `nh_ptdcr` | `-2297.98` |
| `nh_ptddr` | `23.99` |
| `nh_rbudg` | `0.0` |
| `nh_rectype` | `1.0` |
| `nh_year` | `2024.0` |

*Row 2:*
| Field | Value |
|-------|-------|
| `nh_bal` | `-27387.95` |
| `nh_budg` | `0.0` |
| `nh_fbal` | `0.0` |
| `nh_job` | `` |
| `nh_nacnt` | `C310` |
| `nh_ncntr` | `` |
| `nh_nsubt` | `03` |
| `nh_ntype` | `10` |
| `nh_period` | `5.0` |
| `nh_project` | `` |
| `nh_ptdcr` | `-27387.95` |
| `nh_ptddr` | `0.0` |
| `nh_rbudg` | `0.0` |
| `nh_rectype` | `1.0` |
| `nh_year` | `2024.0` |

*Row 3:*
| Field | Value |
|-------|-------|
| `nh_bal` | `40.0` |
| `nh_budg` | `0.0` |
| `nh_fbal` | `0.0` |
| `nh_job` | `` |
| `nh_nacnt` | `C315` |
| `nh_ncntr` | `` |
| `nh_nsubt` | `03` |
| `nh_ntype` | `10` |
| `nh_period` | `5.0` |
| `nh_project` | `` |
| `nh_ptdcr` | `0.0` |
| `nh_ptddr` | `40.0` |
| `nh_rbudg` | `0.0` |
| `nh_rectype` | `1.0` |
| `nh_year` | `2024.0` |

*Row 4:*
| Field | Value |
|-------|-------|
| `nh_bal` | `-1096790.68` |
| `nh_budg` | `0.0` |
| `nh_fbal` | `-175500000.0` |
| `nh_job` | `` |
| `nh_nacnt` | `C320` |
| `nh_ncntr` | `` |
| `nh_nsubt` | `03` |
| `nh_ntype` | `10` |
| `nh_period` | `5.0` |
| `nh_project` | `` |
| `nh_ptdcr` | `-1096790.68` |
| `nh_ptddr` | `0.0` |
| `nh_rbudg` | `0.0` |
| `nh_rectype` | `1.0` |
| `nh_year` | `2024.0` |

*Row 5:*
| Field | Value |
|-------|-------|
| `nh_bal` | `-3500.0` |
| `nh_budg` | `0.0` |
| `nh_fbal` | `0.0` |
| `nh_job` | `` |
| `nh_nacnt` | `C330` |
| `nh_ncntr` | `` |
| `nh_nsubt` | `03` |
| `nh_ntype` | `10` |
| `nh_period` | `5.0` |
| `nh_project` | `` |
| `nh_ptdcr` | `-3500.0` |
| `nh_ptddr` | `0.0` |
| `nh_rbudg` | `0.0` |
| `nh_rectype` | `1.0` |
| `nh_year` | `2024.0` |


#### njmemo

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `nj_binrep` | `1.0` |
| `nj_image` | `` |
| `nj_journal` | `3441.0` |
| `nj_memo` | `ÿ<<JOURNAL_DATA_ONLY>>ÿ` |
| `nj_txtrep` | `` |


#### nsubt

**6 row(s) modified:**

*Row id=6.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `411891.74` | `409617.75` |

*Row id=8.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `3898486.98` | `2770848.35` |

*Row id=10.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `-1173951.96` | `-75030.6` |

*Row id=11.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `-87013.12` | `-61729.95` |

*Row id=12.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `-2511007.72` | `-2505382.96` |


#### ntype

**4 row(s) modified:**

*Row id=3.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `6231605.25` | `5101692.63` |

*Row id=4.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `-1260965.08` | `-136760.55` |

*Row id=5.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `-2511007.72` | `-2505382.96` |

*Row id=10.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `333028.01` | `333111.34` |


---

### New Nominal Account

#### nacnt

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `na_acnt` | `A1224` |
| `na_allwjob` | `2.0` |
| `na_allwprj` | `2.0` |
| `na_balc01` | `0.0` |
| `na_balc02` | `0.0` |
| `na_balc03` | `0.0` |
| `na_balc04` | `0.0` |
| `na_balc05` | `0.0` |
| `na_balc06` | `0.0` |
| `na_balc07` | `0.0` |
| `na_balc08` | `0.0` |
| `na_balc09` | `0.0` |
| `na_balc10` | `0.0` |
| `na_balc11` | `0.0` |
| `na_balc12` | `0.0` |
| `na_balc13` | `0.0` |
| `na_balc14` | `0.0` |
| `na_balc15` | `0.0` |
| `na_balc16` | `0.0` |
| `na_balc17` | `0.0` |
| `na_balc18` | `0.0` |
| `na_balc19` | `0.0` |
| `na_balc20` | `0.0` |
| `na_balc21` | `0.0` |
| `na_balc22` | `0.0` |
| `na_balc23` | `0.0` |
| `na_balc24` | `0.0` |
| `na_balp01` | `0.0` |
| `na_balp02` | `0.0` |
| `na_balp03` | `0.0` |
| `na_balp04` | `0.0` |
| `na_balp05` | `0.0` |
| `na_balp06` | `0.0` |
| `na_balp07` | `0.0` |
| `na_balp08` | `0.0` |
| `na_balp09` | `0.0` |
| `na_balp10` | `0.0` |
| `na_balp11` | `0.0` |
| `na_balp12` | `0.0` |
| `na_balp13` | `0.0` |
| `na_balp14` | `0.0` |
| `na_balp15` | `0.0` |
| `na_balp16` | `0.0` |
| `na_balp17` | `0.0` |
| `na_balp18` | `0.0` |
| `na_balp19` | `0.0` |
| `na_balp20` | `0.0` |
| `na_balp21` | `0.0` |
| `na_balp22` | `0.0` |
| `na_balp23` | `0.0` |
| `na_balp24` | `0.0` |
| `na_cntr` | `` |
| `na_comm` | `1.0` |
| `na_desc` | `Test` |
| `na_extcode` | `` |
| `na_fbalc01` | `0.0` |
| `na_fbalc02` | `0.0` |
| `na_fbalc03` | `0.0` |
| `na_fbalc04` | `0.0` |
| `na_fbalc05` | `0.0` |
| `na_fbalc06` | `0.0` |
| `na_fbalc07` | `0.0` |
| `na_fbalc08` | `0.0` |
| `na_fbalc09` | `0.0` |
| `na_fbalc10` | `0.0` |
| `na_fbalc11` | `0.0` |
| `na_fbalc12` | `0.0` |
| `na_fbalc13` | `0.0` |
| `na_fbalc14` | `0.0` |
| `na_fbalc15` | `0.0` |
| `na_fbalc16` | `0.0` |
| `na_fbalc17` | `0.0` |
| `na_fbalc18` | `0.0` |
| `na_fbalc19` | `0.0` |
| `na_fbalc20` | `0.0` |
| `na_fbalc21` | `0.0` |
| `na_fbalc22` | `0.0` |
| `na_fbalc23` | `0.0` |
| `na_fbalc24` | `0.0` |
| `na_fbalp01` | `0.0` |
| `na_fbalp02` | `0.0` |
| `na_fbalp03` | `0.0` |
| `na_fbalp04` | `0.0` |
| `na_fbalp05` | `0.0` |
| `na_fbalp06` | `0.0` |
| `na_fbalp07` | `0.0` |
| `na_fbalp08` | `0.0` |
| `na_fbalp09` | `0.0` |
| `na_fbalp10` | `0.0` |
| `na_fbalp11` | `0.0` |
| `na_fbalp12` | `0.0` |
| `na_fbalp13` | `0.0` |
| `na_fbalp14` | `0.0` |
| `na_fbalp15` | `0.0` |
| `na_fbalp16` | `0.0` |
| `na_fbalp17` | `0.0` |
| `na_fbalp18` | `0.0` |
| `na_fbalp19` | `0.0` |
| `na_fbalp20` | `0.0` |
| `na_fbalp21` | `0.0` |
| `na_fbalp22` | `0.0` |
| `na_fbalp23` | `0.0` |
| `na_fbalp24` | `0.0` |
| `na_fcdec` | `0.0` |
| `na_fcmult` | `0.0` |
| `na_fcrate` | `0.0` |
| `na_fcurr` | `` |
| `na_fprycr` | `0.0` |
| `na_fprydr` | `0.0` |
| `na_fptdcr` | `0.0` |
| `na_fptddr` | `0.0` |
| `na_fytdcr` | `0.0` |
| `na_fytddr` | `0.0` |
| `na_job` | `A001` |
| `na_key1` | `TEST` |
| `na_key2` | `` |
| `na_key3` | `` |
| `na_key4` | `` |
| `na_memo` | `` |
| `na_open` | `1.0` |
| `na_post` | `0.0` |
| `na_project` | `S020` |
| `na_prycr` | `0.0` |
| `na_prydr` | `0.0` |
| `na_ptdcr` | `0.0` |
| `na_ptddr` | `0.0` |
| `na_redist` | `0.0` |
| `na_repkey1` | `001` |
| `na_repkey2` | `` |
| `na_repkey3` | `` |
| `na_repkey4` | `` |
| `na_repkey5` | `` |
| `na_subt` | `00` |
| `na_type` | `A` |
| `na_ytdcr` | `0.0` |
| `na_ytddr` | `0.0` |
| `sq_private` | `0.0` |


#### nextid

**1 row(s) modified:**

*Row id=101.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `346.0` | `347.0` |


---

### Nominal journal (template not posted - no vat)

#### ndetl

**2 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `nd_account` | `C210` |
| `nd_comm` | `sdfs` |
| `nd_cramnt` | `0.0` |
| `nd_dramnt` | `100.0` |
| `nd_fcdec` | `0.0` |
| `nd_fcmult` | `0.0` |
| `nd_fcrate` | `0.0` |
| `nd_fcurr` | `` |
| `nd_fvalue` | `0.0` |
| `nd_job` | `` |
| `nd_project` | `` |
| `nd_recno` | `1.0` |
| `nd_ref` | `JNL0000060` |
| `nd_tfcdec` | `0.0` |
| `nd_tfcmult` | `0.0` |
| `nd_tfcrate` | `0.0` |
| `nd_tfcurr` | `` |
| `nd_tfvalue` | `0.0` |
| `nd_vatcde` | `` |
| `nd_vattyp` | `` |
| `nd_vatval` | `0.0` |

*Row 2:*
| Field | Value |
|-------|-------|
| `nd_account` | `C210` |
| `nd_comm` | `sdfs` |
| `nd_cramnt` | `100.0` |
| `nd_dramnt` | `0.0` |
| `nd_fcdec` | `0.0` |
| `nd_fcmult` | `0.0` |
| `nd_fcrate` | `0.0` |
| `nd_fcurr` | `` |
| `nd_fvalue` | `0.0` |
| `nd_job` | `` |
| `nd_project` | `` |
| `nd_recno` | `2.0` |
| `nd_ref` | `JNL0000060` |
| `nd_tfcdec` | `0.0` |
| `nd_tfcmult` | `0.0` |
| `nd_tfcrate` | `0.0` |
| `nd_tfcurr` | `` |
| `nd_tfvalue` | `0.0` |
| `nd_vatcde` | `` |
| `nd_vattyp` | `` |
| `nd_vatval` | `0.0` |


#### nextid

**2 row(s) modified:**

*Row id=230.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `34.0` | `36.0` |

*Row id=233.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `10.0` | `11.0` |


#### nhead

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `nh_crtot` | `100.0` |
| `nh_date` | `2024-05-01T00:00:00` |
| `nh_days` | `0.0` |
| `nh_drtot` | `100.0` |
| `nh_fcdec` | `0.0` |
| `nh_fcmult` | `0.0` |
| `nh_fcrate` | `0.0` |
| `nh_fcrtot` | `0.0` |
| `nh_fcurr` | `` |
| `nh_fdrtot` | `0.0` |
| `nh_freq` | `` |
| `nh_inp` | `TEST` |
| `nh_journal` | `0.0` |
| `nh_memo` | `` |
| `nh_narr` | `test3` |
| `nh_periods` | `0.0` |
| `nh_plast` | `0.0` |
| `nh_recur` | `0.0` |
| `nh_ref` | `JNL0000060` |
| `nh_retain` | `1.0` |
| `nh_rev` | `0.0` |
| `nh_times` | `0.0` |
| `nh_vatanal` | `0.0` |
| `nh_ylast` | `0.0` |


#### nparm

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `np_nexdoc` | `JNL0000060` | `JNL0000061` |


---

### Nominal journal (template not posted)

#### ndetl

**2 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `nd_account` | `E330     TEC` |
| `nd_comm` | `test` |
| `nd_cramnt` | `0.0` |
| `nd_dramnt` | `100.0` |
| `nd_fcdec` | `0.0` |
| `nd_fcmult` | `0.0` |
| `nd_fcrate` | `0.0` |
| `nd_fcurr` | `` |
| `nd_fvalue` | `0.0` |
| `nd_job` | `` |
| `nd_project` | `` |
| `nd_recno` | `1.0` |
| `nd_ref` | `JNL0000059` |
| `nd_taxdate` | `2024-05-01T00:00:00` |
| `nd_tfcdec` | `0.0` |
| `nd_tfcmult` | `0.0` |
| `nd_tfcrate` | `0.0` |
| `nd_tfcurr` | `` |
| `nd_tfvalue` | `0.0` |
| `nd_vatcde` | `1` |
| `nd_vattyp` | `P` |
| `nd_vatval` | `16.67` |

*Row 2:*
| Field | Value |
|-------|-------|
| `nd_account` | `A150` |
| `nd_comm` | `test` |
| `nd_cramnt` | `100.0` |
| `nd_dramnt` | `0.0` |
| `nd_fcdec` | `0.0` |
| `nd_fcmult` | `0.0` |
| `nd_fcrate` | `0.0` |
| `nd_fcurr` | `` |
| `nd_fvalue` | `0.0` |
| `nd_job` | `` |
| `nd_project` | `` |
| `nd_recno` | `2.0` |
| `nd_ref` | `JNL0000059` |
| `nd_taxdate` | `2024-05-01T00:00:00` |
| `nd_tfcdec` | `0.0` |
| `nd_tfcmult` | `0.0` |
| `nd_tfcrate` | `0.0` |
| `nd_tfcurr` | `` |
| `nd_tfvalue` | `0.0` |
| `nd_vatcde` | `1` |
| `nd_vattyp` | `S` |
| `nd_vatval` | `16.67` |


#### nextid

**2 row(s) modified:**

*Row id=230.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `32.0` | `34.0` |

*Row id=233.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `9.0` | `10.0` |


#### nhead

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `nh_crtot` | `100.0` |
| `nh_date` | `2024-05-01T00:00:00` |
| `nh_days` | `0.0` |
| `nh_drtot` | `100.0` |
| `nh_fcdec` | `0.0` |
| `nh_fcmult` | `0.0` |
| `nh_fcrate` | `0.0` |
| `nh_fcrtot` | `0.0` |
| `nh_fcurr` | `` |
| `nh_fdrtot` | `0.0` |
| `nh_freq` | `` |
| `nh_inp` | `TEST` |
| `nh_journal` | `0.0` |
| `nh_memo` | `` |
| `nh_narr` | `test2` |
| `nh_periods` | `0.0` |
| `nh_plast` | `0.0` |
| `nh_recur` | `0.0` |
| `nh_ref` | `JNL0000059` |
| `nh_retain` | `1.0` |
| `nh_rev` | `0.0` |
| `nh_times` | `0.0` |
| `nh_vatanal` | `1.0` |
| `nh_ylast` | `0.0` |


#### nparm

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `np_nexdoc` | `JNL0000059` | `JNL0000060` |


---

### Sales transfer

#### idtab

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `id_numericid` | `3443.0` | `3444.0` |


#### nacnt

**7 row(s) modified:**

*Row id=12.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `-2273.99` | `20941.73` |
| `na_ptddr` | `23.99` | `23239.71` |
| `na_ytddr` | `1972622.77` | `1995838.49` |

*Row id=32.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `-37395.91` | `-41265.2` |
| `na_ptdcr` | `37395.91` | `41265.2` |
| `na_ytdcr` | `660594.35` | `664463.64` |

*Row id=55.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `0.0` | `-100.0` |
| `na_ptdcr` | `0.0` | `100.0` |
| `na_ytdcr` | `7931.44` | `8031.44` |

*Row id=57.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `0.0` | `-6568.31` |
| `na_ptdcr` | `0.0` | `6568.31` |
| `na_ytdcr` | `782531.66` | `789099.97` |

*Row id=59.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `0.0` | `-4828.13` |
| `na_ptdcr` | `0.0` | `4828.13` |
| `na_ytdcr` | `556864.31` | `561692.44` |


#### nextid

**3 row(s) modified:**

*Row id=234.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `159898.0` | `159903.0` |

*Row id=235.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `207.0` | `208.0` |

*Row id=244.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `106533.0` | `106569.0` |


#### nhist

**5 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `nh_bal` | `-100.0` |
| `nh_budg` | `0.0` |
| `nh_fbal` | `0.0` |
| `nh_job` | `` |
| `nh_nacnt` | `K110` |
| `nh_ncntr` | `SAL` |
| `nh_nsubt` | `01` |
| `nh_ntype` | `30` |
| `nh_period` | `5.0` |
| `nh_project` | `` |
| `nh_ptdcr` | `-100.0` |
| `nh_ptddr` | `0.0` |
| `nh_rbudg` | `0.0` |
| `nh_rectype` | `1.0` |
| `nh_year` | `2024.0` |

*Row 2:*
| Field | Value |
|-------|-------|
| `nh_bal` | `-6568.31` |
| `nh_budg` | `0.0` |
| `nh_fbal` | `0.0` |
| `nh_job` | `` |
| `nh_nacnt` | `K120` |
| `nh_ncntr` | `` |
| `nh_nsubt` | `02` |
| `nh_ntype` | `30` |
| `nh_period` | `5.0` |
| `nh_project` | `` |
| `nh_ptdcr` | `-6568.31` |
| `nh_ptddr` | `0.0` |
| `nh_rbudg` | `0.0` |
| `nh_rectype` | `1.0` |
| `nh_year` | `2024.0` |

*Row 3:*
| Field | Value |
|-------|-------|
| `nh_bal` | `-4828.13` |
| `nh_budg` | `0.0` |
| `nh_fbal` | `0.0` |
| `nh_job` | `` |
| `nh_nacnt` | `K120` |
| `nh_ncntr` | `LSG` |
| `nh_nsubt` | `02` |
| `nh_ntype` | `30` |
| `nh_period` | `5.0` |
| `nh_project` | `` |
| `nh_ptdcr` | `-4828.13` |
| `nh_ptddr` | `0.0` |
| `nh_rbudg` | `0.0` |
| `nh_rectype` | `1.0` |
| `nh_year` | `2024.0` |

*Row 4:*
| Field | Value |
|-------|-------|
| `nh_bal` | `-7830.0` |
| `nh_budg` | `0.0` |
| `nh_fbal` | `0.0` |
| `nh_job` | `` |
| `nh_nacnt` | `K120` |
| `nh_ncntr` | `SAL` |
| `nh_nsubt` | `02` |
| `nh_ntype` | `30` |
| `nh_period` | `5.0` |
| `nh_project` | `` |
| `nh_ptdcr` | `-7830.0` |
| `nh_ptddr` | `0.0` |
| `nh_rbudg` | `0.0` |
| `nh_rectype` | `1.0` |
| `nh_year` | `2024.0` |

*Row 5:*
| Field | Value |
|-------|-------|
| `nh_bal` | `-19.99` |
| `nh_budg` | `0.0` |
| `nh_fbal` | `0.0` |
| `nh_job` | `` |
| `nh_nacnt` | `K122` |
| `nh_ncntr` | `SAL` |
| `nh_nsubt` | `03` |
| `nh_ntype` | `30` |
| `nh_period` | `5.0` |
| `nh_project` | `` |
| `nh_ptdcr` | `-19.99` |
| `nh_ptddr` | `0.0` |
| `nh_rbudg` | `0.0` |
| `nh_rectype` | `1.0` |
| `nh_year` | `2024.0` |

**2 row(s) modified:**

*Row id=159881.0:*
| Field | Before | After |
|-------|--------|-------|
| `nh_bal` | `-2273.99` | `20941.73` |
| `nh_ptddr` | `23.99` | `23239.71` |

*Row id=159887.0:*
| Field | Before | After |
|-------|--------|-------|
| `nh_bal` | `-37395.91` | `-41265.2` |
| `nh_ptdcr` | `-37395.91` | `-41265.2` |


#### njmemo

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `nj_binrep` | `0.0` |
| `nj_image` | `` |
| `nj_journal` | `3443.0` |
| `nj_memo` | `ÿ<<JOURNAL_DATA_ONLY>>ÿ` |
| `nj_txtrep` | `Sales Ledger Transfer` |


#### nsubt

**5 row(s) modified:**

*Row id=6.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `409617.75` | `432833.47` |

*Row id=11.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `-38719.2` | `-42588.49` |

*Row id=15.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `-12770.02` | `-12870.02` |

*Row id=16.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `-1429373.4` | `-1448599.84` |

*Row id=25.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `-853.96` | `-873.95` |


#### ntype

**3 row(s) modified:**

*Row id=3.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `5101692.63` | `5124908.35` |

*Row id=4.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `-268967.16` | `-272836.45` |

*Row id=7.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `-1442997.38` | `-1462343.81` |


#### snoml

**35 row(s) modified:**

*Row id=18588.0:*
| Field | Before | After |
|-------|--------|-------|
| `sx_done` | `` | `Y` |
| `sx_jrnl` | `0.0` | `3443.0` |

*Row id=18589.0:*
| Field | Before | After |
|-------|--------|-------|
| `sx_done` | `` | `Y` |
| `sx_jrnl` | `0.0` | `3443.0` |

*Row id=18590.0:*
| Field | Before | After |
|-------|--------|-------|
| `sx_done` | `` | `Y` |
| `sx_jrnl` | `0.0` | `3443.0` |

*Row id=18591.0:*
| Field | Before | After |
|-------|--------|-------|
| `sx_done` | `` | `Y` |
| `sx_jrnl` | `0.0` | `3443.0` |

*Row id=18592.0:*
| Field | Before | After |
|-------|--------|-------|
| `sx_done` | `` | `Y` |
| `sx_jrnl` | `0.0` | `3443.0` |


---

### Stock Transfer

#### cnoml

**34 row(s) modified:**

*Row id=20890.0:*
| Field | Before | After |
|-------|--------|-------|
| `cx_done` | `` | `Y` |
| `cx_jrnl` | `0.0` | `3445.0` |

*Row id=20891.0:*
| Field | Before | After |
|-------|--------|-------|
| `cx_done` | `` | `Y` |
| `cx_jrnl` | `0.0` | `3445.0` |

*Row id=20892.0:*
| Field | Before | After |
|-------|--------|-------|
| `cx_done` | `` | `Y` |
| `cx_jrnl` | `0.0` | `3445.0` |

*Row id=20893.0:*
| Field | Before | After |
|-------|--------|-------|
| `cx_done` | `` | `Y` |
| `cx_jrnl` | `0.0` | `3445.0` |

*Row id=20894.0:*
| Field | Before | After |
|-------|--------|-------|
| `cx_done` | `` | `Y` |
| `cx_jrnl` | `0.0` | `3445.0` |


#### idtab

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `id_numericid` | `3445.0` | `3446.0` |


#### nacnt

**3 row(s) modified:**

*Row id=14.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `0.0` | `-9117.68` |
| `na_ptdcr` | `0.0` | `9663.68` |
| `na_ptddr` | `0.0` | `546.0` |
| `na_ytdcr` | `1129803.57` | `1139467.25` |
| `na_ytddr` | `3049081.84` | `3049627.84` |

*Row id=70.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `0.0` | `-546.0` |
| `na_ptdcr` | `0.0` | `546.0` |
| `na_ytdcr` | `1953959.46` | `1954505.46` |

*Row id=71.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `0.0` | `9663.68` |
| `na_ptddr` | `0.0` | `9663.68` |
| `na_ytddr` | `1128825.82` | `1138489.5` |


#### nextid

**3 row(s) modified:**

*Row id=234.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `159916.0` | `159919.0` |

*Row id=235.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `209.0` | `210.0` |

*Row id=244.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `106607.0` | `106641.0` |


#### nhist

**3 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `nh_bal` | `-9117.68` |
| `nh_budg` | `0.0` |
| `nh_fbal` | `0.0` |
| `nh_job` | `` |
| `nh_nacnt` | `C210` |
| `nh_ncntr` | `` |
| `nh_nsubt` | `02` |
| `nh_ntype` | `10` |
| `nh_period` | `5.0` |
| `nh_project` | `` |
| `nh_ptdcr` | `-9663.68` |
| `nh_ptddr` | `546.0` |
| `nh_rbudg` | `0.0` |
| `nh_rectype` | `1.0` |
| `nh_year` | `2024.0` |

*Row 2:*
| Field | Value |
|-------|-------|
| `nh_bal` | `-546.0` |
| `nh_budg` | `0.0` |
| `nh_fbal` | `0.0` |
| `nh_job` | `` |
| `nh_nacnt` | `M510` |
| `nh_ncntr` | `` |
| `nh_nsubt` | `02` |
| `nh_ntype` | `35` |
| `nh_period` | `5.0` |
| `nh_project` | `` |
| `nh_ptdcr` | `-546.0` |
| `nh_ptddr` | `0.0` |
| `nh_rbudg` | `0.0` |
| `nh_rectype` | `1.0` |
| `nh_year` | `2024.0` |

*Row 3:*
| Field | Value |
|-------|-------|
| `nh_bal` | `9663.68` |
| `nh_budg` | `0.0` |
| `nh_fbal` | `0.0` |
| `nh_job` | `` |
| `nh_nacnt` | `M520` |
| `nh_ncntr` | `` |
| `nh_nsubt` | `02` |
| `nh_ntype` | `35` |
| `nh_period` | `5.0` |
| `nh_project` | `` |
| `nh_ptdcr` | `0.0` |
| `nh_ptddr` | `9663.68` |
| `nh_rbudg` | `0.0` |
| `nh_rectype` | `1.0` |
| `nh_year` | `2024.0` |


#### njmemo

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `nj_binrep` | `0.0` |
| `nj_image` | `` |
| `nj_journal` | `3445.0` |
| `nj_memo` | `ÿ<<JOURNAL_DATA_ONLY>>ÿ` |
| `nj_txtrep` | `Stock Transfer` |


#### nsubt

**2 row(s) modified:**

*Row id=7.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `1921226.53` | `1912108.85` |

*Row id=19.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `-1098482.77` | `-1089365.09` |


#### ntype

**2 row(s) modified:**

*Row id=3.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `5124908.35` | `5115790.67` |

*Row id=9.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `195552.58` | `204670.26` |


---

### purchase transfer

#### idtab

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `id_numericid` | `3442.0` | `3443.0` |


#### nacnt

**14 row(s) modified:**

*Row id=28.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `1098921.36` | `943704.0` |
| `na_ptdcr` | `150.0` | `155367.36` |
| `na_ytdcr` | `3011981.91` | `3167199.27` |

*Row id=66.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `0.0` | `23587.59` |
| `na_ptddr` | `0.0` | `23587.59` |
| `na_ytddr` | `70762.77` | `94350.36` |

*Row id=67.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `0.0` | `73751.11` |
| `na_ptddr` | `0.0` | `73751.11` |
| `na_ytddr` | `221253.33` | `295004.44` |

*Row id=68.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `0.0` | `2622.4` |
| `na_ptddr` | `0.0` | `2622.4` |
| `na_ytddr` | `77818.87` | `80441.27` |

*Row id=88.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `-21.63` | `5692.66` |
| `na_ptddr` | `19.37` | `5733.66` |
| `na_ytddr` | `260.85` | `5975.14` |


#### nextid

**3 row(s) modified:**

*Row id=234.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `159894.0` | `159898.0` |

*Row id=235.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `206.0` | `207.0` |

*Row id=244.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `106431.0` | `106533.0` |


#### nhist

**4 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `nh_bal` | `23587.59` |
| `nh_budg` | `0.0` |
| `nh_fbal` | `0.0` |
| `nh_job` | `` |
| `nh_nacnt` | `M210` |
| `nh_ncntr` | `` |
| `nh_nsubt` | `01` |
| `nh_ntype` | `35` |
| `nh_period` | `5.0` |
| `nh_project` | `` |
| `nh_ptdcr` | `0.0` |
| `nh_ptddr` | `23587.59` |
| `nh_rbudg` | `0.0` |
| `nh_rectype` | `1.0` |
| `nh_year` | `2024.0` |

*Row 2:*
| Field | Value |
|-------|-------|
| `nh_bal` | `73751.11` |
| `nh_budg` | `0.0` |
| `nh_fbal` | `0.0` |
| `nh_job` | `` |
| `nh_nacnt` | `M310` |
| `nh_ncntr` | `` |
| `nh_nsubt` | `01` |
| `nh_ntype` | `35` |
| `nh_period` | `5.0` |
| `nh_project` | `` |
| `nh_ptdcr` | `0.0` |
| `nh_ptddr` | `73751.11` |
| `nh_rbudg` | `0.0` |
| `nh_rectype` | `1.0` |
| `nh_year` | `2024.0` |

*Row 3:*
| Field | Value |
|-------|-------|
| `nh_bal` | `2622.4` |
| `nh_budg` | `0.0` |
| `nh_fbal` | `0.0` |
| `nh_job` | `` |
| `nh_nacnt` | `M315` |
| `nh_ncntr` | `` |
| `nh_nsubt` | `01` |
| `nh_ntype` | `35` |
| `nh_period` | `5.0` |
| `nh_project` | `` |
| `nh_ptdcr` | `0.0` |
| `nh_ptddr` | `2622.4` |
| `nh_rbudg` | `0.0` |
| `nh_rectype` | `1.0` |
| `nh_year` | `2024.0` |

*Row 4:*
| Field | Value |
|-------|-------|
| `nh_bal` | `3388.37` |
| `nh_budg` | `0.0` |
| `nh_fbal` | `0.0` |
| `nh_job` | `SM` |
| `nh_nacnt` | `M320` |
| `nh_ncntr` | `` |
| `nh_nsubt` | `01` |
| `nh_ntype` | `35` |
| `nh_period` | `5.0` |
| `nh_project` | `PMA1` |
| `nh_ptdcr` | `0.0` |
| `nh_ptddr` | `3388.37` |
| `nh_rbudg` | `0.0` |
| `nh_rectype` | `1.0` |
| `nh_year` | `2024.0` |

**10 row(s) modified:**

*Row id=159863.0:*
| Field | Before | After |
|-------|--------|-------|
| `nh_bal` | `-21.63` | `5692.66` |
| `nh_ptddr` | `19.37` | `5733.66` |

*Row id=159864.0:*
| Field | Before | After |
|-------|--------|-------|
| `nh_bal` | `-21.63` | `5692.66` |
| `nh_ptddr` | `19.37` | `5733.66` |

*Row id=159865.0:*
| Field | Before | After |
|-------|--------|-------|
| `nh_bal` | `-21.63` | `5692.66` |
| `nh_ptddr` | `19.37` | `5733.66` |

*Row id=159866.0:*
| Field | Before | After |
|-------|--------|-------|
| `nh_bal` | `-21.62` | `5692.67` |
| `nh_ptddr` | `19.38` | `5733.67` |

*Row id=159867.0:*
| Field | Before | After |
|-------|--------|-------|
| `nh_bal` | `-80.0` | `1420.0` |
| `nh_ptddr` | `0.0` | `1500.0` |


#### njmemo

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `nj_binrep` | `0.0` |
| `nj_image` | `` |
| `nj_journal` | `3442.0` |
| `nj_memo` | `ÿ<<JOURNAL_DATA_ONLY>>ÿ` |
| `nj_txtrep` | `Purchase Ledger Transfer` |


#### nsubt

**4 row(s) modified:**

*Row id=10.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `-75030.6` | `-230247.96` |

*Row id=11.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `-61729.95` | `-38719.2` |

*Row id=18.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `1190685.88` | `1294035.35` |

*Row id=21.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `7963.04` | `36820.2` |


#### ntype

**3 row(s) modified:**

*Row id=4.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `-136760.55` | `-268967.16` |

*Row id=9.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `92203.11` | `195552.58` |

*Row id=10.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `333111.34` | `361968.5` |


#### pnoml

**95 row(s) modified:**

*Row id=6960.0:*
| Field | Before | After |
|-------|--------|-------|
| `px_done` | `` | `Y` |
| `px_jrnl` | `0.0` | `3442.0` |

*Row id=6961.0:*
| Field | Before | After |
|-------|--------|-------|
| `px_done` | `` | `Y` |
| `px_jrnl` | `0.0` | `3442.0` |

*Row id=6962.0:*
| Field | Before | After |
|-------|--------|-------|
| `px_done` | `` | `Y` |
| `px_jrnl` | `0.0` | `3442.0` |

*Row id=6963.0:*
| Field | Before | After |
|-------|--------|-------|
| `px_done` | `` | `Y` |
| `px_jrnl` | `0.0` | `3442.0` |

*Row id=6964.0:*
| Field | Before | After |
|-------|--------|-------|
| `px_done` | `` | `Y` |
| `px_jrnl` | `0.0` | `3442.0` |


---

## Purchase Ledger Transactions

### Purchase Invoice

#### dmcomp

**1 row(s) modified:**

*Row id=22.0:*
| Field | Before | After |
|-------|--------|-------|
| `changedon` | `2023-05-04T19:45:25` | `2026-04-01T14:29:26` |


#### idtab

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `id_numericid` | `3449.0` | `3450.0` |


#### nacnt

**3 row(s) modified:**

*Row id=28.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `943704.0` | `943584.0` |
| `na_ptdcr` | `155367.36` | `155487.36` |
| `na_ytdcr` | `3167199.27` | `3167319.27` |

*Row id=67.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `73751.11` | `73851.11` |
| `na_ptddr` | `73751.11` | `73851.11` |
| `na_ytddr` | `295004.44` | `295104.44` |

*Row id=252.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `74990.03` | `75010.03` |
| `na_ptddr` | `74990.03` | `75010.03` |
| `na_ytddr` | `750777.72` | `750797.72` |


#### nextid

**6 row(s) modified:**

*Row id=234.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `159922.0` | `159923.0` |

*Row id=235.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `213.0` | `214.0` |

*Row id=244.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `106649.0` | `106652.0` |

*Row id=249.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `3182.0` | `3183.0` |

*Row id=261.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `7055.0` | `7058.0` |


#### nhist

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `nh_bal` | `100.0` |
| `nh_budg` | `0.0` |
| `nh_fbal` | `0.0` |
| `nh_job` | `VM` |
| `nh_nacnt` | `M310` |
| `nh_ncntr` | `` |
| `nh_nsubt` | `01` |
| `nh_ntype` | `35` |
| `nh_period` | `5.0` |
| `nh_project` | `VM1` |
| `nh_ptdcr` | `0.0` |
| `nh_ptddr` | `100.0` |
| `nh_rbudg` | `0.0` |
| `nh_rectype` | `1.0` |
| `nh_year` | `2024.0` |

**2 row(s) modified:**

*Row id=159886.0:*
| Field | Before | After |
|-------|--------|-------|
| `nh_bal` | `943704.0` | `943584.0` |
| `nh_ptdcr` | `-155367.36` | `-155487.36` |

*Row id=159888.0:*
| Field | Before | After |
|-------|--------|-------|
| `nh_bal` | `74990.03` | `75010.03` |
| `nh_ptddr` | `74990.03` | `75010.03` |


#### njmemo

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `nj_binrep` | `0.0` |
| `nj_image` | `` |
| `nj_journal` | `3449.0` |
| `nj_memo` | `ÿ<<JOURNAL_DATA_ONLY>>ÿ` |
| `nj_txtrep` | `Purchase Ledger Transfer` |


#### nsubt

**3 row(s) modified:**

*Row id=10.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `-230247.96` | `-230367.96` |

*Row id=11.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `-54960.12` | `-54940.12` |

*Row id=18.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `1294035.35` | `1294135.35` |


#### ntype

**2 row(s) modified:**

*Row id=4.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `-285208.08` | `-285308.08` |

*Row id=9.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `204670.26` | `204770.26` |


#### panal

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `pa_account` | `CAR0001` |
| `pa_adjsv` | `0.0` |
| `pa_advance` | `N` |
| `pa_ancode` | `M310` |
| `pa_anvat` | `1` |
| `pa_box1` | `0.0` |
| `pa_box2` | `0.0` |
| `pa_box4` | `1.0` |
| `pa_box6` | `0.0` |
| `pa_box7` | `1.0` |
| `pa_box9` | `0.0` |
| `pa_commod` | `` |
| `pa_cost` | `0.0` |
| `pa_country` | `GB` |
| `pa_crdate` | `2024-05-01T00:00:00` |
| `pa_ctryorg` | `` |
| `pa_daccnt` | `CAR0001` |
| `pa_delterm` | `` |
| `pa_domrc` | `0.0` |
| `pa_facatg` | `` |
| `pa_fadesc` | `` |
| `pa_fasset` | `` |
| `pa_fasub` | `` |
| `pa_fccost` | `0.0` |
| `pa_fcdec` | `2.0` |
| `pa_fcurr` | `` |
| `pa_fcval` | `0.0` |
| `pa_fcvat` | `0.0` |
| `pa_interbr` | `0.0` |
| `pa_jccode` | `` |
| `pa_jcstdoc` | `` |
| `pa_jline` | `` |
| `pa_job` | `VM` |
| `pa_jphase` | `` |
| `pa_netmass` | `0.0` |
| `pa_nrthire` | `0.0` |
| `pa_project` | `VM1` |
| `pa_pvaimp` | `0.0` |
| `pa_qty` | `0.0` |
| `pa_regctry` | `` |
| `pa_regvat` | `` |
| `pa_sentvat` | `0.0` |
| `pa_setdisc` | `0.0` |
| `pa_ssdfval` | `0.0` |
| `pa_ssdpost` | `0.0` |
| `pa_ssdpre` | `0.0` |
| `pa_ssdsupp` | `0.0` |
| `pa_ssdval` | `0.0` |
| `pa_supanal` | `` |
| `pa_suppqty` | `0.0` |
| `pa_suptype` | `PR` |
| `pa_taxdate` | `2024-05-01T00:00:00` |
| `pa_transac` | `` |
| `pa_transpt` | `` |
| `pa_trdate` | `2024-05-01T00:00:00` |
| `pa_trref` | `p inv` |
| `pa_trtype` | `I` |
| `pa_trvalue` | `100.0` |
| `pa_vatctry` | `H` |
| `pa_vatrate` | `20.0` |
| `pa_vatset1` | `0.0` |
| `pa_vatset2` | `0.0` |
| `pa_vattype` | `P` |
| `pa_vatval` | `20.0` |


#### pname

**1 row(s) modified:**

*Row id=2.0:*
| Field | Before | After |
|-------|--------|-------|
| `pn_currbal` | `88479.78` | `88599.78` |
| `pn_trnover` | `322207.6` | `322307.6` |


#### pnoml

**3 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `px_cdesc` | `` |
| `px_comment` | `Carters Limited` |
| `px_date` | `2024-05-01T00:00:00` |
| `px_done` | `Y` |
| `px_fcdec` | `0.0` |
| `px_fcmult` | `0.0` |
| `px_fcrate` | `0.0` |
| `px_fcurr` | `` |
| `px_fvalue` | `0.0` |
| `px_job` | `VM` |
| `px_jrnl` | `3449.0` |
| `px_nacnt` | `M310` |
| `px_ncntr` | `` |
| `px_nlpdate` | `2024-05-01T00:00:00` |
| `px_project` | `VM1` |
| `px_srcco` | `Z` |
| `px_tref` | `Purchases - Maintenance       p inv` |
| `px_type` | `I` |
| `px_unique` | `_7FL0V1TSB` |
| `px_value` | `100.0` |

*Row 2:*
| Field | Value |
|-------|-------|
| `px_cdesc` | `` |
| `px_comment` | `Carters Limited` |
| `px_date` | `2024-05-01T00:00:00` |
| `px_done` | `Y` |
| `px_fcdec` | `0.0` |
| `px_fcmult` | `0.0` |
| `px_fcrate` | `0.0` |
| `px_fcurr` | `` |
| `px_fvalue` | `0.0` |
| `px_job` | `` |
| `px_jrnl` | `3449.0` |
| `px_nacnt` | `E225` |
| `px_ncntr` | `` |
| `px_nlpdate` | `2024-05-01T00:00:00` |
| `px_project` | `` |
| `px_srcco` | `Z` |
| `px_tref` | `Purchases - Maintenance       p inv` |
| `px_type` | `I` |
| `px_unique` | `_7FL0V1TSB` |
| `px_value` | `20.0` |

*Row 3:*
| Field | Value |
|-------|-------|
| `px_cdesc` | `` |
| `px_comment` | `Carters Limited` |
| `px_date` | `2024-05-01T00:00:00` |
| `px_done` | `Y` |
| `px_fcdec` | `0.0` |
| `px_fcmult` | `0.0` |
| `px_fcrate` | `0.0` |
| `px_fcurr` | `` |
| `px_fvalue` | `0.0` |
| `px_job` | `` |
| `px_jrnl` | `3449.0` |
| `px_nacnt` | `E110` |
| `px_ncntr` | `` |
| `px_nlpdate` | `2024-05-01T00:00:00` |
| `px_project` | `` |
| `px_srcco` | `Z` |
| `px_tref` | `p inv` |
| `px_type` | `I` |
| `px_unique` | `_7FL0V1TSB` |
| `px_value` | `-120.0` |


#### ptran

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `pt_account` | `CAR0001` |
| `pt_adjsv` | `0.0` |
| `pt_adval` | `0.0` |
| `pt_advance` | `N` |
| `pt_apadoc` | `` |
| `pt_cbtype` | `` |
| `pt_crdate` | `2024-05-01T00:00:00` |
| `pt_dueday` | `2024-05-31T00:00:00` |
| `pt_entry` | `` |
| `pt_eurind` | `` |
| `pt_euro` | `0.0` |
| `pt_fadval` | `0.0` |
| `pt_fcbal` | `0.0` |
| `pt_fcdec` | `0.0` |
| `pt_fcmult` | `0.0` |
| `pt_fcrate` | `0.0` |
| `pt_fcurr` | `` |
| `pt_fcval` | `0.0` |
| `pt_fcvat` | `0.0` |
| `pt_held` | `N` |
| `pt_memo` | `Analysis of Invoice p inv                Dated 01/05/2024  NL Posting Date 01...` |
| `pt_nlpdate` | `2024-05-01T00:00:00` |
| `pt_origcur` | `` |
| `pt_paid` | `` |
| `pt_payadvl` | `0.0` |
| `pt_payflag` | `0.0` |
| `pt_plimage` | `` |
| `pt_pyroute` | `0.0` |
| `pt_rcode` | `` |
| `pt_revchrg` | `0.0` |
| `pt_ruser` | `` |
| `pt_set1` | `0.0` |
| `pt_set1day` | `0.0` |
| `pt_set2` | `0.0` |
| `pt_set2day` | `0.0` |
| `pt_supref` | `` |
| `pt_suptype` | `PR` |
| `pt_trbal` | `120.0` |
| `pt_trdate` | `2024-05-01T00:00:00` |
| `pt_trref` | `p inv` |
| `pt_trtype` | `I` |
| `pt_trvalue` | `120.0` |
| `pt_unique` | `_7FL0V1TSB` |
| `pt_vatset1` | `0.0` |
| `pt_vatset2` | `0.0` |
| `pt_vatval` | `20.0` |


#### zcontacts

**2 row(s) modified:**

*Row id=263.0:*
| Field | Before | After |
|-------|--------|-------|

*Row id=264.0:*
| Field | Before | After |
|-------|--------|-------|
| `zc_email` | `sales@com.co.uk` | `orders@com.co.uk` |


---

### Purchase payment bacs

#### aentry

**1 row(s) modified:**

*Row id=11014.0:*
| Field | Before | After |
|-------|--------|-------|
| `ae_entref` | `pay` | `test` |
| `ae_postgrp` | `6.0` | `7.0` |
| `ae_value` | `-13274742.0` | `-13334642.0` |
| `sq_amtime` | `12:58:21` | `14:41:12` |


#### anoml

**2 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `ax_comment` | `Carters Limited               BACS` |
| `ax_date` | `2024-05-01T00:00:00` |
| `ax_done` | `Y` |
| `ax_fcdec` | `0.0` |
| `ax_fcmult` | `0.0` |
| `ax_fcrate` | `0.0` |
| `ax_fcurr` | `` |
| `ax_fvalue` | `0.0` |
| `ax_job` | `` |
| `ax_jrnl` | `3450.0` |
| `ax_nacnt` | `C310` |
| `ax_ncntr` | `` |
| `ax_nlpdate` | `2024-05-01T00:00:00` |
| `ax_project` | `` |
| `ax_source` | `P` |
| `ax_srcco` | `Z` |
| `ax_tref` | `test` |
| `ax_unique` | `_7FL0VGKLX` |
| `ax_value` | `-599.0` |

*Row 2:*
| Field | Value |
|-------|-------|
| `ax_comment` | `Carters Limited               BACS` |
| `ax_date` | `2024-05-01T00:00:00` |
| `ax_done` | `Y` |
| `ax_fcdec` | `0.0` |
| `ax_fcmult` | `0.0` |
| `ax_fcrate` | `0.0` |
| `ax_fcurr` | `` |
| `ax_fvalue` | `0.0` |
| `ax_job` | `` |
| `ax_jrnl` | `3450.0` |
| `ax_nacnt` | `E110` |
| `ax_ncntr` | `` |
| `ax_nlpdate` | `2024-05-01T00:00:00` |
| `ax_project` | `` |
| `ax_source` | `P` |
| `ax_srcco` | `Z` |
| `ax_tref` | `test` |
| `ax_unique` | `_7FL0VGKLX` |
| `ax_value` | `599.0` |


#### atran

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `at_account` | `CAR0001` |
| `at_acnt` | `C310` |
| `at_atpycd` | `` |
| `at_bacprn` | `0.0` |
| `at_bic` | `` |
| `at_bsname` | `` |
| `at_bsref` | `` |
| `at_cash` | `0.0` |
| `at_cbtype` | `P2` |
| `at_ccauth` | `` |
| `at_ccdno` | `` |
| `at_ccdprn` | `0.0` |
| `at_chqlst` | `0.0` |
| `at_chqprn` | `0.0` |
| `at_cntr` | `` |
| `at_comment` | `` |
| `at_disc` | `0.0` |
| `at_ecb` | `0.0` |
| `at_ecbtype` | `` |
| `at_entry` | `P200000427` |
| `at_fcdec` | `2.0` |
| `at_fcexch` | `1.0` |
| `at_fcmult` | `0.0` |
| `at_fcurr` | `` |
| `at_iban` | `` |
| `at_inputby` | `TEST` |
| `at_job` | `` |
| `at_memo` | `` |
| `at_name` | `Carters Limited` |
| `at_number` | `` |
| `at_payee` | `` |
| `at_payname` | `Carters (UK) Limited` |
| `at_payslp` | `0.0` |
| `at_postgrp` | `7.0` |
| `at_project` | `` |
| `at_pstdate` | `2024-05-01T00:00:00` |
| `at_pysprn` | `0.0` |
| `at_refer` | `test` |
| `at_remit` | `0.0` |
| `at_remove` | `0.0` |
| `at_sort` | `` |
| `at_srcco` | `Z` |
| `at_sysdate` | `2024-05-01T00:00:00` |
| `at_tperiod` | `1.0` |
| `at_type` | `5.0` |
| `at_unique` | `_7FL0VGKLX` |
| `at_value` | `-59900.0` |
| `at_vattycd` | `` |


#### dmcomp

**1 row(s) modified:**

*Row id=22.0:*
| Field | Before | After |
|-------|--------|-------|
| `changedon` | `2026-04-01T14:29:26` | `2026-04-01T14:41:14` |


#### idtab

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `id_numericid` | `3450.0` | `3451.0` |


#### nacnt

**2 row(s) modified:**

*Row id=16.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `-27332.95` | `-27931.95` |
| `na_ptdcr` | `27387.95` | `27986.95` |
| `na_ytdcr` | `224206.11` | `224805.11` |

*Row id=28.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `943584.0` | `944183.0` |
| `na_ptddr` | `1099071.36` | `1099670.36` |
| `na_ytddr` | `2941951.31` | `2942550.31` |


#### nbank

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `nk_curbal` | `4793526.0` | `4733626.0` |


#### ndetail

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `nt_acnt` | `C310` |
| `nt_cdesc` | `` |
| `nt_cmnt` | `test` |
| `nt_cntr` | `` |
| `nt_consol` | `0.0` |
| `nt_distrib` | `0.0` |
| `nt_entr` | `2024-05-01T00:00:00` |
| `nt_fcdec` | `0.0` |
| `nt_fcmult` | `0.0` |
| `nt_fcrate` | `0.0` |
| `nt_fcurr` | `` |
| `nt_fvalue` | `0.0` |
| `nt_inp` | `TEST` |
| `nt_job` | `` |
| `nt_jrnl` | `3450.0` |
| `nt_period` | `5.0` |
| `nt_perpost` | `0.0` |
| `nt_posttyp` | `P` |
| `nt_prevyr` | `0.0` |
| `nt_project` | `` |
| `nt_pstgrp` | `1.0` |
| `nt_pstid` | `_7FL0VH8YK` |
| `nt_recjrnl` | `0.0` |
| `nt_rectify` | `0.0` |
| `nt_recurr` | `0.0` |
| `nt_ref` | `` |
| `nt_rvrse` | `0.0` |
| `nt_srcco` | `Z` |
| `nt_subt` | `03` |
| `nt_trnref` | `Carters Limited               BACS` |
| `nt_trtype` | `A` |
| `nt_type` | `10` |
| `nt_value` | `-599.0` |
| `nt_vatanal` | `0.0` |
| `nt_year` | `2024.0` |


#### nextid

**7 row(s) modified:**

*Row id=13.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `26123.0` | `26125.0` |

*Row id=21.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `17746.0` | `17747.0` |

*Row id=229.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `6895.0` | `6896.0` |

*Row id=235.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `214.0` | `215.0` |

*Row id=244.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `106652.0` | `106654.0` |


#### nhist

**2 row(s) modified:**

*Row id=159882.0:*
| Field | Before | After |
|-------|--------|-------|
| `nh_bal` | `-27332.95` | `-27931.95` |
| `nh_ptdcr` | `-27387.95` | `-27986.95` |

*Row id=159886.0:*
| Field | Before | After |
|-------|--------|-------|
| `nh_bal` | `943584.0` | `944183.0` |
| `nh_ptddr` | `1099071.36` | `1099670.36` |


#### njmemo

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `nj_binrep` | `0.0` |
| `nj_image` | `` |
| `nj_journal` | `3450.0` |
| `nj_memo` | `ÿ<<JOURNAL_DATA_ONLY>>ÿ` |
| `nj_txtrep` | `Cashbook Ledger Transfer` |


#### nsubt

**2 row(s) modified:**

*Row id=8.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `2770903.35` | `2770304.35` |

*Row id=10.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `-230367.96` | `-229768.96` |


#### ntype

**2 row(s) modified:**

*Row id=3.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `5115725.67` | `5115126.67` |

*Row id=4.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `-285308.08` | `-284709.08` |


#### palloc

**2 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `al_account` | `CAR0001` |
| `al_acnt` | `C310` |
| `al_adjsv` | `0.0` |
| `al_advind` | `0.0` |
| `al_advtran` | `0.0` |
| `al_bacsid` | `0.0` |
| `al_cheq` | `` |
| `al_cnclchq` | `` |
| `al_cntr` | `` |
| `al_ctype` | `P` |
| `al_date` | `2024-05-03T00:00:00` |
| `al_dval` | `0.0` |
| `al_fcurr` | `` |
| `al_fdec` | `0.0` |
| `al_fdval` | `0.0` |
| `al_forigvl` | `0.0` |
| `al_fval` | `59900.0` |
| `al_origval` | `87923.38` |
| `al_payday` | `2024-05-01T00:00:00` |
| `al_payee` | `` |
| `al_payflag` | `104.0` |
| `al_payind` | `P` |
| `al_preprd` | `0.0` |
| `al_ref1` | `CART-0524-45456M90` |
| `al_ref2` | `` |
| `al_rem` | `` |
| `al_type` | `I` |
| `al_unique` | `4515.0` |
| `al_val` | `599.0` |

*Row 2:*
| Field | Value |
|-------|-------|
| `al_account` | `CAR0001` |
| `al_acnt` | `C310` |
| `al_adjsv` | `0.0` |
| `al_advind` | `0.0` |
| `al_advtran` | `0.0` |
| `al_bacsid` | `0.0` |
| `al_cheq` | `S` |
| `al_cnclchq` | `` |
| `al_cntr` | `` |
| `al_ctype` | `C` |
| `al_date` | `2024-05-01T00:00:00` |
| `al_dval` | `0.0` |
| `al_fcurr` | `` |
| `al_fdec` | `0.0` |
| `al_fdval` | `0.0` |
| `al_forigvl` | `0.0` |
| `al_fval` | `0.0` |
| `al_origval` | `-599.0` |
| `al_payday` | `2024-05-01T00:00:00` |
| `al_payee` | `Carters (UK) Limited` |
| `al_payflag` | `104.0` |
| `al_payind` | `A` |
| `al_preprd` | `0.0` |
| `al_ref1` | `test` |
| `al_ref2` | `BACS` |
| `al_rem` | `` |
| `al_type` | `P` |
| `al_unique` | `4554.0` |
| `al_val` | `-599.0` |


#### pname

**1 row(s) modified:**

*Row id=2.0:*
| Field | Before | After |
|-------|--------|-------|
| `pn_currbal` | `88599.78` | `88000.78` |
| `pn_nextpay` | `104.0` | `105.0` |


#### ptran

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `pt_account` | `CAR0001` |
| `pt_adjsv` | `0.0` |
| `pt_adval` | `0.0` |
| `pt_advance` | `N` |
| `pt_apadoc` | `` |
| `pt_cbtype` | `P2` |
| `pt_crdate` | `2024-05-01T00:00:00` |
| `pt_entry` | `P200000429` |
| `pt_eurind` | `` |
| `pt_euro` | `0.0` |
| `pt_fadval` | `0.0` |
| `pt_fcbal` | `0.0` |
| `pt_fcdec` | `0.0` |
| `pt_fcmult` | `0.0` |
| `pt_fcrate` | `0.0` |
| `pt_fcurr` | `` |
| `pt_fcval` | `0.0` |
| `pt_fcvat` | `0.0` |
| `pt_held` | `` |
| `pt_memo` | `Analysis of Payment test                  Amount       599.00  Dated 01/05/20...` |
| `pt_nlpdate` | `2024-05-01T00:00:00` |
| `pt_origcur` | `` |
| `pt_paid` | `A` |
| `pt_payadvl` | `0.0` |
| `pt_payday` | `2024-05-01T00:00:00` |
| `pt_payflag` | `104.0` |
| `pt_plimage` | `` |
| `pt_pyroute` | `0.0` |
| `pt_rcode` | `` |
| `pt_revchrg` | `0.0` |
| `pt_ruser` | `` |
| `pt_set1` | `0.0` |
| `pt_set1day` | `0.0` |
| `pt_set2` | `0.0` |
| `pt_set2day` | `0.0` |
| `pt_supref` | `BACS` |
| `pt_suptype` | `` |
| `pt_trbal` | `0.0` |
| `pt_trdate` | `2024-05-01T00:00:00` |
| `pt_trref` | `test` |
| `pt_trtype` | `P` |
| `pt_trvalue` | `-599.0` |
| `pt_unique` | `_7FL0VGKLX` |
| `pt_vatset1` | `0.0` |
| `pt_vatset2` | `0.0` |
| `pt_vatval` | `0.0` |

**1 row(s) modified:**

*Row id=4515.0:*
| Field | Before | After |
|-------|--------|-------|
| `pt_lastpay` | `NaT` | `2024-05-03T00:00:00` |
| `pt_paid` | `` | `B` |
| `pt_payday` | `NaT` | `2024-05-01T00:00:00` |
| `pt_payflag` | `0.0` | `104.0` |
| `pt_trbal` | `87923.38` | `87324.38` |


#### zlock

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|


---

### Refund

#### aentry

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `ae_acnt` | `C310` |
| `ae_batchid` | `0.0` |
| `ae_brwptr` | `` |
| `ae_cbtype` | `R4` |
| `ae_cntr` | `` |
| `ae_comment` | `` |
| `ae_complet` | `1.0` |
| `ae_entref` | `test` |
| `ae_entry` | `R400000016` |
| `ae_frstat` | `0.0` |
| `ae_lstdate` | `2024-05-01T00:00:00` |
| `ae_payid` | `0.0` |
| `ae_postgrp` | `0.0` |
| `ae_recbal` | `0.0` |
| `ae_reclnum` | `0.0` |
| `ae_remove` | `0.0` |
| `ae_statln` | `0.0` |
| `ae_tmpstat` | `0.0` |
| `ae_tostat` | `0.0` |
| `ae_value` | `5000.0` |
| `sq_amtime` | `` |
| `sq_amuser` | `` |
| `sq_crdate` | `2026-04-01T00:00:00` |
| `sq_crtime` | `13:19:34` |
| `sq_cruser` | `TEST` |


#### anoml

**2 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `ax_comment` | `Crown Venue Catering Ltd.     Refund` |
| `ax_date` | `2024-05-01T00:00:00` |
| `ax_done` | `` |
| `ax_fcdec` | `0.0` |
| `ax_fcmult` | `0.0` |
| `ax_fcrate` | `0.0` |
| `ax_fcurr` | `` |
| `ax_fvalue` | `0.0` |
| `ax_job` | `` |
| `ax_jrnl` | `0.0` |
| `ax_nacnt` | `C310` |
| `ax_ncntr` | `` |
| `ax_nlpdate` | `2024-05-01T00:00:00` |
| `ax_project` | `` |
| `ax_source` | `P` |
| `ax_srcco` | `Z` |
| `ax_tref` | `test` |
| `ax_unique` | `_7FL0SK9JK` |
| `ax_value` | `50.0` |

*Row 2:*
| Field | Value |
|-------|-------|
| `ax_comment` | `Crown Venue Catering Ltd.     Refund` |
| `ax_date` | `2024-05-01T00:00:00` |
| `ax_done` | `` |
| `ax_fcdec` | `0.0` |
| `ax_fcmult` | `0.0` |
| `ax_fcrate` | `0.0` |
| `ax_fcurr` | `` |
| `ax_fvalue` | `0.0` |
| `ax_job` | `` |
| `ax_jrnl` | `0.0` |
| `ax_nacnt` | `E110` |
| `ax_ncntr` | `` |
| `ax_nlpdate` | `2024-05-01T00:00:00` |
| `ax_project` | `` |
| `ax_source` | `P` |
| `ax_srcco` | `Z` |
| `ax_tref` | `test` |
| `ax_unique` | `_7FL0SK9JK` |
| `ax_value` | `-50.0` |


#### atran

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `at_account` | `CVC0001` |
| `at_acnt` | `C310` |
| `at_atpycd` | `` |
| `at_bacprn` | `0.0` |
| `at_bic` | `` |
| `at_bsname` | `` |
| `at_bsref` | `` |
| `at_cash` | `0.0` |
| `at_cbtype` | `R4` |
| `at_ccauth` | `` |
| `at_ccdno` | `` |
| `at_ccdprn` | `0.0` |
| `at_chqlst` | `0.0` |
| `at_chqprn` | `0.0` |
| `at_cntr` | `` |
| `at_comment` | `` |
| `at_disc` | `0.0` |
| `at_ecb` | `0.0` |
| `at_ecbtype` | `` |
| `at_entry` | `R400000016` |
| `at_fcdec` | `2.0` |
| `at_fcexch` | `1.0` |
| `at_fcmult` | `0.0` |
| `at_fcurr` | `` |
| `at_iban` | `` |
| `at_inputby` | `TEST` |
| `at_job` | `` |
| `at_memo` | `` |
| `at_name` | `Crown Venue Catering Ltd.` |
| `at_number` | `` |
| `at_payee` | `` |
| `at_payname` | `Crown Venue Catering Ltd.` |
| `at_payslp` | `0.0` |
| `at_postgrp` | `0.0` |
| `at_project` | `` |
| `at_pstdate` | `2024-05-01T00:00:00` |
| `at_pysprn` | `0.0` |
| `at_refer` | `test` |
| `at_remit` | `0.0` |
| `at_remove` | `0.0` |
| `at_sort` | `` |
| `at_srcco` | `Z` |
| `at_sysdate` | `2024-05-01T00:00:00` |
| `at_tperiod` | `1.0` |
| `at_type` | `6.0` |
| `at_unique` | `_7FL0SK9JK` |
| `at_value` | `5000.0` |
| `at_vattycd` | `` |


#### atype

**1 row(s) modified:**

*Row id=12.0:*
| Field | Before | After |
|-------|--------|-------|
| `ay_entry` | `R400000016` | `R400000017` |


#### nbank

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `nk_curbal` | `4807026.0` | `4812026.0` |


#### nextid

**5 row(s) modified:**

*Row id=10.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `11053.0` | `11054.0` |

*Row id=13.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `26112.0` | `26114.0` |

*Row id=21.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `17738.0` | `17739.0` |

*Row id=248.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `841.0` | `842.0` |

*Row id=267.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `4552.0` | `4553.0` |


#### palloc

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `al_account` | `CVC0001` |
| `al_acnt` | `C310` |
| `al_adjsv` | `0.0` |
| `al_advind` | `0.0` |
| `al_advtran` | `0.0` |
| `al_bacsid` | `0.0` |
| `al_cheq` | `` |
| `al_cnclchq` | `` |
| `al_cntr` | `` |
| `al_ctype` | `O` |
| `al_date` | `2024-05-01T00:00:00` |
| `al_dval` | `0.0` |
| `al_fcurr` | `` |
| `al_fdec` | `0.0` |
| `al_fdval` | `0.0` |
| `al_forigvl` | `0.0` |
| `al_fval` | `0.0` |
| `al_origval` | `50.0` |
| `al_payday` | `2024-05-01T00:00:00` |
| `al_payee` | `Crown Venue Catering Ltd.` |
| `al_payflag` | `0.0` |
| `al_payind` | `P` |
| `al_preprd` | `0.0` |
| `al_ref1` | `test` |
| `al_ref2` | `Refund` |
| `al_rem` | `` |
| `al_type` | `F` |
| `al_unique` | `4552.0` |
| `al_val` | `50.0` |


#### pname

**1 row(s) modified:**

*Row id=44.0:*
| Field | Before | After |
|-------|--------|-------|
| `pn_currbal` | `0.0` | `50.0` |


#### ptran

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `pt_account` | `CVC0001` |
| `pt_adjsv` | `0.0` |
| `pt_adval` | `0.0` |
| `pt_advance` | `N` |
| `pt_apadoc` | `` |
| `pt_cbtype` | `R4` |
| `pt_crdate` | `2024-05-01T00:00:00` |
| `pt_entry` | `R400000016` |
| `pt_eurind` | `` |
| `pt_euro` | `0.0` |
| `pt_fadval` | `0.0` |
| `pt_fcbal` | `0.0` |
| `pt_fcdec` | `0.0` |
| `pt_fcmult` | `0.0` |
| `pt_fcrate` | `0.0` |
| `pt_fcurr` | `` |
| `pt_fcval` | `0.0` |
| `pt_fcvat` | `0.0` |
| `pt_held` | `` |
| `pt_memo` | `` |
| `pt_nlpdate` | `2024-05-01T00:00:00` |
| `pt_origcur` | `` |
| `pt_paid` | `` |
| `pt_payadvl` | `0.0` |
| `pt_payflag` | `0.0` |
| `pt_plimage` | `` |
| `pt_pyroute` | `0.0` |
| `pt_rcode` | `` |
| `pt_revchrg` | `0.0` |
| `pt_ruser` | `` |
| `pt_set1` | `0.0` |
| `pt_set1day` | `0.0` |
| `pt_set2` | `0.0` |
| `pt_set2day` | `0.0` |
| `pt_supref` | `Refund` |
| `pt_suptype` | `` |
| `pt_trbal` | `50.0` |
| `pt_trdate` | `2024-05-01T00:00:00` |
| `pt_trref` | `test` |
| `pt_trtype` | `F` |
| `pt_trvalue` | `50.0` |
| `pt_unique` | `_7FL0SK9JK` |
| `pt_vatset1` | `0.0` |
| `pt_vatset2` | `0.0` |
| `pt_vatval` | `0.0` |


#### zlock

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|


---

## Sales Ledger Transactions

### Adjustment

#### dmaddr

**1 row(s) modified:**

*Row id=20.0:*
| Field | Before | After |
|-------|--------|-------|


#### dmcomp

**1 row(s) modified:**

*Row id=3.0:*
| Field | Before | After |
|-------|--------|-------|
| `changedon` | `2022-10-03T10:53:29` | `2026-04-06T20:48:27` |
| `compnotes` | `NEW SERVER 10/05/18\\FS01\O3 Server...` | `NEW SERVER 10/05/18\\FS01\O3 Server...` |
| `email` | `ryan@hellenist.org.uk` | `CarlaN@hellenist.org.uk` |


#### idtab

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `id_numericid` | `48375.0` | `48376.0` |


#### nacnt

**2 row(s) modified:**

*Row id=5.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc03` | `-25277.07` | `-25177.07` |
| `na_ptddr` | `203116.64` | `203216.64` |
| `na_ytddr` | `340093.25` | `340193.25` |

*Row id=69.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc03` | `2080.8` | `1980.8` |
| `na_ptdcr` | `0.0` | `100.0` |
| `na_ytdcr` | `0.0` | `100.0` |


#### nextid

**4 row(s) modified:**

*Row id=111.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `31586.0` | `31587.0` |

*Row id=120.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `146236.0` | `146238.0` |

*Row id=149.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `6259.0` | `6260.0` |

*Row id=154.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `33096.0` | `33097.0` |


#### nsubt

**2 row(s) modified:**

*Row id=3.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `180341.54` | `180441.54` |

*Row id=7.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `120.8` | `20.8` |


#### ntype

**2 row(s) modified:**

*Row id=2.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `2429101.21` | `2429201.21` |

*Row id=4.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `-21297.48` | `-21397.48` |


#### sname

**1 row(s) modified:**

*Row id=10.0:*
| Field | Before | After |
|-------|--------|-------|
| `sn_currbal` | `0.0` | `100.0` |


#### snoml

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `sx_cdesc` | `` |
| `sx_comment` | `The Athenaeum                 Contra` |
| `sx_date` | `2026-03-31T00:00:00` |
| `sx_done` | `Y` |
| `sx_fcdec` | `0.0` |
| `sx_fcmult` | `0.0` |
| `sx_fcrate` | `0.0` |
| `sx_fcurr` | `` |
| `sx_fvalue` | `0.0` |
| `sx_job` | `` |
| `sx_jrnl` | `48375.0` |
| `sx_nacnt` | `DB020` |
| `sx_ncntr` | `` |
| `sx_nlpdate` | `2026-03-31T00:00:00` |
| `sx_project` | `` |
| `sx_srcco` | `I` |
| `sx_tref` | `test                          adjust` |
| `sx_type` | `A` |
| `sx_unique` | `_7FQ18LFAN` |
| `sx_value` | `100.0` |


#### zcontacts

**2 row(s) modified:**

*Row id=3073.0:*
| Field | Before | After |
|-------|--------|-------|

*Row id=4776.0:*
| Field | Before | After |
|-------|--------|-------|


---

### Sales Allocation

#### dmcomp

**1 row(s) modified:**

*Row id=2.0:*
| Field | Before | After |
|-------|--------|-------|
| `changedon` | `2026-04-01T14:15:46` | `2026-04-01T14:20:08` |


#### dmcont

**5 row(s) modified:**

*Row id=18.0:*
| Field | Before | After |
|-------|--------|-------|

*Row id=19.0:*
| Field | Before | After |
|-------|--------|-------|

*Row id=20.0:*
| Field | Before | After |
|-------|--------|-------|

*Row id=249.0:*
| Field | Before | After |
|-------|--------|-------|

*Row id=260.0:*
| Field | Before | After |
|-------|--------|-------|


#### nextid

**1 row(s) modified:**

*Row id=269.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `1748.0` | `1752.0` |


#### salloc

**4 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `al_account` | `ADA0001` |
| `al_acnt` | `C310` |
| `al_adjsv` | `0.0` |
| `al_advind` | `0.0` |
| `al_cntr` | `` |
| `al_date` | `2024-01-31T00:00:00` |
| `al_fcurr` | `` |
| `al_fdec` | `0.0` |
| `al_fval` | `0.0` |
| `al_payday` | `2024-05-01T00:00:00` |
| `al_payflag` | `92.0` |
| `al_payind` | `A` |
| `al_preprd` | `0.0` |
| `al_ref1` | `REC-ADA-39393E` |
| `al_ref2` | `BACS` |
| `al_type` | `R` |
| `al_unique` | `9068.0` |
| `al_val` | `-10000.0` |

*Row 2:*
| Field | Value |
|-------|-------|
| `al_account` | `ADA0001` |
| `al_acnt` | `C310` |
| `al_adjsv` | `0.0` |
| `al_advind` | `0.0` |
| `al_cntr` | `` |
| `al_date` | `2024-03-31T00:00:00` |
| `al_fcurr` | `` |
| `al_fdec` | `0.0` |
| `al_fval` | `1000000.0` |
| `al_payday` | `2024-05-01T00:00:00` |
| `al_payflag` | `92.0` |
| `al_payind` | `P` |
| `al_preprd` | `0.0` |
| `al_ref1` | `INV05178` |
| `al_ref2` | `*CONSOLID*` |
| `al_type` | `I` |
| `al_unique` | `9188.0` |
| `al_val` | `10000.0` |

*Row 3:*
| Field | Value |
|-------|-------|
| `al_account` | `ADA0001` |
| `al_acnt` | `C310` |
| `al_adjsv` | `0.0` |
| `al_advind` | `0.0` |
| `al_cntr` | `` |
| `al_date` | `2024-05-01T00:00:00` |
| `al_fcurr` | `` |
| `al_fdec` | `0.0` |
| `al_fval` | `0.0` |
| `al_payday` | `2024-05-01T00:00:00` |
| `al_payflag` | `92.0` |
| `al_payind` | `A` |
| `al_preprd` | `0.0` |
| `al_ref1` | `re1` |
| `al_ref2` | `ref2` |
| `al_type` | `I` |
| `al_unique` | `9283.0` |
| `al_val` | `120.0` |

*Row 4:*
| Field | Value |
|-------|-------|
| `al_account` | `ADA0001` |
| `al_acnt` | `C310` |
| `al_adjsv` | `0.0` |
| `al_advind` | `0.0` |
| `al_cntr` | `` |
| `al_date` | `2024-05-01T00:00:00` |
| `al_fcurr` | `` |
| `al_fdec` | `0.0` |
| `al_fval` | `0.0` |
| `al_payday` | `2024-05-01T00:00:00` |
| `al_payflag` | `92.0` |
| `al_payind` | `A` |
| `al_preprd` | `0.0` |
| `al_ref1` | `test` |
| `al_ref2` | `test` |
| `al_type` | `C` |
| `al_unique` | `9284.0` |
| `al_val` | `-120.0` |


#### sname

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `sn_lupdate` | `2023-01-22T00:00:00` | `2026-04-01T00:00:00` |
| `sn_nextpay` | `92.0` | `93.0` |


#### stran

**4 row(s) modified:**

*Row id=9283.0:*
| Field | Before | After |
|-------|--------|-------|
| `st_lastrec` | `NaT` | `2024-05-01T00:00:00` |
| `st_paid` | `` | `P` |
| `st_payday` | `NaT` | `2024-05-01T00:00:00` |
| `st_payflag` | `0.0` | `92.0` |
| `st_trbal` | `120.0` | `0.0` |

*Row id=9068.0:*
| Field | Before | After |
|-------|--------|-------|
| `st_paid` | `` | `A` |
| `st_payday` | `NaT` | `2024-05-01T00:00:00` |
| `st_payflag` | `0.0` | `92.0` |
| `st_trbal` | `-10000.0` | `0.0` |

*Row id=9188.0:*
| Field | Before | After |
|-------|--------|-------|
| `st_lastrec` | `NaT` | `2024-03-31T00:00:00` |
| `st_paid` | `` | `B` |
| `st_payday` | `NaT` | `2024-05-01T00:00:00` |
| `st_payflag` | `0.0` | `92.0` |
| `st_trbal` | `11434.63` | `1434.63` |

*Row id=9284.0:*
| Field | Before | After |
|-------|--------|-------|
| `st_paid` | `` | `A` |
| `st_payday` | `NaT` | `2024-05-01T00:00:00` |
| `st_payflag` | `0.0` | `92.0` |
| `st_trbal` | `-120.0` | `0.0` |


---

### Sales Credit Note

#### dmcomp

**1 row(s) modified:**

*Row id=2.0:*
| Field | Before | After |
|-------|--------|-------|
| `changedon` | `2026-04-01T13:49:10` | `2026-04-01T14:15:46` |


#### dmcont

**5 row(s) modified:**

*Row id=18.0:*
| Field | Before | After |
|-------|--------|-------|

*Row id=19.0:*
| Field | Before | After |
|-------|--------|-------|

*Row id=20.0:*
| Field | Before | After |
|-------|--------|-------|

*Row id=249.0:*
| Field | Before | After |
|-------|--------|-------|

*Row id=260.0:*
| Field | Before | After |
|-------|--------|-------|


#### idtab

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `id_numericid` | `3447.0` | `3448.0` |


#### nacnt

**3 row(s) modified:**

*Row id=12.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `20941.73` | `20821.73` |
| `na_ptdcr` | `2297.98` | `2417.98` |
| `na_ytdcr` | `1563005.02` | `1563125.02` |

*Row id=32.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `-41265.2` | `-41245.2` |
| `na_ptddr` | `0.0` | `20.0` |
| `na_ytddr` | `0.0` | `20.0` |

*Row id=286.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `0.0` | `100.0` |
| `na_ptddr` | `0.0` | `100.0` |
| `na_ytddr` | `0.0` | `100.0` |


#### nextid

**6 row(s) modified:**

*Row id=234.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `159920.0` | `159921.0` |

*Row id=235.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `211.0` | `212.0` |

*Row id=244.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `106643.0` | `106646.0` |

*Row id=270.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `9327.0` | `9328.0` |

*Row id=280.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `18623.0` | `18625.0` |


#### nhist

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `nh_bal` | `100.0` |
| `nh_budg` | `0.0` |
| `nh_fbal` | `0.0` |
| `nh_job` | `` |
| `nh_nacnt` | `K126` |
| `nh_ncntr` | `` |
| `nh_nsubt` | `03` |
| `nh_ntype` | `30` |
| `nh_period` | `5.0` |
| `nh_project` | `` |
| `nh_ptdcr` | `0.0` |
| `nh_ptddr` | `100.0` |
| `nh_rbudg` | `0.0` |
| `nh_rectype` | `1.0` |
| `nh_year` | `2024.0` |

**2 row(s) modified:**

*Row id=159881.0:*
| Field | Before | After |
|-------|--------|-------|
| `nh_bal` | `20941.73` | `20821.73` |
| `nh_ptdcr` | `-2297.98` | `-2417.98` |

*Row id=159887.0:*
| Field | Before | After |
|-------|--------|-------|
| `nh_bal` | `-41265.2` | `-41245.2` |
| `nh_ptddr` | `0.0` | `20.0` |


#### njmemo

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `nj_binrep` | `0.0` |
| `nj_image` | `` |
| `nj_journal` | `3447.0` |
| `nj_memo` | `ÿ<<JOURNAL_DATA_ONLY>>ÿ` |
| `nj_txtrep` | `Sales Ledger Transfer` |


#### nsubt

**3 row(s) modified:**

*Row id=6.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `432833.47` | `432713.47` |

*Row id=11.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `-54896.79` | `-54876.79` |

*Row id=25.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `-873.95` | `-773.95` |


#### ntype

**3 row(s) modified:**

*Row id=3.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `5115845.67` | `5115725.67` |

*Row id=4.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `-285144.75` | `-285124.75` |

*Row id=7.0:*
| Field | Before | After |
|-------|--------|-------|
| `nt_bal` | `-1462343.81` | `-1462243.81` |


#### sanal

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `sa_account` | `ADA0001` |
| `sa_adjsv` | `0.0` |
| `sa_advance` | `N` |
| `sa_ancode` | `CWASH001` |
| `sa_anvat` | `1` |
| `sa_box1` | `1.0` |
| `sa_box6` | `1.0` |
| `sa_box8` | `0.0` |
| `sa_commod` | `` |
| `sa_cost` | `-30.0` |
| `sa_country` | `GB` |
| `sa_crdate` | `2024-05-01T00:00:00` |
| `sa_ctryorg` | `` |
| `sa_cusanal` | `` |
| `sa_custype` | `CPT` |
| `sa_daccnt` | `ADA0001` |
| `sa_delterm` | `` |
| `sa_desc` | `Car Wash/Valet Contracts` |
| `sa_discost` | `0.0` |
| `sa_domrc` | `0.0` |
| `sa_eslproc` | `0.0` |
| `sa_eslsupp` | `0.0` |
| `sa_esltrig` | `0.0` |
| `sa_exten` | `` |
| `sa_fccost` | `0.0` |
| `sa_fcdec` | `2.0` |
| `sa_fcurr` | `` |
| `sa_fcval` | `0.0` |
| `sa_fcvat` | `0.0` |
| `sa_interbr` | `0.0` |
| `sa_jccode` | `` |
| `sa_jcstdoc` | `` |
| `sa_jline` | `` |
| `sa_job` | `` |
| `sa_jphase` | `` |
| `sa_netmass` | `0.0` |
| `sa_nrthire` | `0.0` |
| `sa_product` | `` |
| `sa_project` | `` |
| `sa_qty` | `0.0` |
| `sa_regctry` | `` |
| `sa_region` | `NE` |
| `sa_regvat` | `` |
| `sa_sentvat` | `0.0` |
| `sa_serv` | `0.0` |
| `sa_setdisc` | `0.0` |
| `sa_ssdfval` | `0.0` |
| `sa_ssdpost` | `0.0` |
| `sa_ssdpre` | `0.0` |
| `sa_ssdsupp` | `0.0` |
| `sa_ssdval` | `0.0` |
| `sa_suppqty` | `0.0` |
| `sa_taxdate` | `2024-05-01T00:00:00` |
| `sa_terr` | `EX1` |
| `sa_transac` | `` |
| `sa_transpt` | `` |
| `sa_trdate` | `2024-05-01T00:00:00` |
| `sa_trref` | `test` |
| `sa_trtype` | `C` |
| `sa_trvalue` | `-100.0` |
| `sa_vatctry` | `H` |
| `sa_vatrate` | `20.0` |
| `sa_vattype` | `S` |
| `sa_vatval` | `-20.0` |


#### sname

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `sn_currbal` | `16852.18` | `16732.18` |
| `sn_trnover` | `12874.81` | `12774.81` |


#### snoml

**2 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `sx_cdesc` | `` |
| `sx_comment` | `Adams Light Engineering Ltd   test` |
| `sx_date` | `2024-05-01T00:00:00` |
| `sx_done` | `Y` |
| `sx_fcdec` | `0.0` |
| `sx_fcmult` | `0.0` |
| `sx_fcrate` | `0.0` |
| `sx_fcurr` | `` |
| `sx_fvalue` | `0.0` |
| `sx_job` | `` |
| `sx_jrnl` | `3447.0` |
| `sx_nacnt` | `K126` |
| `sx_ncntr` | `` |
| `sx_nlpdate` | `2024-05-01T00:00:00` |
| `sx_project` | `` |
| `sx_srcco` | `Z` |
| `sx_tref` | `Car Wash/Valet Contracts      test` |
| `sx_type` | `C` |
| `sx_unique` | `_7FL0UK3NK` |
| `sx_value` | `-100.0` |

*Row 2:*
| Field | Value |
|-------|-------|
| `sx_cdesc` | `` |
| `sx_comment` | `Adams Light Engineering Ltd   test` |
| `sx_date` | `2024-05-01T00:00:00` |
| `sx_done` | `Y` |
| `sx_fcdec` | `0.0` |
| `sx_fcmult` | `0.0` |
| `sx_fcrate` | `0.0` |
| `sx_fcurr` | `` |
| `sx_fvalue` | `0.0` |
| `sx_job` | `` |
| `sx_jrnl` | `3447.0` |
| `sx_nacnt` | `E220` |
| `sx_ncntr` | `` |
| `sx_nlpdate` | `2024-05-01T00:00:00` |
| `sx_project` | `` |
| `sx_srcco` | `Z` |
| `sx_tref` | `Car Wash/Valet Contracts      test` |
| `sx_type` | `C` |
| `sx_unique` | `_7FL0UK3NK` |
| `sx_value` | `-20.0` |


#### stran

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `jxrenewal` | `0.0` |
| `jxservid` | `0.0` |
| `st_account` | `ADA0001` |
| `st_adjsv` | `0.0` |
| `st_advallc` | `0.0` |
| `st_advance` | `N` |
| `st_binrep` | `0.0` |
| `st_cash` | `0.0` |
| `st_cbtype` | `` |
| `st_crdate` | `2024-05-01T00:00:00` |
| `st_custref` | `test` |
| `st_delacc` | `ADA0001` |
| `st_dispute` | `0.0` |
| `st_edi` | `0.0` |
| `st_editx` | `0.0` |
| `st_edivn` | `0.0` |
| `st_entry` | `` |
| `st_eurind` | `` |
| `st_euro` | `0.0` |
| `st_exttime` | `` |
| `st_fadval` | `0.0` |
| `st_fcbal` | `0.0` |
| `st_fcdec` | `0.0` |
| `st_fcmult` | `0.0` |
| `st_fcrate` | `0.0` |
| `st_fcurr` | `` |
| `st_fcval` | `0.0` |
| `st_fcvat` | `0.0` |
| `st_fullamt` | `0.0` |
| `st_fullcb` | `` |
| `st_fullnar` | `` |
| `st_gateid` | `0.0` |
| `st_gatetr` | `0.0` |
| `st_luptime` | `` |
| `st_memo` | `Analysis of Cr.Note test                 Dated 01/05/2024  NL Posting Date 01...` |
| `st_nlpdate` | `2024-05-01T00:00:00` |
| `st_origcur` | `` |
| `st_paid` | `` |
| `st_payadvl` | `0.0` |
| `st_payflag` | `0.0` |
| `st_rcode` | `` |
| `st_region` | `NE` |
| `st_revchrg` | `0.0` |
| `st_ruser` | `` |
| `st_set1` | `0.0` |
| `st_set1day` | `0.0` |
| `st_set2` | `0.0` |
| `st_set2day` | `0.0` |
| `st_taxpoin` | `2024-05-01T00:00:00` |
| `st_terr` | `EX1` |
| `st_trbal` | `-120.0` |
| `st_trdate` | `2024-05-01T00:00:00` |
| `st_trref` | `test` |
| `st_trtype` | `C` |
| `st_trvalue` | `-120.0` |
| `st_txtrep` | `` |
| `st_type` | `CPT` |
| `st_unique` | `_7FL0UK3NK` |
| `st_vatval` | `-20.0` |


#### zcontacts

**2 row(s) modified:**

*Row id=188.0:*
| Field | Before | After |
|-------|--------|-------|

*Row id=189.0:*
| Field | Before | After |
|-------|--------|-------|


---

### Sales Invoice

#### dmcomp

**1 row(s) modified:**

*Row id=2.0:*
| Field | Before | After |
|-------|--------|-------|
| `changedon` | `2024-01-24T16:55:02` | `2026-04-01T13:49:10` |


#### dmcont

**5 row(s) modified:**

*Row id=18.0:*
| Field | Before | After |
|-------|--------|-------|

*Row id=19.0:*
| Field | Before | After |
|-------|--------|-------|

*Row id=20.0:*
| Field | Before | After |
|-------|--------|-------|

*Row id=249.0:*
| Field | Before | After |
|-------|--------|-------|

*Row id=260.0:*
| Field | Before | After |
|-------|--------|-------|


#### nextid

**3 row(s) modified:**

*Row id=270.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `9326.0` | `9327.0` |

*Row id=280.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `18621.0` | `18623.0` |

*Row id=286.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `9283.0` | `9284.0` |


#### sanal

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `sa_account` | `ADA0001` |
| `sa_adjsv` | `0.0` |
| `sa_advance` | `N` |
| `sa_ancode` | `ACCE01` |
| `sa_anvat` | `1` |
| `sa_box1` | `1.0` |
| `sa_box6` | `1.0` |
| `sa_box8` | `0.0` |
| `sa_commod` | `` |
| `sa_cost` | `50.0` |
| `sa_country` | `GB` |
| `sa_crdate` | `2024-05-01T00:00:00` |
| `sa_ctryorg` | `` |
| `sa_cusanal` | `SAL` |
| `sa_custype` | `CPT` |
| `sa_daccnt` | `ADA0001` |
| `sa_delterm` | `` |
| `sa_desc` | `Lease - Accessories` |
| `sa_discost` | `0.0` |
| `sa_domrc` | `0.0` |
| `sa_eslproc` | `0.0` |
| `sa_eslsupp` | `0.0` |
| `sa_esltrig` | `0.0` |
| `sa_exten` | `` |
| `sa_fccost` | `0.0` |
| `sa_fcdec` | `2.0` |
| `sa_fcurr` | `` |
| `sa_fcval` | `0.0` |
| `sa_fcvat` | `0.0` |
| `sa_interbr` | `0.0` |
| `sa_jccode` | `` |
| `sa_jcstdoc` | `` |
| `sa_jline` | `` |
| `sa_job` | `` |
| `sa_jphase` | `` |
| `sa_netmass` | `0.0` |
| `sa_nrthire` | `0.0` |
| `sa_product` | `` |
| `sa_project` | `` |
| `sa_qty` | `0.0` |
| `sa_regctry` | `` |
| `sa_region` | `NE` |
| `sa_regvat` | `` |
| `sa_sentvat` | `0.0` |
| `sa_serv` | `0.0` |
| `sa_setdisc` | `0.0` |
| `sa_ssdfval` | `0.0` |
| `sa_ssdpost` | `0.0` |
| `sa_ssdpre` | `0.0` |
| `sa_ssdsupp` | `0.0` |
| `sa_ssdval` | `0.0` |
| `sa_suppqty` | `0.0` |
| `sa_taxdate` | `2024-05-01T00:00:00` |
| `sa_terr` | `EX1` |
| `sa_transac` | `` |
| `sa_transpt` | `` |
| `sa_trdate` | `2024-05-01T00:00:00` |
| `sa_trref` | `re1` |
| `sa_trtype` | `I` |
| `sa_trvalue` | `100.0` |
| `sa_vatctry` | `H` |
| `sa_vatrate` | `20.0` |
| `sa_vattype` | `S` |
| `sa_vatval` | `20.0` |


#### sname

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `sn_currbal` | `16732.18` | `16852.18` |
| `sn_trnover` | `12774.81` | `12874.81` |


#### snoml

**2 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `sx_cdesc` | `` |
| `sx_comment` | `Adams Light Engineering Ltd   ref2` |
| `sx_date` | `2024-05-01T00:00:00` |
| `sx_done` | `` |
| `sx_fcdec` | `0.0` |
| `sx_fcmult` | `0.0` |
| `sx_fcrate` | `0.0` |
| `sx_fcurr` | `` |
| `sx_fvalue` | `0.0` |
| `sx_job` | `` |
| `sx_jrnl` | `0.0` |
| `sx_nacnt` | `K110` |
| `sx_ncntr` | `SAL` |
| `sx_nlpdate` | `2024-05-01T00:00:00` |
| `sx_project` | `` |
| `sx_srcco` | `Z` |
| `sx_tref` | `Lease - Accessories           re1` |
| `sx_type` | `I` |
| `sx_unique` | `_7FL0TM90Z` |
| `sx_value` | `100.0` |

*Row 2:*
| Field | Value |
|-------|-------|
| `sx_cdesc` | `` |
| `sx_comment` | `Adams Light Engineering Ltd   ref2` |
| `sx_date` | `2024-05-01T00:00:00` |
| `sx_done` | `` |
| `sx_fcdec` | `0.0` |
| `sx_fcmult` | `0.0` |
| `sx_fcrate` | `0.0` |
| `sx_fcurr` | `` |
| `sx_fvalue` | `0.0` |
| `sx_job` | `` |
| `sx_jrnl` | `0.0` |
| `sx_nacnt` | `E220` |
| `sx_ncntr` | `` |
| `sx_nlpdate` | `2024-05-01T00:00:00` |
| `sx_project` | `` |
| `sx_srcco` | `Z` |
| `sx_tref` | `Lease - Accessories           re1` |
| `sx_type` | `I` |
| `sx_unique` | `_7FL0TM90Z` |
| `sx_value` | `20.0` |


#### stran

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `jxrenewal` | `0.0` |
| `jxservid` | `0.0` |
| `st_account` | `ADA0001` |
| `st_adjsv` | `0.0` |
| `st_advallc` | `0.0` |
| `st_advance` | `N` |
| `st_binrep` | `0.0` |
| `st_cash` | `0.0` |
| `st_cbtype` | `` |
| `st_crdate` | `2024-05-01T00:00:00` |
| `st_custref` | `ref2` |
| `st_delacc` | `ADA0001` |
| `st_dispute` | `0.0` |
| `st_dueday` | `2024-06-15T00:00:00` |
| `st_edi` | `0.0` |
| `st_editx` | `0.0` |
| `st_edivn` | `0.0` |
| `st_entry` | `` |
| `st_eurind` | `` |
| `st_euro` | `0.0` |
| `st_exttime` | `` |
| `st_fadval` | `0.0` |
| `st_fcbal` | `0.0` |
| `st_fcdec` | `0.0` |
| `st_fcmult` | `0.0` |
| `st_fcrate` | `0.0` |
| `st_fcurr` | `` |
| `st_fcval` | `0.0` |
| `st_fcvat` | `0.0` |
| `st_fullamt` | `0.0` |
| `st_fullcb` | `` |
| `st_fullnar` | `` |
| `st_gateid` | `0.0` |
| `st_gatetr` | `0.0` |
| `st_luptime` | `` |
| `st_memo` | `Analysis of Invoice re1                  Dated 01/05/2024  NL Posting Date 01...` |
| `st_nlpdate` | `2024-05-01T00:00:00` |
| `st_origcur` | `` |
| `st_paid` | `` |
| `st_payadvl` | `0.0` |
| `st_payflag` | `0.0` |
| `st_rcode` | `` |
| `st_region` | `NE` |
| `st_revchrg` | `0.0` |
| `st_ruser` | `` |
| `st_set1` | `0.0` |
| `st_set1day` | `0.0` |
| `st_set2` | `0.0` |
| `st_set2day` | `0.0` |
| `st_taxpoin` | `2024-05-01T00:00:00` |
| `st_terr` | `EX1` |
| `st_trbal` | `120.0` |
| `st_trdate` | `2024-05-01T00:00:00` |
| `st_trref` | `re1` |
| `st_trtype` | `I` |
| `st_trvalue` | `120.0` |
| `st_txtrep` | `` |
| `st_type` | `CPT` |
| `st_unique` | `_7FL0TM90Z` |
| `st_vatval` | `20.0` |

**1630 row(s) modified:**

*Row id=5731.0:*
| Field | Before | After |
|-------|--------|-------|
| `st_taxpoin` | `None` | `NaT` |

*Row id=5732.0:*
| Field | Before | After |
|-------|--------|-------|
| `st_taxpoin` | `None` | `NaT` |

*Row id=5772.0:*
| Field | Before | After |
|-------|--------|-------|
| `st_taxpoin` | `None` | `NaT` |

*Row id=5773.0:*
| Field | Before | After |
|-------|--------|-------|
| `st_taxpoin` | `None` | `NaT` |

*Row id=5817.0:*
| Field | Before | After |
|-------|--------|-------|
| `st_taxpoin` | `None` | `NaT` |


#### zcontacts

**2 row(s) modified:**

*Row id=188.0:*
| Field | Before | After |
|-------|--------|-------|

*Row id=189.0:*
| Field | Before | After |
|-------|--------|-------|


---

### Sales receipt without  allocation

#### aentry

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `ae_acnt` | `C310` |
| `ae_batchid` | `0.0` |
| `ae_brwptr` | `` |
| `ae_cbtype` | `R2` |
| `ae_cntr` | `` |
| `ae_comment` | `` |
| `ae_complet` | `1.0` |
| `ae_entref` | `test` |
| `ae_entry` | `R200000719` |
| `ae_frstat` | `0.0` |
| `ae_lstdate` | `2024-05-01T00:00:00` |
| `ae_payid` | `0.0` |
| `ae_postgrp` | `0.0` |
| `ae_recbal` | `0.0` |
| `ae_reclnum` | `0.0` |
| `ae_remove` | `0.0` |
| `ae_statln` | `0.0` |
| `ae_tmpstat` | `0.0` |
| `ae_tostat` | `0.0` |
| `ae_value` | `50000.0` |
| `sq_amtime` | `` |
| `sq_amuser` | `` |
| `sq_crdate` | `2026-04-01T00:00:00` |
| `sq_crtime` | `14:43:14` |
| `sq_cruser` | `TEST` |


#### anoml

**2 row(s) added:**

*Row 1:*
| Field | Value |
|-------|-------|
| `ax_comment` | `Adams Light Engineering Ltd   BACS` |
| `ax_date` | `2024-05-01T00:00:00` |
| `ax_done` | `Y` |
| `ax_fcdec` | `0.0` |
| `ax_fcmult` | `0.0` |
| `ax_fcrate` | `0.0` |
| `ax_fcurr` | `` |
| `ax_fvalue` | `0.0` |
| `ax_job` | `` |
| `ax_jrnl` | `3451.0` |
| `ax_nacnt` | `C310` |
| `ax_ncntr` | `` |
| `ax_nlpdate` | `2024-05-01T00:00:00` |
| `ax_project` | `` |
| `ax_source` | `S` |
| `ax_srcco` | `Z` |
| `ax_tref` | `test` |
| `ax_unique` | `_7FL0VJUWZ` |
| `ax_value` | `500.0` |

*Row 2:*
| Field | Value |
|-------|-------|
| `ax_comment` | `Adams Light Engineering Ltd   BACS` |
| `ax_date` | `2024-05-01T00:00:00` |
| `ax_done` | `Y` |
| `ax_fcdec` | `0.0` |
| `ax_fcmult` | `0.0` |
| `ax_fcrate` | `0.0` |
| `ax_fcurr` | `` |
| `ax_fvalue` | `0.0` |
| `ax_job` | `` |
| `ax_jrnl` | `3451.0` |
| `ax_nacnt` | `C110` |
| `ax_ncntr` | `` |
| `ax_nlpdate` | `2024-05-01T00:00:00` |
| `ax_project` | `` |
| `ax_source` | `S` |
| `ax_srcco` | `Z` |
| `ax_tref` | `test` |
| `ax_unique` | `_7FL0VJUWZ` |
| `ax_value` | `-500.0` |


#### atran

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `at_account` | `ADA0001` |
| `at_acnt` | `C310` |
| `at_atpycd` | `` |
| `at_bacprn` | `0.0` |
| `at_bic` | `` |
| `at_bsname` | `` |
| `at_bsref` | `` |
| `at_cash` | `0.0` |
| `at_cbtype` | `R2` |
| `at_ccauth` | `0` |
| `at_ccdno` | `` |
| `at_ccdprn` | `0.0` |
| `at_chqlst` | `0.0` |
| `at_chqprn` | `0.0` |
| `at_cntr` | `` |
| `at_comment` | `` |
| `at_disc` | `0.0` |
| `at_ecb` | `0.0` |
| `at_ecbtype` | `` |
| `at_entry` | `R200000719` |
| `at_fcdec` | `2.0` |
| `at_fcexch` | `1.0` |
| `at_fcmult` | `0.0` |
| `at_fcurr` | `` |
| `at_iban` | `` |
| `at_inputby` | `TEST` |
| `at_job` | `` |
| `at_memo` | `` |
| `at_name` | `Adams Light Engineering Ltd` |
| `at_number` | `` |
| `at_payee` | `` |
| `at_payname` | `` |
| `at_payslp` | `0.0` |
| `at_postgrp` | `0.0` |
| `at_project` | `` |
| `at_pstdate` | `2024-05-01T00:00:00` |
| `at_pysprn` | `0.0` |
| `at_refer` | `test` |
| `at_remit` | `0.0` |
| `at_remove` | `0.0` |
| `at_sort` | `` |
| `at_srcco` | `Z` |
| `at_sysdate` | `2024-05-01T00:00:00` |
| `at_tperiod` | `1.0` |
| `at_type` | `4.0` |
| `at_unique` | `_7FL0VJUWZ` |
| `at_value` | `50000.0` |
| `at_vattycd` | `` |


#### atype

**1 row(s) modified:**

*Row id=10.0:*
| Field | Before | After |
|-------|--------|-------|
| `ay_entry` | `R200000719` | `R200000720` |


#### dmcomp

**1 row(s) modified:**

*Row id=2.0:*
| Field | Before | After |
|-------|--------|-------|
| `changedon` | `2026-04-01T14:20:08` | `2026-04-01T14:43:21` |


#### dmcont

**5 row(s) modified:**

*Row id=18.0:*
| Field | Before | After |
|-------|--------|-------|

*Row id=19.0:*
| Field | Before | After |
|-------|--------|-------|

*Row id=20.0:*
| Field | Before | After |
|-------|--------|-------|

*Row id=249.0:*
| Field | Before | After |
|-------|--------|-------|

*Row id=260.0:*
| Field | Before | After |
|-------|--------|-------|


#### idtab

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `id_numericid` | `3451.0` | `3452.0` |


#### nacnt

**2 row(s) modified:**

*Row id=12.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `20821.73` | `20321.73` |
| `na_ptdcr` | `2417.98` | `2917.98` |
| `na_ytdcr` | `1563125.02` | `1563625.02` |

*Row id=16.0:*
| Field | Before | After |
|-------|--------|-------|
| `na_balc05` | `-27931.95` | `-27431.95` |
| `na_ptddr` | `55.0` | `555.0` |
| `na_ytddr` | `272141.37` | `272641.37` |


#### nbank

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `nk_curbal` | `4733626.0` | `4783626.0` |


#### ndetail

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `nt_acnt` | `C310` |
| `nt_cdesc` | `` |
| `nt_cmnt` | `test` |
| `nt_cntr` | `` |
| `nt_consol` | `0.0` |
| `nt_distrib` | `0.0` |
| `nt_entr` | `2024-05-01T00:00:00` |
| `nt_fcdec` | `0.0` |
| `nt_fcmult` | `0.0` |
| `nt_fcrate` | `0.0` |
| `nt_fcurr` | `` |
| `nt_fvalue` | `0.0` |
| `nt_inp` | `TEST` |
| `nt_job` | `` |
| `nt_jrnl` | `3451.0` |
| `nt_period` | `5.0` |
| `nt_perpost` | `0.0` |
| `nt_posttyp` | `S` |
| `nt_prevyr` | `0.0` |
| `nt_project` | `` |
| `nt_pstgrp` | `1.0` |
| `nt_pstid` | `_7FL0VJYCQ` |
| `nt_recjrnl` | `0.0` |
| `nt_rectify` | `0.0` |
| `nt_recurr` | `0.0` |
| `nt_ref` | `` |
| `nt_rvrse` | `0.0` |
| `nt_srcco` | `Z` |
| `nt_subt` | `03` |
| `nt_trnref` | `Adams Light Engineering Ltd   BACS` |
| `nt_trtype` | `A` |
| `nt_type` | `10` |
| `nt_value` | `500.0` |
| `nt_vatanal` | `0.0` |
| `nt_year` | `2024.0` |


#### nextid

**7 row(s) modified:**

*Row id=10.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `11059.0` | `11060.0` |

*Row id=13.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `26125.0` | `26127.0` |

*Row id=21.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `17747.0` | `17748.0` |

*Row id=229.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `6896.0` | `6897.0` |

*Row id=235.0:*
| Field | Before | After |
|-------|--------|-------|
| `nextid` | `215.0` | `216.0` |


#### nhist

**2 row(s) modified:**

*Row id=159881.0:*
| Field | Before | After |
|-------|--------|-------|
| `nh_bal` | `20821.73` | `20321.73` |
| `nh_ptdcr` | `-2417.98` | `-2917.98` |

*Row id=159882.0:*
| Field | Before | After |
|-------|--------|-------|
| `nh_bal` | `-27931.95` | `-27431.95` |
| `nh_ptddr` | `55.0` | `555.0` |


#### njmemo

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `nj_binrep` | `0.0` |
| `nj_image` | `` |
| `nj_journal` | `3451.0` |
| `nj_memo` | `ÿ<<JOURNAL_DATA_ONLY>>ÿ` |
| `nj_txtrep` | `Cashbook Ledger Transfer` |


#### nsubt

**2 row(s) modified:**

*Row id=6.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `432713.47` | `432213.47` |

*Row id=8.0:*
| Field | Before | After |
|-------|--------|-------|
| `ns_balance` | `2770304.35` | `2770804.35` |


#### ntype

**1 row(s) modified:**

*Row id=3.0:*
| Field | Before | After |
|-------|--------|-------|


#### sname

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `sn_currbal` | `16732.18` | `16232.18` |


#### stran

**1 row(s) added:**

| Field | Value |
|-------|-------|
| `jxrenewal` | `0.0` |
| `jxservid` | `0.0` |
| `st_account` | `ADA0001` |
| `st_adjsv` | `0.0` |
| `st_advallc` | `0.0` |
| `st_advance` | `N` |
| `st_binrep` | `0.0` |
| `st_cash` | `0.0` |
| `st_cbtype` | `R2` |
| `st_crdate` | `2024-05-01T00:00:00` |
| `st_custref` | `BACS` |
| `st_delacc` | `ADA0001` |
| `st_dispute` | `0.0` |
| `st_edi` | `0.0` |
| `st_editx` | `0.0` |
| `st_edivn` | `0.0` |
| `st_entry` | `R200000719` |
| `st_eurind` | `` |
| `st_euro` | `0.0` |
| `st_exttime` | `` |
| `st_fadval` | `0.0` |
| `st_fcbal` | `0.0` |
| `st_fcdec` | `0.0` |
| `st_fcmult` | `0.0` |
| `st_fcrate` | `0.0` |
| `st_fcurr` | `` |
| `st_fcval` | `0.0` |
| `st_fcvat` | `0.0` |
| `st_fullamt` | `0.0` |
| `st_fullcb` | `` |
| `st_fullnar` | `` |
| `st_gateid` | `0.0` |
| `st_gatetr` | `0.0` |
| `st_luptime` | `` |
| `st_memo` | `` |
| `st_nlpdate` | `2024-05-01T00:00:00` |
| `st_origcur` | `` |
| `st_paid` | `` |
| `st_payadvl` | `0.0` |
| `st_payflag` | `0.0` |
| `st_rcode` | `` |
| `st_region` | `` |
| `st_revchrg` | `0.0` |
| `st_ruser` | `` |
| `st_set1` | `0.0` |
| `st_set1day` | `0.0` |
| `st_set2` | `0.0` |
| `st_set2day` | `0.0` |
| `st_terr` | `` |
| `st_trbal` | `-500.0` |
| `st_trdate` | `2024-05-01T00:00:00` |
| `st_trref` | `test` |
| `st_trtype` | `R` |
| `st_trvalue` | `-500.0` |
| `st_txtrep` | `` |
| `st_type` | `` |
| `st_unique` | `_7FL0VJUWZ` |
| `st_vatval` | `0.0` |


#### zlock

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|


---

## System Configuration

### Turn Real Tine on

#### seqco

**6 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|
| `co_rtupdnl` | `0.0` | `1.0` |

*Row id=2.0:*
| Field | Before | After |
|-------|--------|-------|

*Row id=5.0:*
| Field | Before | After |
|-------|--------|-------|

*Row id=6.0:*
| Field | Before | After |
|-------|--------|-------|

*Row id=7.0:*
| Field | Before | After |
|-------|--------|-------|


#### seqsys

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|


#### sequser

**1 row(s) modified:**

*Row id=3.0:*
| Field | Before | After |
|-------|--------|-------|


#### cparm

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|


#### fparm

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|


#### pparm

**1 row(s) modified:**

*Row id=1.0:*
| Field | Before | After |
|-------|--------|-------|


#### sparm

**1 row(s) modified:**

*Row id=2.0:*
| Field | Before | After |
|-------|--------|-------|


---
