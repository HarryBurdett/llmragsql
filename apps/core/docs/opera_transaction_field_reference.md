# Opera Transaction Posting — Complete Field Reference

Generated from transaction snapshot library (30 entries, 45 tables).
Every field value captured from real Opera postings.
Use this reference when writing transactions back to Opera with 100% accuracy.

---

## Cashbook Transactions

### Bank Rec update

*Purchase invoice posting. Creates: ptran, pnoml, ntran, nacnt, pname balance.*

**aentry**
  *12 row(s) modified*

**nbank**
  *7 row(s) modified*

---

### Nominal Payment

*Payment to nominal account (no ledger). Creates: aentry, atran, ntran, anoml, nacnt, nbank.*

**aentry**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `ae_acnt` | `C310` |  |
  | `ae_batchid` | `0.0` |  |
  | `ae_brwptr` | `` |  |
  | `ae_cbtype` | `P1` |  |
  | `ae_cntr` | `` |  |
  | `ae_comment` | `test` |  |
  | `ae_complet` | `1.0` |  |
  | `ae_entref` | `test` |  |
  | `ae_entry` | `P100000768` | From atype counter |
  | `ae_frstat` | `0.0` |  |
  | `ae_lstdate` | `2024-05-01T00:00:00` |  |
  | `ae_payid` | `0.0` |  |
  | `ae_postgrp` | `0.0` |  |
  | `ae_recbal` | `0.0` |  |
  | `ae_reclnum` | `0.0` |  |
  | `ae_remove` | `0.0` |  |
  | `ae_statln` | `0.0` |  |
  | `ae_tmpstat` | `0.0` |  |
  | `ae_tostat` | `0.0` |  |
  | `ae_value` | `-10000.0` |  |
  | `sq_amtime` | `` |  |
  | `sq_amuser` | `` |  |
  | `sq_crdate` | `2024-05-01T00:00:00` |  |
  | `sq_crtime` | `13:51:57` |  |
  | `sq_cruser` | `TEST` |  |

**anoml**
  *3 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ax_comment` | `test` |  |
  | `ax_date` | `2024-05-01T00:00:00` |  |
  | `ax_done` | `` |  |
  | `ax_fcdec` | `2.0` | Foreign currency field |
  | `ax_fcmult` | `0.0` |  |
  | `ax_fcrate` | `1.0` | Foreign currency field |
  | `ax_fcurr` | `` | Foreign currency field |
  | `ax_fvalue` | `8333.0` |  |
  | `ax_job` | `SM` |  |
  | `ax_jrnl` | `0.0` | From nparm.np_nexjrnl |
  | `ax_nacnt` | `S120` |  |
  | `ax_ncntr` | `ADM` |  |
  | `ax_nlpdate` | `2024-05-01T00:00:00` |  |
  | `ax_project` | `` |  |
  | `ax_source` | `A` |  |
  | `ax_srcco` | `Z` |  |
  | `ax_tref` | `test` |  |
  | `ax_unique` | `_7FL0TPWZX` | Base-36 unique ID |
  | `ax_value` | `83.33` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ax_comment` | `test` |  |
  | `ax_date` | `2024-05-01T00:00:00` |  |
  | `ax_done` | `` |  |
  | `ax_fcdec` | `2.0` | Foreign currency field |
  | `ax_fcmult` | `0.0` |  |
  | `ax_fcrate` | `1.0` | Foreign currency field |
  | `ax_fcurr` | `` | Foreign currency field |
  | `ax_fvalue` | `1667.0` |  |
  | `ax_job` | `` |  |
  | `ax_jrnl` | `0.0` | From nparm.np_nexjrnl |
  | `ax_nacnt` | `E225` |  |
  | `ax_ncntr` | `` |  |
  | `ax_nlpdate` | `2024-05-01T00:00:00` |  |
  | `ax_project` | `` |  |
  | `ax_source` | `A` |  |
  | `ax_srcco` | `Z` |  |
  | `ax_tref` | `test` |  |
  | `ax_unique` | `_7FL0TPWZX` | Base-36 unique ID |
  | `ax_value` | `16.67` |  |

  Row 3:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ax_comment` | `test` |  |
  | `ax_date` | `2024-05-01T00:00:00` |  |
  | `ax_done` | `` |  |
  | `ax_fcdec` | `2.0` | Foreign currency field |
  | `ax_fcmult` | `0.0` |  |
  | `ax_fcrate` | `1.0` | Foreign currency field |
  | `ax_fcurr` | `` | Foreign currency field |
  | `ax_fvalue` | `-10000.0` |  |
  | `ax_job` | `` |  |
  | `ax_jrnl` | `0.0` | From nparm.np_nexjrnl |
  | `ax_nacnt` | `C310` |  |
  | `ax_ncntr` | `` |  |
  | `ax_nlpdate` | `2024-05-01T00:00:00` |  |
  | `ax_project` | `` |  |
  | `ax_source` | `A` |  |
  | `ax_srcco` | `Z` |  |
  | `ax_tref` | `test` |  |
  | `ax_unique` | `_7FL0TPWZX` | Base-36 unique ID |
  | `ax_value` | `-100.0` |  |

**atran**
  *2 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `at_account` | `S120    ADM` |  |
  | `at_acnt` | `C310` |  |
  | `at_atpycd` | `` |  |
  | `at_bacprn` | `0.0` |  |
  | `at_bic` | `` |  |
  | `at_bsname` | `` |  |
  | `at_bsref` | `` |  |
  | `at_cash` | `0.0` |  |
  | `at_cbtype` | `P1` |  |
  | `at_ccauth` | `` |  |
  | `at_ccdno` | `` |  |
  | `at_ccdprn` | `0.0` |  |
  | `at_chqlst` | `0.0` |  |
  | `at_chqprn` | `0.0` |  |
  | `at_cntr` | `` |  |
  | `at_comment` | `test` |  |
  | `at_disc` | `0.0` |  |
  | `at_ecb` | `0.0` |  |
  | `at_ecbpayd` | `2024-05-01T00:00:00` |  |
  | `at_ecbtype` | `` |  |
  | `at_entry` | `P100000768` | From atype counter |
  | `at_fcdec` | `2.0` | Foreign currency field |
  | `at_fcexch` | `1.0` | Foreign currency field |
  | `at_fcmult` | `0.0` |  |
  | `at_fcurr` | `` | Foreign currency field |
  | `at_iban` | `` |  |
  | `at_inputby` | `TEST` |  |
  | `at_job` | `SM` |  |
  | `at_memo` | `` |  |
  | `at_name` | `Travel Expenses / Subsistence` |  |
  | `at_number` | `` |  |
  | `at_payee` | `` |  |
  | `at_payname` | `` |  |
  | `at_payslp` | `0.0` |  |
  | `at_postgrp` | `0.0` |  |
  | `at_project` | `` |  |
  | `at_pstdate` | `2024-05-01T00:00:00` |  |
  | `at_pysprn` | `0.0` |  |
  | `at_refer` | `test` |  |
  | `at_remit` | `0.0` |  |
  | `at_remove` | `0.0` |  |
  | `at_sort` | `` |  |
  | `at_srcco` | `Z` |  |
  | `at_sysdate` | `2024-05-01T00:00:00` |  |
  | `at_tperiod` | `1.0` | From nominal calendar |
  | `at_type` | `1.0` |  |
  | `at_unique` | `_7FL0TPWZX` | Base-36 unique ID |
  | `at_value` | `-8333.0` |  |
  | `at_vattycd` | `` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `at_account` | `E225` |  |
  | `at_acnt` | `C310` |  |
  | `at_atpycd` | `` |  |
  | `at_bacprn` | `0.0` |  |
  | `at_bic` | `` |  |
  | `at_bsname` | `` |  |
  | `at_bsref` | `` |  |
  | `at_cash` | `0.0` |  |
  | `at_cbtype` | `P1` |  |
  | `at_ccauth` | `` |  |
  | `at_ccdno` | `` |  |
  | `at_ccdprn` | `0.0` |  |
  | `at_chqlst` | `0.0` |  |
  | `at_chqprn` | `0.0` |  |
  | `at_cntr` | `` |  |
  | `at_comment` | `test` |  |
  | `at_disc` | `0.0` |  |
  | `at_ecb` | `0.0` |  |
  | `at_ecbpayd` | `2024-05-01T00:00:00` |  |
  | `at_ecbtype` | `` |  |
  | `at_entry` | `P100000768` | From atype counter |
  | `at_fcdec` | `2.0` | Foreign currency field |
  | `at_fcexch` | `1.0` | Foreign currency field |
  | `at_fcmult` | `0.0` |  |
  | `at_fcurr` | `` | Foreign currency field |
  | `at_iban` | `` |  |
  | `at_inputby` | `TEST` |  |
  | `at_job` | `SM` |  |
  | `at_memo` | `` |  |
  | `at_name` | `VAT (Input - Purchases)` |  |
  | `at_number` | `` |  |
  | `at_payee` | `` |  |
  | `at_payname` | `` |  |
  | `at_payslp` | `0.0` |  |
  | `at_postgrp` | `0.0` |  |
  | `at_project` | `` |  |
  | `at_pstdate` | `2024-05-01T00:00:00` |  |
  | `at_pysprn` | `0.0` |  |
  | `at_refer` | `test` |  |
  | `at_remit` | `0.0` |  |
  | `at_remove` | `0.0` |  |
  | `at_sort` | `` |  |
  | `at_srcco` | `Z` |  |
  | `at_sysdate` | `2024-05-01T00:00:00` |  |
  | `at_tperiod` | `1.0` | From nominal calendar |
  | `at_type` | `1.0` |  |
  | `at_unique` | `_7FL0TPWZX` | Base-36 unique ID |
  | `at_value` | `-1667.0` |  |
  | `at_vattycd` | `` |  |

**atype**
  *1 row(s) modified*

**nbank**
  *1 row(s) modified*

**nextid**
  *4 row(s) modified*

**zlock**
  *1 row(s) modified*

---

### Nominal Receipt

*Receipt from nominal account (no ledger). Creates: aentry, atran, ntran, anoml, nacnt, nbank.*

**aentry**
  *1 row(s) modified*

**anoml**
  *2 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ax_comment` | `test` |  |
  | `ax_date` | `2024-05-01T00:00:00` |  |
  | `ax_done` | `Y` | NL transfer complete |
  | `ax_fcdec` | `2.0` | Foreign currency field |
  | `ax_fcmult` | `0.0` |  |
  | `ax_fcrate` | `1.0` | Foreign currency field |
  | `ax_fcurr` | `` | Foreign currency field |
  | `ax_fvalue` | `5500.0` |  |
  | `ax_job` | `` |  |
  | `ax_jrnl` | `3446.0` | From nparm.np_nexjrnl |
  | `ax_nacnt` | `C310` |  |
  | `ax_ncntr` | `` |  |
  | `ax_nlpdate` | `2024-05-01T00:00:00` |  |
  | `ax_project` | `` |  |
  | `ax_source` | `A` |  |
  | `ax_srcco` | `Z` |  |
  | `ax_tref` | `test` |  |
  | `ax_unique` | `_7FL0UH01D` | Base-36 unique ID |
  | `ax_value` | `55.0` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ax_comment` | `test` |  |
  | `ax_date` | `2024-05-01T00:00:00` |  |
  | `ax_done` | `Y` | NL transfer complete |
  | `ax_fcdec` | `2.0` | Foreign currency field |
  | `ax_fcmult` | `0.0` |  |
  | `ax_fcrate` | `1.0` | Foreign currency field |
  | `ax_fcurr` | `` | Foreign currency field |
  | `ax_fvalue` | `-5500.0` |  |
  | `ax_job` | `` |  |
  | `ax_jrnl` | `3446.0` | From nparm.np_nexjrnl |
  | `ax_nacnt` | `S120` |  |
  | `ax_ncntr` | `ADM` |  |
  | `ax_nlpdate` | `2024-05-01T00:00:00` |  |
  | `ax_project` | `` |  |
  | `ax_source` | `A` |  |
  | `ax_srcco` | `Z` |  |
  | `ax_tref` | `test` |  |
  | `ax_unique` | `_7FL0UH01D` | Base-36 unique ID |
  | `ax_value` | `-55.0` |  |

**atran**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `at_account` | `S120    ADM` |  |
  | `at_acnt` | `C310` |  |
  | `at_atpycd` | `` |  |
  | `at_bacprn` | `0.0` |  |
  | `at_bic` | `` |  |
  | `at_bsname` | `` |  |
  | `at_bsref` | `` |  |
  | `at_cash` | `0.0` |  |
  | `at_cbtype` | `R1` |  |
  | `at_ccauth` | `` |  |
  | `at_ccdno` | `` |  |
  | `at_ccdprn` | `0.0` |  |
  | `at_chqlst` | `0.0` |  |
  | `at_chqprn` | `0.0` |  |
  | `at_cntr` | `` |  |
  | `at_comment` | `test` |  |
  | `at_disc` | `0.0` |  |
  | `at_ecb` | `0.0` |  |
  | `at_ecbpayd` | `2024-05-01T00:00:00` |  |
  | `at_ecbtype` | `` |  |
  | `at_entry` | `R100000232` | From atype counter |
  | `at_fcdec` | `2.0` | Foreign currency field |
  | `at_fcexch` | `1.0` | Foreign currency field |
  | `at_fcmult` | `0.0` |  |
  | `at_fcurr` | `` | Foreign currency field |
  | `at_iban` | `` |  |
  | `at_inputby` | `TEST` |  |
  | `at_job` | `` |  |
  | `at_memo` | `` |  |
  | `at_name` | `Travel Expenses / Subsistence` |  |
  | `at_number` | `` |  |
  | `at_payee` | `` |  |
  | `at_payname` | `` |  |
  | `at_payslp` | `0.0` |  |
  | `at_postgrp` | `1.0` |  |
  | `at_project` | `` |  |
  | `at_pstdate` | `2024-05-01T00:00:00` |  |
  | `at_pysprn` | `1.0` |  |
  | `at_refer` | `test` |  |
  | `at_remit` | `0.0` |  |
  | `at_remove` | `0.0` |  |
  | `at_sort` | `` |  |
  | `at_srcco` | `Z` |  |
  | `at_sysdate` | `2024-05-01T00:00:00` |  |
  | `at_tperiod` | `1.0` | From nominal calendar |
  | `at_type` | `2.0` |  |
  | `at_unique` | `_7FL0UH01D` | Base-36 unique ID |
  | `at_value` | `5500.0` |  |
  | `at_vattycd` | `` |  |

**idtab**
  *1 row(s) modified*

**nacnt**
  *2 row(s) modified*

**nbank**
  *1 row(s) modified*

**ndetail**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `nt_acnt` | `C310` |  |
  | `nt_cdesc` | `` |  |
  | `nt_cmnt` | `test` |  |
  | `nt_cntr` | `` |  |
  | `nt_consol` | `0.0` |  |
  | `nt_distrib` | `0.0` |  |
  | `nt_entr` | `2024-05-01T00:00:00` |  |
  | `nt_fcdec` | `0.0` |  |
  | `nt_fcmult` | `0.0` |  |
  | `nt_fcrate` | `0.0` |  |
  | `nt_fcurr` | `` | Foreign currency field |
  | `nt_fvalue` | `0.0` |  |
  | `nt_inp` | `TEST` |  |
  | `nt_job` | `` |  |
  | `nt_jrnl` | `3446.0` | From nparm.np_nexjrnl |
  | `nt_period` | `5.0` | From nominal calendar |
  | `nt_perpost` | `0.0` |  |
  | `nt_posttyp` | `A` |  |
  | `nt_prevyr` | `0.0` |  |
  | `nt_project` | `` |  |
  | `nt_pstgrp` | `1.0` |  |
  | `nt_pstid` | `_7FL0UH0Q3` |  |
  | `nt_recjrnl` | `0.0` | From nparm.np_nexjrnl |
  | `nt_rectify` | `0.0` |  |
  | `nt_recurr` | `0.0` |  |
  | `nt_ref` | `` |  |
  | `nt_rvrse` | `0.0` |  |
  | `nt_srcco` | `Z` |  |
  | `nt_subt` | `03` |  |
  | `nt_trnref` | `test` |  |
  | `nt_trtype` | `A` |  |
  | `nt_type` | `10` |  |
  | `nt_value` | `55.0` |  |
  | `nt_vatanal` | `0.0` |  |
  | `nt_year` | `2024.0` |  |

**nextid**
  *6 row(s) modified*

**nhist**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `nh_bal` | `-55.0` |  |
  | `nh_budg` | `0.0` |  |
  | `nh_fbal` | `0.0` |  |
  | `nh_job` | `` |  |
  | `nh_nacnt` | `S120` |  |
  | `nh_ncntr` | `ADM` |  |
  | `nh_nsubt` | `03` |  |
  | `nh_ntype` | `45` |  |
  | `nh_period` | `5.0` | From nominal calendar |
  | `nh_project` | `` |  |
  | `nh_ptdcr` | `-55.0` |  |
  | `nh_ptddr` | `0.0` |  |
  | `nh_rbudg` | `0.0` |  |
  | `nh_rectype` | `1.0` |  |
  | `nh_year` | `2024.0` |  |

  *1 row(s) modified*

**njmemo**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `nj_binrep` | `0.0` |  |
  | `nj_image` | `` |  |
  | `nj_journal` | `3446.0` | From nparm.np_nexjrnl |
  | `nj_memo` | `ÿ<<JOURNAL_DATA_ONLY>>ÿ` |  |
  | `nj_txtrep` | `Cashbook Ledger Transfer` |  |

**nsubt**
  *2 row(s) modified*

**ntype**
  *2 row(s) modified*

**zlock**
  *1 row(s) modified*

---

### Recurring entry posting

*Purchase invoice posting. Creates: ptran, pnoml, ntran, nacnt, pname balance.*

**aentry**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `ae_acnt` | `C310` |  |
  | `ae_batchid` | `0.0` |  |
  | `ae_brwptr` | `` |  |
  | `ae_cbtype` | `P1` |  |
  | `ae_cntr` | `` |  |
  | `ae_comment` | `` |  |
  | `ae_complet` | `1.0` |  |
  | `ae_entref` | `test` |  |
  | `ae_entry` | `P100000769` | From atype counter |
  | `ae_frstat` | `0.0` |  |
  | `ae_lstdate` | `2024-05-01T00:00:00` |  |
  | `ae_payid` | `0.0` |  |
  | `ae_postgrp` | `0.0` |  |
  | `ae_recbal` | `0.0` |  |
  | `ae_reclnum` | `0.0` |  |
  | `ae_remove` | `0.0` |  |
  | `ae_statln` | `0.0` |  |
  | `ae_tmpstat` | `0.0` |  |
  | `ae_tostat` | `0.0` |  |
  | `ae_value` | `-10000.0` |  |
  | `sq_amtime` | `` |  |
  | `sq_amuser` | `` |  |
  | `sq_crdate` | `2024-05-01T00:00:00` |  |
  | `sq_crtime` | `14:46:39` |  |
  | `sq_cruser` | `TEST` |  |

**anoml**
  *3 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ax_comment` | `` |  |
  | `ax_date` | `2024-05-01T00:00:00` |  |
  | `ax_done` | `Y` | NL transfer complete |
  | `ax_fcdec` | `2.0` | Foreign currency field |
  | `ax_fcmult` | `0.0` |  |
  | `ax_fcrate` | `1.0` | Foreign currency field |
  | `ax_fcurr` | `` | Foreign currency field |
  | `ax_fvalue` | `-10000.0` |  |
  | `ax_job` | `` |  |
  | `ax_jrnl` | `3452.0` | From nparm.np_nexjrnl |
  | `ax_nacnt` | `C310` |  |
  | `ax_ncntr` | `` |  |
  | `ax_nlpdate` | `2024-05-01T00:00:00` |  |
  | `ax_project` | `` |  |
  | `ax_source` | `A` |  |
  | `ax_srcco` | `Z` |  |
  | `ax_tref` | `test` |  |
  | `ax_unique` | `_7FL0VO92J` | Base-36 unique ID |
  | `ax_value` | `-100.0` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ax_comment` | `` |  |
  | `ax_date` | `2024-05-01T00:00:00` |  |
  | `ax_done` | `Y` | NL transfer complete |
  | `ax_fcdec` | `2.0` | Foreign currency field |
  | `ax_fcmult` | `0.0` |  |
  | `ax_fcrate` | `1.0` | Foreign currency field |
  | `ax_fcurr` | `` | Foreign currency field |
  | `ax_fvalue` | `1667.0` |  |
  | `ax_job` | `` |  |
  | `ax_jrnl` | `3452.0` | From nparm.np_nexjrnl |
  | `ax_nacnt` | `E225` |  |
  | `ax_ncntr` | `` |  |
  | `ax_nlpdate` | `2024-05-01T00:00:00` |  |
  | `ax_project` | `` |  |
  | `ax_source` | `A` |  |
  | `ax_srcco` | `Z` |  |
  | `ax_tref` | `test` |  |
  | `ax_unique` | `_7FL0VO92J` | Base-36 unique ID |
  | `ax_value` | `16.67` |  |

  Row 3:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ax_comment` | `` |  |
  | `ax_date` | `2024-05-01T00:00:00` |  |
  | `ax_done` | `Y` | NL transfer complete |
  | `ax_fcdec` | `2.0` | Foreign currency field |
  | `ax_fcmult` | `0.0` |  |
  | `ax_fcrate` | `1.0` | Foreign currency field |
  | `ax_fcurr` | `` | Foreign currency field |
  | `ax_fvalue` | `8333.0` |  |
  | `ax_job` | `` |  |
  | `ax_jrnl` | `3452.0` | From nparm.np_nexjrnl |
  | `ax_nacnt` | `S120` |  |
  | `ax_ncntr` | `ADM` |  |
  | `ax_nlpdate` | `2024-05-01T00:00:00` |  |
  | `ax_project` | `` |  |
  | `ax_source` | `A` |  |
  | `ax_srcco` | `Z` |  |
  | `ax_tref` | `test` |  |
  | `ax_unique` | `_7FL0VO92J` | Base-36 unique ID |
  | `ax_value` | `83.33` |  |

**aparm**
  *1 row(s) modified*

**arhead**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `ae_acnt` | `C310` |  |
  | `ae_cntr` | `` |  |
  | `ae_desc` | `test` |  |
  | `ae_entry` | `REC0000026` | From atype counter |
  | `ae_every` | `1.0` |  |
  | `ae_freq` | `M` |  |
  | `ae_inputby` | `TEST` |  |
  | `ae_lstpost` | `2024-05-01T00:00:00` |  |
  | `ae_nxtpost` | `2024-06-01T00:00:00` |  |
  | `ae_posted` | `1.0` |  |
  | `ae_srcco` | `Z` |  |
  | `ae_topost` | `1.0` |  |
  | `ae_type` | `1.0` |  |
  | `ae_vatanal` | `1.0` |  |
  | `sq_amtime` | `` |  |
  | `sq_amuser` | `` |  |
  | `sq_crdate` | `2024-05-01T00:00:00` |  |
  | `sq_crtime` | `14:46:16` |  |
  | `sq_cruser` | `TEST` |  |
  | `sq_memo` | `` |  |

**arline**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `at_account` | `S120    ADM` |  |
  | `at_acnt` | `C310` |  |
  | `at_atpycd` | `` |  |
  | `at_bacprn` | `0.0` |  |
  | `at_bic` | `` |  |
  | `at_cbtype` | `P1` |  |
  | `at_ccdno` | `` |  |
  | `at_chqprn` | `0.0` |  |
  | `at_cntr` | `` |  |
  | `at_comment` | `` |  |
  | `at_disc` | `0.0` |  |
  | `at_discnl` | `` |  |
  | `at_ecb` | `0.0` |  |
  | `at_ecbtype` | `` |  |
  | `at_entref` | `test` |  |
  | `at_entry` | `REC0000026` | From atype counter |
  | `at_fcdec` | `2.0` | Foreign currency field |
  | `at_fcurr` | `` | Foreign currency field |
  | `at_iban` | `` |  |
  | `at_job` | `` |  |
  | `at_line` | `1.0` |  |
  | `at_number` | `` |  |
  | `at_payee` | `` |  |
  | `at_payname` | `` |  |
  | `at_project` | `` |  |
  | `at_ref2` | `` |  |
  | `at_remit` | `0.0` |  |
  | `at_sort` | `` |  |
  | `at_srcco` | `` |  |
  | `at_unique` | `` | Base-36 unique ID |
  | `at_value` | `-10000.0` |  |
  | `at_vatcde` | `1` |  |
  | `at_vattyp` | `P` |  |
  | `at_vatval` | `-1667.0` |  |
  | `sq_amtime` | `` |  |
  | `sq_amuser` | `` |  |
  | `sq_crdate` | `2024-05-01T00:00:00` |  |
  | `sq_crtime` | `14:46:12` |  |
  | `sq_cruser` | `TEST` |  |
  | `sq_memo` | `` |  |

**atran**
  *2 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `at_account` | `S120    ADM` |  |
  | `at_acnt` | `C310` |  |
  | `at_atpycd` | `` |  |
  | `at_bacprn` | `0.0` |  |
  | `at_bic` | `` |  |
  | `at_bsname` | `` |  |
  | `at_bsref` | `` |  |
  | `at_cash` | `0.0` |  |
  | `at_cbtype` | `P1` |  |
  | `at_ccauth` | `` |  |
  | `at_ccdno` | `` |  |
  | `at_ccdprn` | `0.0` |  |
  | `at_chqlst` | `0.0` |  |
  | `at_chqprn` | `0.0` |  |
  | `at_cntr` | `` |  |
  | `at_comment` | `` |  |
  | `at_disc` | `0.0` |  |
  | `at_ecb` | `0.0` |  |
  | `at_ecbpayd` | `2024-05-01T00:00:00` |  |
  | `at_ecbtype` | `` |  |
  | `at_entry` | `P100000769` | From atype counter |
  | `at_fcdec` | `2.0` | Foreign currency field |
  | `at_fcexch` | `1.0` | Foreign currency field |
  | `at_fcmult` | `0.0` |  |
  | `at_fcurr` | `` | Foreign currency field |
  | `at_iban` | `` |  |
  | `at_inputby` | `TEST` |  |
  | `at_job` | `` |  |
  | `at_memo` | `` |  |
  | `at_name` | `Travel Expenses / Subsistence` |  |
  | `at_number` | `` |  |
  | `at_payee` | `` |  |
  | `at_payname` | `` |  |
  | `at_payslp` | `0.0` |  |
  | `at_postgrp` | `0.0` |  |
  | `at_project` | `` |  |
  | `at_pstdate` | `2024-05-01T00:00:00` |  |
  | `at_pysprn` | `0.0` |  |
  | `at_refer` | `test` |  |
  | `at_remit` | `0.0` |  |
  | `at_remove` | `0.0` |  |
  | `at_sort` | `` |  |
  | `at_srcco` | `Z` |  |
  | `at_sysdate` | `2024-05-01T00:00:00` |  |
  | `at_tperiod` | `1.0` | From nominal calendar |
  | `at_type` | `1.0` |  |
  | `at_unique` | `_7FL0VO97M` | Base-36 unique ID |
  | `at_value` | `-8333.0` |  |
  | `at_vattycd` | `` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `at_account` | `E225` |  |
  | `at_acnt` | `C310` |  |
  | `at_atpycd` | `` |  |
  | `at_bacprn` | `0.0` |  |
  | `at_bic` | `` |  |
  | `at_bsname` | `` |  |
  | `at_bsref` | `` |  |
  | `at_cash` | `0.0` |  |
  | `at_cbtype` | `P1` |  |
  | `at_ccauth` | `` |  |
  | `at_ccdno` | `` |  |
  | `at_ccdprn` | `0.0` |  |
  | `at_chqlst` | `0.0` |  |
  | `at_chqprn` | `0.0` |  |
  | `at_cntr` | `` |  |
  | `at_comment` | `` |  |
  | `at_disc` | `0.0` |  |
  | `at_ecb` | `0.0` |  |
  | `at_ecbpayd` | `2024-05-01T00:00:00` |  |
  | `at_ecbtype` | `` |  |
  | `at_entry` | `P100000769` | From atype counter |
  | `at_fcdec` | `2.0` | Foreign currency field |
  | `at_fcexch` | `1.0` | Foreign currency field |
  | `at_fcmult` | `0.0` |  |
  | `at_fcurr` | `` | Foreign currency field |
  | `at_iban` | `` |  |
  | `at_inputby` | `TEST` |  |
  | `at_job` | `` |  |
  | `at_memo` | `` |  |
  | `at_name` | `VAT (Input - Purchases)` |  |
  | `at_number` | `` |  |
  | `at_payee` | `` |  |
  | `at_payname` | `` |  |
  | `at_payslp` | `0.0` |  |
  | `at_postgrp` | `0.0` |  |
  | `at_project` | `` |  |
  | `at_pstdate` | `2024-05-01T00:00:00` |  |
  | `at_pysprn` | `0.0` |  |
  | `at_refer` | `test` |  |
  | `at_remit` | `0.0` |  |
  | `at_remove` | `0.0` |  |
  | `at_sort` | `` |  |
  | `at_srcco` | `Z` |  |
  | `at_sysdate` | `2024-05-01T00:00:00` |  |
  | `at_tperiod` | `1.0` | From nominal calendar |
  | `at_type` | `1.0` |  |
  | `at_unique` | `_7FL0VO97M` | Base-36 unique ID |
  | `at_value` | `-1667.0` |  |
  | `at_vattycd` | `` |  |

**atype**
  *1 row(s) modified*

**idtab**
  *1 row(s) modified*

**nacnt**
  *3 row(s) modified*

**nbank**
  *1 row(s) modified*

**ndetail**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `nt_acnt` | `C310` |  |
  | `nt_cdesc` | `` |  |
  | `nt_cmnt` | `test` |  |
  | `nt_cntr` | `` |  |
  | `nt_consol` | `0.0` |  |
  | `nt_distrib` | `0.0` |  |
  | `nt_entr` | `2024-05-01T00:00:00` |  |
  | `nt_fcdec` | `0.0` |  |
  | `nt_fcmult` | `0.0` |  |
  | `nt_fcrate` | `0.0` |  |
  | `nt_fcurr` | `` | Foreign currency field |
  | `nt_fvalue` | `0.0` |  |
  | `nt_inp` | `TEST` |  |
  | `nt_job` | `` |  |
  | `nt_jrnl` | `3452.0` | From nparm.np_nexjrnl |
  | `nt_period` | `5.0` | From nominal calendar |
  | `nt_perpost` | `0.0` |  |
  | `nt_posttyp` | `A` |  |
  | `nt_prevyr` | `0.0` |  |
  | `nt_project` | `` |  |
  | `nt_pstgrp` | `1.0` |  |
  | `nt_pstid` | `_7FL0VOOTP` |  |
  | `nt_recjrnl` | `0.0` | From nparm.np_nexjrnl |
  | `nt_rectify` | `0.0` |  |
  | `nt_recurr` | `0.0` |  |
  | `nt_ref` | `` |  |
  | `nt_rvrse` | `0.0` |  |
  | `nt_srcco` | `Z` |  |
  | `nt_subt` | `03` |  |
  | `nt_trnref` | `` |  |
  | `nt_trtype` | `A` |  |
  | `nt_type` | `10` |  |
  | `nt_value` | `-100.0` |  |
  | `nt_vatanal` | `0.0` |  |
  | `nt_year` | `2024.0` |  |

**nextid**
  *10 row(s) modified*

**nhist**
  *3 row(s) modified*

**njmemo**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `nj_binrep` | `0.0` |  |
  | `nj_image` | `` |  |
  | `nj_journal` | `3452.0` | From nparm.np_nexjrnl |
  | `nj_memo` | `ÿ<<JOURNAL_DATA_ONLY>>ÿ` |  |
  | `nj_txtrep` | `Cashbook Ledger Transfer` |  |

**nsubt**
  *3 row(s) modified*

**ntype**
  *3 row(s) modified*

**nvat**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `nv_acnt` | `S120` |  |
  | `nv_advance` | `0.0` |  |
  | `nv_cntr` | `ADM` |  |
  | `nv_comment` | `` |  |
  | `nv_crdate` | `2024-05-01T00:00:00` |  |
  | `nv_date` | `2024-05-01T00:00:00` |  |
  | `nv_ref` | `test` |  |
  | `nv_taxdate` | `2024-05-01T00:00:00` |  |
  | `nv_type` | `I` |  |
  | `nv_value` | `100.0` |  |
  | `nv_vatcode` | `1` |  |
  | `nv_vatctry` | `H` |  |
  | `nv_vatrate` | `20.0` |  |
  | `nv_vattype` | `P` |  |
  | `nv_vatval` | `16.67` |  |

**zpool**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `sp_cby` | `TEST` |  |
  | `sp_cdate` | `2026-04-01T00:00:00` |  |
  | `sp_ctime` | `14:46` |  |
  | `sp_desc` | `dfg` |  |
  | `sp_file` | `TESTSDGV` |  |
  | `sp_origin` | `` |  |
  | `sp_pby` | `` |  |
  | `sp_platfrm` | `32BIT` |  |
  | `sp_printer` | `PDF:` |  |
  | `sp_ptime` | `` |  |
  | `sp_rephite` | `0.0` |  |
  | `sp_repwide` | `0.0` |  |

---

### Sales Receipt — BACS

*Receipt from customer via BACS. Creates: aentry, atran, stran, ntran, anoml, nacnt, nbank, sname balance update.*

**aentry**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `ae_acnt` | `C310` |  |
  | `ae_batchid` | `0.0` |  |
  | `ae_brwptr` | `` |  |
  | `ae_cbtype` | `R2` |  |
  | `ae_cntr` | `` |  |
  | `ae_comment` | `` |  |
  | `ae_complet` | `1.0` |  |
  | `ae_entref` | `rec` |  |
  | `ae_entry` | `R200000718` | From atype counter |
  | `ae_frstat` | `0.0` |  |
  | `ae_lstdate` | `2024-05-01T00:00:00` |  |
  | `ae_payid` | `0.0` |  |
  | `ae_postgrp` | `0.0` |  |
  | `ae_recbal` | `0.0` |  |
  | `ae_reclnum` | `0.0` |  |
  | `ae_remove` | `0.0` |  |
  | `ae_statln` | `0.0` |  |
  | `ae_tmpstat` | `0.0` |  |
  | `ae_tostat` | `0.0` |  |
  | `ae_value` | `2399.0` |  |
  | `sq_amtime` | `` |  |
  | `sq_amuser` | `` |  |
  | `sq_crdate` | `2026-04-01T00:00:00` |  |
  | `sq_crtime` | `12:46:51` |  |
  | `sq_cruser` | `TEST` |  |

**anoml**
  *2 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ax_comment` | `Adams Light Engineering Ltd   BACS` |  |
  | `ax_date` | `2024-05-01T00:00:00` |  |
  | `ax_done` | `` |  |
  | `ax_fcdec` | `0.0` |  |
  | `ax_fcmult` | `0.0` |  |
  | `ax_fcrate` | `0.0` |  |
  | `ax_fcurr` | `` | Foreign currency field |
  | `ax_fvalue` | `0.0` |  |
  | `ax_job` | `` |  |
  | `ax_jrnl` | `0.0` | From nparm.np_nexjrnl |
  | `ax_nacnt` | `C310` |  |
  | `ax_ncntr` | `` |  |
  | `ax_nlpdate` | `2024-05-01T00:00:00` |  |
  | `ax_project` | `` |  |
  | `ax_source` | `S` |  |
  | `ax_srcco` | `Z` |  |
  | `ax_tref` | `rec` |  |
  | `ax_unique` | `_7FL0RDZ8K` | Base-36 unique ID |
  | `ax_value` | `23.99` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ax_comment` | `Adams Light Engineering Ltd   BACS` |  |
  | `ax_date` | `2024-05-01T00:00:00` |  |
  | `ax_done` | `` |  |
  | `ax_fcdec` | `0.0` |  |
  | `ax_fcmult` | `0.0` |  |
  | `ax_fcrate` | `0.0` |  |
  | `ax_fcurr` | `` | Foreign currency field |
  | `ax_fvalue` | `0.0` |  |
  | `ax_job` | `` |  |
  | `ax_jrnl` | `0.0` | From nparm.np_nexjrnl |
  | `ax_nacnt` | `C110` |  |
  | `ax_ncntr` | `` |  |
  | `ax_nlpdate` | `2024-05-01T00:00:00` |  |
  | `ax_project` | `` |  |
  | `ax_source` | `S` |  |
  | `ax_srcco` | `Z` |  |
  | `ax_tref` | `rec` |  |
  | `ax_unique` | `_7FL0RDZ8K` | Base-36 unique ID |
  | `ax_value` | `-23.99` |  |

**atran**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `at_account` | `ADA0001` |  |
  | `at_acnt` | `C310` |  |
  | `at_atpycd` | `` |  |
  | `at_bacprn` | `0.0` |  |
  | `at_bic` | `` |  |
  | `at_bsname` | `` |  |
  | `at_bsref` | `` |  |
  | `at_cash` | `0.0` |  |
  | `at_cbtype` | `R2` |  |
  | `at_ccauth` | `0` |  |
  | `at_ccdno` | `` |  |
  | `at_ccdprn` | `0.0` |  |
  | `at_chqlst` | `0.0` |  |
  | `at_chqprn` | `0.0` |  |
  | `at_cntr` | `` |  |
  | `at_comment` | `` |  |
  | `at_disc` | `0.0` |  |
  | `at_ecb` | `0.0` |  |
  | `at_ecbtype` | `` |  |
  | `at_entry` | `R200000718` | From atype counter |
  | `at_fcdec` | `2.0` | Foreign currency field |
  | `at_fcexch` | `1.0` | Foreign currency field |
  | `at_fcmult` | `0.0` |  |
  | `at_fcurr` | `` | Foreign currency field |
  | `at_iban` | `` |  |
  | `at_inputby` | `TEST` |  |
  | `at_job` | `` |  |
  | `at_memo` | `` |  |
  | `at_name` | `Adams Light Engineering Ltd` |  |
  | `at_number` | `` |  |
  | `at_payee` | `` |  |
  | `at_payname` | `` |  |
  | `at_payslp` | `0.0` |  |
  | `at_postgrp` | `0.0` |  |
  | `at_project` | `` |  |
  | `at_pstdate` | `2024-05-01T00:00:00` |  |
  | `at_pysprn` | `0.0` |  |
  | `at_refer` | `rec` |  |
  | `at_remit` | `0.0` |  |
  | `at_remove` | `0.0` |  |
  | `at_sort` | `` |  |
  | `at_srcco` | `Z` |  |
  | `at_sysdate` | `2024-05-01T00:00:00` |  |
  | `at_tperiod` | `1.0` | From nominal calendar |
  | `at_type` | `4.0` |  |
  | `at_unique` | `_7FL0RDZ8K` | Base-36 unique ID |
  | `at_value` | `2399.0` |  |
  | `at_vattycd` | `` |  |

**atype**
  *1 row(s) modified*

**nbank**
  *1 row(s) modified*

**nextid**
  *5 row(s) modified*

**salloc**
  *2 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `al_account` | `ADA0001` |  |
  | `al_acnt` | `C310` |  |
  | `al_adjsv` | `0.0` |  |
  | `al_advind` | `0.0` |  |
  | `al_cntr` | `` |  |
  | `al_date` | `2024-04-19T00:00:00` |  |
  | `al_fcurr` | `` | Foreign currency field |
  | `al_fdec` | `0.0` |  |
  | `al_fval` | `0.0` |  |
  | `al_payday` | `2024-05-01T00:00:00` |  |
  | `al_payflag` | `91.0` |  |
  | `al_payind` | `A` |  |
  | `al_preprd` | `0.0` |  |
  | `al_ref1` | `INV05188` |  |
  | `al_ref2` | `AHL-CONT-0722/AX001` |  |
  | `al_type` | `I` |  |
  | `al_unique` | `9213.0` | Base-36 unique ID |
  | `al_val` | `23.99` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `al_account` | `ADA0001` |  |
  | `al_acnt` | `C310` |  |
  | `al_adjsv` | `0.0` |  |
  | `al_advind` | `0.0` |  |
  | `al_cntr` | `` |  |
  | `al_date` | `2024-05-01T00:00:00` |  |
  | `al_fcurr` | `` | Foreign currency field |
  | `al_fdec` | `0.0` |  |
  | `al_fval` | `0.0` |  |
  | `al_payday` | `2024-05-01T00:00:00` |  |
  | `al_payflag` | `91.0` |  |
  | `al_payind` | `A` |  |
  | `al_preprd` | `0.0` |  |
  | `al_ref1` | `rec` |  |
  | `al_ref2` | `BACS` |  |
  | `al_type` | `R` |  |
  | `al_unique` | `9281.0` | Base-36 unique ID |
  | `al_val` | `-23.99` |  |

**sname**
  *1 row(s) modified*

**stran**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `jxrenewal` | `0.0` |  |
  | `jxservid` | `0.0` |  |
  | `st_account` | `ADA0001` |  |
  | `st_adjsv` | `0.0` |  |
  | `st_advallc` | `0.0` |  |
  | `st_advance` | `N` |  |
  | `st_binrep` | `0.0` |  |
  | `st_cash` | `0.0` |  |
  | `st_cbtype` | `R2` |  |
  | `st_crdate` | `2024-05-01T00:00:00` |  |
  | `st_custref` | `BACS` |  |
  | `st_delacc` | `` |  |
  | `st_dispute` | `0.0` |  |
  | `st_edi` | `0.0` |  |
  | `st_editx` | `0.0` |  |
  | `st_edivn` | `0.0` |  |
  | `st_entry` | `R200000718` | From atype counter |
  | `st_eurind` | `` |  |
  | `st_euro` | `0.0` |  |
  | `st_exttime` | `` |  |
  | `st_fadval` | `0.0` |  |
  | `st_fcbal` | `0.0` |  |
  | `st_fcdec` | `0.0` |  |
  | `st_fcmult` | `0.0` |  |
  | `st_fcrate` | `0.0` |  |
  | `st_fcurr` | `` | Foreign currency field |
  | `st_fcval` | `0.0` |  |
  | `st_fcvat` | `0.0` |  |
  | `st_fullamt` | `0.0` |  |
  | `st_fullcb` | `` |  |
  | `st_fullnar` | `` |  |
  | `st_gateid` | `0.0` |  |
  | `st_gatetr` | `0.0` |  |
  | `st_luptime` | `` |  |
  | `st_memo` | `Analysis of Receipt rec                   Amount        2...` |  |
  | `st_nlpdate` | `2024-05-01T00:00:00` |  |
  | `st_origcur` | `` |  |
  | `st_paid` | `A` |  |
  | `st_payadvl` | `0.0` |  |
  | `st_payday` | `2024-05-01T00:00:00` |  |
  | `st_payflag` | `91.0` |  |
  | `st_rcode` | `` |  |
  | `st_region` | `` |  |
  | `st_revchrg` | `0.0` |  |
  | `st_ruser` | `` |  |
  | `st_set1` | `0.0` |  |
  | `st_set1day` | `0.0` |  |
  | `st_set2` | `0.0` |  |
  | `st_set2day` | `0.0` |  |
  | `st_terr` | `` |  |
  | `st_trbal` | `0.0` |  |
  | `st_trdate` | `2024-05-01T00:00:00` |  |
  | `st_trref` | `rec` |  |
  | `st_trtype` | `R` |  |
  | `st_trvalue` | `-23.99` |  |
  | `st_txtrep` | `` |  |
  | `st_type` | `` |  |
  | `st_unique` | `_7FL0RDZ8K` | Base-36 unique ID |
  | `st_vatval` | `0.0` |  |

  *1 row(s) modified*

**zlock**
  *1 row(s) modified*

---

### Sales Receipt — Cheque

*Receipt from customer via cheque.*

**aentry**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `ae_acnt` | `C310` |  |
  | `ae_batchid` | `0.0` |  |
  | `ae_brwptr` | `` |  |
  | `ae_cbtype` | `R2` |  |
  | `ae_cntr` | `` |  |
  | `ae_comment` | `` |  |
  | `ae_complet` | `1.0` |  |
  | `ae_entref` | `pay` |  |
  | `ae_entry` | `R200000717` | From atype counter |
  | `ae_frstat` | `0.0` |  |
  | `ae_lstdate` | `2024-05-01T00:00:00` |  |
  | `ae_payid` | `0.0` |  |
  | `ae_postgrp` | `0.0` |  |
  | `ae_recbal` | `0.0` |  |
  | `ae_reclnum` | `0.0` |  |
  | `ae_remove` | `0.0` |  |
  | `ae_statln` | `0.0` |  |
  | `ae_tmpstat` | `0.0` |  |
  | `ae_tostat` | `0.0` |  |
  | `ae_value` | `2399.0` |  |
  | `sq_amtime` | `` |  |
  | `sq_amuser` | `` |  |
  | `sq_crdate` | `2026-04-01T00:00:00` |  |
  | `sq_crtime` | `12:41:12` |  |
  | `sq_cruser` | `TEST` |  |

**anoml**
  *2 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ax_comment` | `Adams Light Engineering Ltd   BACS` |  |
  | `ax_date` | `2024-05-01T00:00:00` |  |
  | `ax_done` | `` |  |
  | `ax_fcdec` | `0.0` |  |
  | `ax_fcmult` | `0.0` |  |
  | `ax_fcrate` | `0.0` |  |
  | `ax_fcurr` | `` | Foreign currency field |
  | `ax_fvalue` | `0.0` |  |
  | `ax_job` | `` |  |
  | `ax_jrnl` | `0.0` | From nparm.np_nexjrnl |
  | `ax_nacnt` | `C310` |  |
  | `ax_ncntr` | `` |  |
  | `ax_nlpdate` | `2024-05-01T00:00:00` |  |
  | `ax_project` | `` |  |
  | `ax_source` | `S` |  |
  | `ax_srcco` | `Z` |  |
  | `ax_tref` | `pay` |  |
  | `ax_unique` | `_7FL0R6FLA` | Base-36 unique ID |
  | `ax_value` | `23.99` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ax_comment` | `Adams Light Engineering Ltd   BACS` |  |
  | `ax_date` | `2024-05-01T00:00:00` |  |
  | `ax_done` | `` |  |
  | `ax_fcdec` | `0.0` |  |
  | `ax_fcmult` | `0.0` |  |
  | `ax_fcrate` | `0.0` |  |
  | `ax_fcurr` | `` | Foreign currency field |
  | `ax_fvalue` | `0.0` |  |
  | `ax_job` | `` |  |
  | `ax_jrnl` | `0.0` | From nparm.np_nexjrnl |
  | `ax_nacnt` | `C110` |  |
  | `ax_ncntr` | `` |  |
  | `ax_nlpdate` | `2024-05-01T00:00:00` |  |
  | `ax_project` | `` |  |
  | `ax_source` | `S` |  |
  | `ax_srcco` | `Z` |  |
  | `ax_tref` | `pay` |  |
  | `ax_unique` | `_7FL0R6FLA` | Base-36 unique ID |
  | `ax_value` | `-23.99` |  |

**atran**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `at_account` | `ADA0001` |  |
  | `at_acnt` | `C310` |  |
  | `at_atpycd` | `` |  |
  | `at_bacprn` | `0.0` |  |
  | `at_bic` | `` |  |
  | `at_bsname` | `` |  |
  | `at_bsref` | `` |  |
  | `at_cash` | `0.0` |  |
  | `at_cbtype` | `R2` |  |
  | `at_ccauth` | `0` |  |
  | `at_ccdno` | `` |  |
  | `at_ccdprn` | `0.0` |  |
  | `at_chqlst` | `0.0` |  |
  | `at_chqprn` | `0.0` |  |
  | `at_cntr` | `` |  |
  | `at_comment` | `` |  |
  | `at_disc` | `0.0` |  |
  | `at_ecb` | `0.0` |  |
  | `at_ecbtype` | `` |  |
  | `at_entry` | `R200000717` | From atype counter |
  | `at_fcdec` | `2.0` | Foreign currency field |
  | `at_fcexch` | `1.0` | Foreign currency field |
  | `at_fcmult` | `0.0` |  |
  | `at_fcurr` | `` | Foreign currency field |
  | `at_iban` | `` |  |
  | `at_inputby` | `TEST` |  |
  | `at_job` | `` |  |
  | `at_memo` | `` |  |
  | `at_name` | `Adams Light Engineering Ltd` |  |
  | `at_number` | `` |  |
  | `at_payee` | `` |  |
  | `at_payname` | `` |  |
  | `at_payslp` | `0.0` |  |
  | `at_postgrp` | `0.0` |  |
  | `at_project` | `` |  |
  | `at_pstdate` | `2024-05-01T00:00:00` |  |
  | `at_pysprn` | `0.0` |  |
  | `at_refer` | `pay` |  |
  | `at_remit` | `0.0` |  |
  | `at_remove` | `0.0` |  |
  | `at_sort` | `` |  |
  | `at_srcco` | `Z` |  |
  | `at_sysdate` | `2024-05-01T00:00:00` |  |
  | `at_tperiod` | `1.0` | From nominal calendar |
  | `at_type` | `4.0` |  |
  | `at_unique` | `_7FL0R6FLA` | Base-36 unique ID |
  | `at_value` | `2399.0` |  |
  | `at_vattycd` | `` |  |

**atype**
  *1 row(s) modified*

**nbank**
  *1 row(s) modified*

**nextid**
  *5 row(s) modified*

**salloc**
  *2 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `al_account` | `ADA0001` |  |
  | `al_acnt` | `C310` |  |
  | `al_adjsv` | `0.0` |  |
  | `al_advind` | `0.0` |  |
  | `al_cntr` | `` |  |
  | `al_date` | `2024-05-17T00:00:00` |  |
  | `al_fcurr` | `` | Foreign currency field |
  | `al_fdec` | `0.0` |  |
  | `al_fval` | `0.0` |  |
  | `al_payday` | `2024-05-01T00:00:00` |  |
  | `al_payflag` | `90.0` |  |
  | `al_payind` | `A` |  |
  | `al_preprd` | `0.0` |  |
  | `al_ref1` | `INV05214` |  |
  | `al_ref2` | `AHL-CONT-0722/AX001` |  |
  | `al_type` | `I` |  |
  | `al_unique` | `9264.0` | Base-36 unique ID |
  | `al_val` | `23.99` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `al_account` | `ADA0001` |  |
  | `al_acnt` | `C310` |  |
  | `al_adjsv` | `0.0` |  |
  | `al_advind` | `0.0` |  |
  | `al_cntr` | `` |  |
  | `al_date` | `2024-05-01T00:00:00` |  |
  | `al_fcurr` | `` | Foreign currency field |
  | `al_fdec` | `0.0` |  |
  | `al_fval` | `0.0` |  |
  | `al_payday` | `2024-05-01T00:00:00` |  |
  | `al_payflag` | `90.0` |  |
  | `al_payind` | `A` |  |
  | `al_preprd` | `0.0` |  |
  | `al_ref1` | `pay` |  |
  | `al_ref2` | `BACS` |  |
  | `al_type` | `R` |  |
  | `al_unique` | `9280.0` | Base-36 unique ID |
  | `al_val` | `-23.99` |  |

**sname**
  *1 row(s) modified*

**stran**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `jxrenewal` | `0.0` |  |
  | `jxservid` | `0.0` |  |
  | `st_account` | `ADA0001` |  |
  | `st_adjsv` | `0.0` |  |
  | `st_advallc` | `0.0` |  |
  | `st_advance` | `N` |  |
  | `st_binrep` | `0.0` |  |
  | `st_cash` | `0.0` |  |
  | `st_cbtype` | `R2` |  |
  | `st_crdate` | `2024-05-01T00:00:00` |  |
  | `st_custref` | `BACS` |  |
  | `st_delacc` | `` |  |
  | `st_dispute` | `0.0` |  |
  | `st_edi` | `0.0` |  |
  | `st_editx` | `0.0` |  |
  | `st_edivn` | `0.0` |  |
  | `st_entry` | `R200000717` | From atype counter |
  | `st_eurind` | `` |  |
  | `st_euro` | `0.0` |  |
  | `st_exttime` | `` |  |
  | `st_fadval` | `0.0` |  |
  | `st_fcbal` | `0.0` |  |
  | `st_fcdec` | `0.0` |  |
  | `st_fcmult` | `0.0` |  |
  | `st_fcrate` | `0.0` |  |
  | `st_fcurr` | `` | Foreign currency field |
  | `st_fcval` | `0.0` |  |
  | `st_fcvat` | `0.0` |  |
  | `st_fullamt` | `0.0` |  |
  | `st_fullcb` | `` |  |
  | `st_fullnar` | `` |  |
  | `st_gateid` | `0.0` |  |
  | `st_gatetr` | `0.0` |  |
  | `st_luptime` | `` |  |
  | `st_memo` | `Analysis of Receipt pay                   Amount        2...` |  |
  | `st_nlpdate` | `2024-05-01T00:00:00` |  |
  | `st_origcur` | `` |  |
  | `st_paid` | `A` |  |
  | `st_payadvl` | `0.0` |  |
  | `st_payday` | `2024-05-01T00:00:00` |  |
  | `st_payflag` | `90.0` |  |
  | `st_rcode` | `` |  |
  | `st_region` | `` |  |
  | `st_revchrg` | `0.0` |  |
  | `st_ruser` | `` |  |
  | `st_set1` | `0.0` |  |
  | `st_set1day` | `0.0` |  |
  | `st_set2` | `0.0` |  |
  | `st_set2day` | `0.0` |  |
  | `st_terr` | `` |  |
  | `st_trbal` | `0.0` |  |
  | `st_trdate` | `2024-05-01T00:00:00` |  |
  | `st_trref` | `pay` |  |
  | `st_trtype` | `R` |  |
  | `st_trvalue` | `-23.99` |  |
  | `st_txtrep` | `` |  |
  | `st_type` | `` |  |
  | `st_unique` | `_7FL0R6FLA` | Base-36 unique ID |
  | `st_vatval` | `0.0` |  |

  *1 row(s) modified*

**zlock**
  *1 row(s) modified*

---

### Sales Refund

*Refund to customer. Creates: aentry, atran, stran, ntran, anoml, nacnt, nbank, sname balance update. Opposite signs to receipt.*

**aentry**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `ae_acnt` | `C310` |  |
  | `ae_batchid` | `0.0` |  |
  | `ae_brwptr` | `` |  |
  | `ae_cbtype` | `P6` |  |
  | `ae_cntr` | `` |  |
  | `ae_comment` | `` |  |
  | `ae_complet` | `1.0` |  |
  | `ae_entref` | `test` |  |
  | `ae_entry` | `P600000039` | From atype counter |
  | `ae_frstat` | `0.0` |  |
  | `ae_lstdate` | `2024-05-01T00:00:00` |  |
  | `ae_payid` | `0.0` |  |
  | `ae_postgrp` | `0.0` |  |
  | `ae_recbal` | `0.0` |  |
  | `ae_reclnum` | `0.0` |  |
  | `ae_remove` | `0.0` |  |
  | `ae_statln` | `0.0` |  |
  | `ae_tmpstat` | `0.0` |  |
  | `ae_tostat` | `0.0` |  |
  | `ae_value` | `-2399.0` |  |
  | `sq_amtime` | `` |  |
  | `sq_amuser` | `` |  |
  | `sq_crdate` | `2026-04-01T00:00:00` |  |
  | `sq_crtime` | `12:50:07` |  |
  | `sq_cruser` | `TEST` |  |

**anoml**
  *2 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ax_comment` | `Anderson Car Factors Ltd      Refund` |  |
  | `ax_date` | `2024-05-01T00:00:00` |  |
  | `ax_done` | `` |  |
  | `ax_fcdec` | `0.0` |  |
  | `ax_fcmult` | `0.0` |  |
  | `ax_fcrate` | `0.0` |  |
  | `ax_fcurr` | `` | Foreign currency field |
  | `ax_fvalue` | `0.0` |  |
  | `ax_job` | `` |  |
  | `ax_jrnl` | `0.0` | From nparm.np_nexjrnl |
  | `ax_nacnt` | `C310` |  |
  | `ax_ncntr` | `` |  |
  | `ax_nlpdate` | `2024-05-01T00:00:00` |  |
  | `ax_project` | `` |  |
  | `ax_source` | `S` |  |
  | `ax_srcco` | `Z` |  |
  | `ax_tref` | `test` |  |
  | `ax_unique` | `_7FL0RIDYH` | Base-36 unique ID |
  | `ax_value` | `-23.99` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ax_comment` | `Anderson Car Factors Ltd      Refund` |  |
  | `ax_date` | `2024-05-01T00:00:00` |  |
  | `ax_done` | `` |  |
  | `ax_fcdec` | `0.0` |  |
  | `ax_fcmult` | `0.0` |  |
  | `ax_fcrate` | `0.0` |  |
  | `ax_fcurr` | `` | Foreign currency field |
  | `ax_fvalue` | `0.0` |  |
  | `ax_job` | `` |  |
  | `ax_jrnl` | `0.0` | From nparm.np_nexjrnl |
  | `ax_nacnt` | `C110` |  |
  | `ax_ncntr` | `` |  |
  | `ax_nlpdate` | `2024-05-01T00:00:00` |  |
  | `ax_project` | `` |  |
  | `ax_source` | `S` |  |
  | `ax_srcco` | `Z` |  |
  | `ax_tref` | `test` |  |
  | `ax_unique` | `_7FL0RIDYH` | Base-36 unique ID |
  | `ax_value` | `23.99` |  |

**atran**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `at_account` | `AND0001` |  |
  | `at_acnt` | `C310` |  |
  | `at_atpycd` | `` |  |
  | `at_bacprn` | `0.0` |  |
  | `at_bic` | `` |  |
  | `at_bsname` | `` |  |
  | `at_bsref` | `` |  |
  | `at_cash` | `0.0` |  |
  | `at_cbtype` | `P6` |  |
  | `at_ccauth` | `` |  |
  | `at_ccdno` | `` |  |
  | `at_ccdprn` | `0.0` |  |
  | `at_chqlst` | `0.0` |  |
  | `at_chqprn` | `0.0` |  |
  | `at_cntr` | `` |  |
  | `at_comment` | `` |  |
  | `at_disc` | `0.0` |  |
  | `at_ecb` | `0.0` |  |
  | `at_ecbtype` | `` |  |
  | `at_entry` | `P600000039` | From atype counter |
  | `at_fcdec` | `2.0` | Foreign currency field |
  | `at_fcexch` | `1.0` | Foreign currency field |
  | `at_fcmult` | `0.0` |  |
  | `at_fcurr` | `` | Foreign currency field |
  | `at_iban` | `` |  |
  | `at_inputby` | `TEST` |  |
  | `at_job` | `` |  |
  | `at_memo` | `` |  |
  | `at_name` | `Anderson Car Factors Ltd` |  |
  | `at_number` | `` |  |
  | `at_payee` | `` |  |
  | `at_payname` | `` |  |
  | `at_payslp` | `0.0` |  |
  | `at_postgrp` | `0.0` |  |
  | `at_project` | `` |  |
  | `at_pstdate` | `2024-05-01T00:00:00` |  |
  | `at_pysprn` | `0.0` |  |
  | `at_refer` | `test` |  |
  | `at_remit` | `0.0` |  |
  | `at_remove` | `0.0` |  |
  | `at_sort` | `` |  |
  | `at_srcco` | `Z` |  |
  | `at_sysdate` | `2024-05-01T00:00:00` |  |
  | `at_tperiod` | `1.0` | From nominal calendar |
  | `at_type` | `3.0` |  |
  | `at_unique` | `_7FL0RIDYH` | Base-36 unique ID |
  | `at_value` | `-2399.0` |  |
  | `at_vattycd` | `` |  |

**atype**
  *1 row(s) modified*

**nbank**
  *1 row(s) modified*

**nextid**
  *4 row(s) modified*

**sname**
  *1 row(s) modified*

**stran**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `jxrenewal` | `0.0` |  |
  | `jxservid` | `0.0` |  |
  | `st_account` | `AND0001` |  |
  | `st_adjsv` | `0.0` |  |
  | `st_advallc` | `0.0` |  |
  | `st_advance` | `N` |  |
  | `st_binrep` | `0.0` |  |
  | `st_cash` | `0.0` |  |
  | `st_cbtype` | `P6` |  |
  | `st_crdate` | `2024-05-01T00:00:00` |  |
  | `st_custref` | `Refund` |  |
  | `st_delacc` | `` |  |
  | `st_dispute` | `0.0` |  |
  | `st_edi` | `0.0` |  |
  | `st_editx` | `0.0` |  |
  | `st_edivn` | `0.0` |  |
  | `st_entry` | `P600000039` | From atype counter |
  | `st_eurind` | `` |  |
  | `st_euro` | `0.0` |  |
  | `st_exttime` | `` |  |
  | `st_fadval` | `0.0` |  |
  | `st_fcbal` | `0.0` |  |
  | `st_fcdec` | `0.0` |  |
  | `st_fcmult` | `0.0` |  |
  | `st_fcrate` | `0.0` |  |
  | `st_fcurr` | `` | Foreign currency field |
  | `st_fcval` | `0.0` |  |
  | `st_fcvat` | `0.0` |  |
  | `st_fullamt` | `0.0` |  |
  | `st_fullcb` | `` |  |
  | `st_fullnar` | `` |  |
  | `st_gateid` | `0.0` |  |
  | `st_gatetr` | `0.0` |  |
  | `st_luptime` | `` |  |
  | `st_memo` | `` |  |
  | `st_nlpdate` | `2024-05-01T00:00:00` |  |
  | `st_origcur` | `` |  |
  | `st_paid` | `` |  |
  | `st_payadvl` | `0.0` |  |
  | `st_payflag` | `0.0` |  |
  | `st_rcode` | `` |  |
  | `st_region` | `` |  |
  | `st_revchrg` | `0.0` |  |
  | `st_ruser` | `` |  |
  | `st_set1` | `0.0` |  |
  | `st_set1day` | `0.0` |  |
  | `st_set2` | `0.0` |  |
  | `st_set2day` | `0.0` |  |
  | `st_terr` | `` |  |
  | `st_trbal` | `23.99` |  |
  | `st_trdate` | `2024-05-01T00:00:00` |  |
  | `st_trref` | `test` |  |
  | `st_trtype` | `F` |  |
  | `st_trvalue` | `23.99` |  |
  | `st_txtrep` | `` |  |
  | `st_type` | `` |  |
  | `st_unique` | `_7FL0RIDYH` | Base-36 unique ID |
  | `st_vatval` | `0.0` |  |

**zlock**
  *1 row(s) modified*

---

### Transfer

**aentry**
  *2 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ae_acnt` | `C310` |  |
  | `ae_batchid` | `0.0` |  |
  | `ae_brwptr` | `` |  |
  | `ae_cbtype` | `T1` |  |
  | `ae_cntr` | `` |  |
  | `ae_comment` | `` |  |
  | `ae_complet` | `1.0` |  |
  | `ae_entref` | `test` |  |
  | `ae_entry` | `T100000104` | From atype counter |
  | `ae_frstat` | `0.0` |  |
  | `ae_lstdate` | `2024-05-01T00:00:00` |  |
  | `ae_payid` | `0.0` |  |
  | `ae_postgrp` | `0.0` |  |
  | `ae_recbal` | `0.0` |  |
  | `ae_reclnum` | `0.0` |  |
  | `ae_remove` | `0.0` |  |
  | `ae_statln` | `0.0` |  |
  | `ae_tmpstat` | `0.0` |  |
  | `ae_tostat` | `0.0` |  |
  | `ae_value` | `-4000.0` |  |
  | `sq_amtime` | `` |  |
  | `sq_amuser` | `` |  |
  | `sq_crdate` | `2024-05-01T00:00:00` |  |
  | `sq_crtime` | `13:46:10` |  |
  | `sq_cruser` | `TEST` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ae_acnt` | `C315` |  |
  | `ae_batchid` | `0.0` |  |
  | `ae_brwptr` | `` |  |
  | `ae_cbtype` | `T1` |  |
  | `ae_cntr` | `` |  |
  | `ae_comment` | `` |  |
  | `ae_complet` | `1.0` |  |
  | `ae_entref` | `test` |  |
  | `ae_entry` | `T100000105` | From atype counter |
  | `ae_frstat` | `0.0` |  |
  | `ae_lstdate` | `2024-05-01T00:00:00` |  |
  | `ae_payid` | `0.0` |  |
  | `ae_postgrp` | `0.0` |  |
  | `ae_recbal` | `0.0` |  |
  | `ae_reclnum` | `0.0` |  |
  | `ae_remove` | `0.0` |  |
  | `ae_statln` | `0.0` |  |
  | `ae_tmpstat` | `0.0` |  |
  | `ae_tostat` | `0.0` |  |
  | `ae_value` | `4000.0` |  |
  | `sq_amtime` | `` |  |
  | `sq_amuser` | `` |  |
  | `sq_crdate` | `2024-05-01T00:00:00` |  |
  | `sq_crtime` | `13:46:10` |  |
  | `sq_cruser` | `TEST` |  |

**anoml**
  *2 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ax_comment` | `test` |  |
  | `ax_date` | `2024-05-01T00:00:00` |  |
  | `ax_done` | `` |  |
  | `ax_fcdec` | `2.0` | Foreign currency field |
  | `ax_fcmult` | `0.0` |  |
  | `ax_fcrate` | `1.0` | Foreign currency field |
  | `ax_fcurr` | `` | Foreign currency field |
  | `ax_fvalue` | `4000.0` |  |
  | `ax_job` | `` |  |
  | `ax_jrnl` | `0.0` | From nparm.np_nexjrnl |
  | `ax_nacnt` | `C315` |  |
  | `ax_ncntr` | `` |  |
  | `ax_nlpdate` | `2024-05-01T00:00:00` |  |
  | `ax_project` | `` |  |
  | `ax_source` | `A` |  |
  | `ax_srcco` | `Z` |  |
  | `ax_tref` | `test` |  |
  | `ax_unique` | `_7FL0TIH1C` | Base-36 unique ID |
  | `ax_value` | `40.0` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ax_comment` | `test` |  |
  | `ax_date` | `2024-05-01T00:00:00` |  |
  | `ax_done` | `` |  |
  | `ax_fcdec` | `2.0` | Foreign currency field |
  | `ax_fcmult` | `0.0` |  |
  | `ax_fcrate` | `1.0` | Foreign currency field |
  | `ax_fcurr` | `` | Foreign currency field |
  | `ax_fvalue` | `-4000.0` |  |
  | `ax_job` | `` |  |
  | `ax_jrnl` | `0.0` | From nparm.np_nexjrnl |
  | `ax_nacnt` | `C310` |  |
  | `ax_ncntr` | `` |  |
  | `ax_nlpdate` | `2024-05-01T00:00:00` |  |
  | `ax_project` | `` |  |
  | `ax_source` | `A` |  |
  | `ax_srcco` | `Z` |  |
  | `ax_tref` | `test` |  |
  | `ax_unique` | `_7FL0TIH1C` | Base-36 unique ID |
  | `ax_value` | `-40.0` |  |

**atran**
  *2 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `at_account` | `C315` |  |
  | `at_acnt` | `C310` |  |
  | `at_atpycd` | `` |  |
  | `at_bacprn` | `0.0` |  |
  | `at_bic` | `` |  |
  | `at_bsname` | `` |  |
  | `at_bsref` | `` |  |
  | `at_cash` | `0.0` |  |
  | `at_cbtype` | `T1` |  |
  | `at_ccauth` | `` |  |
  | `at_ccdno` | `` |  |
  | `at_ccdprn` | `0.0` |  |
  | `at_chqlst` | `0.0` |  |
  | `at_chqprn` | `0.0` |  |
  | `at_cntr` | `` |  |
  | `at_comment` | `test` |  |
  | `at_disc` | `0.0` |  |
  | `at_ecb` | `0.0` |  |
  | `at_ecbpayd` | `2024-05-01T00:00:00` |  |
  | `at_ecbtype` | `` |  |
  | `at_entry` | `T100000104` | From atype counter |
  | `at_fcdec` | `2.0` | Foreign currency field |
  | `at_fcexch` | `1.0` | Foreign currency field |
  | `at_fcmult` | `0.0` |  |
  | `at_fcurr` | `` | Foreign currency field |
  | `at_iban` | `` |  |
  | `at_inputby` | `TEST` |  |
  | `at_job` | `` |  |
  | `at_memo` | `` |  |
  | `at_name` | `Second Bank Current Account` |  |
  | `at_number` | `32873945` |  |
  | `at_payee` | `` |  |
  | `at_payname` | `` |  |
  | `at_payslp` | `0.0` |  |
  | `at_postgrp` | `0.0` |  |
  | `at_project` | `` |  |
  | `at_pstdate` | `2024-05-01T00:00:00` |  |
  | `at_pysprn` | `0.0` |  |
  | `at_refer` | `test` |  |
  | `at_remit` | `0.0` |  |
  | `at_remove` | `0.0` |  |
  | `at_sort` | `33-44-99` |  |
  | `at_srcco` | `Z` |  |
  | `at_sysdate` | `2024-05-01T00:00:00` |  |
  | `at_tperiod` | `1.0` | From nominal calendar |
  | `at_type` | `8.0` |  |
  | `at_unique` | `_7FL0TIH1C` | Base-36 unique ID |
  | `at_value` | `-4000.0` |  |
  | `at_vattycd` | `` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `at_account` | `C310` |  |
  | `at_acnt` | `C315` |  |
  | `at_atpycd` | `` |  |
  | `at_bacprn` | `0.0` |  |
  | `at_bic` | `` |  |
  | `at_bsname` | `` |  |
  | `at_bsref` | `` |  |
  | `at_cash` | `0.0` |  |
  | `at_cbtype` | `T1` |  |
  | `at_ccauth` | `` |  |
  | `at_ccdno` | `` |  |
  | `at_ccdprn` | `0.0` |  |
  | `at_chqlst` | `0.0` |  |
  | `at_chqprn` | `0.0` |  |
  | `at_cntr` | `` |  |
  | `at_comment` | `test` |  |
  | `at_disc` | `0.0` |  |
  | `at_ecb` | `0.0` |  |
  | `at_ecbpayd` | `2024-05-01T00:00:00` |  |
  | `at_ecbtype` | `` |  |
  | `at_entry` | `T100000105` | From atype counter |
  | `at_fcdec` | `2.0` | Foreign currency field |
  | `at_fcexch` | `1.0` | Foreign currency field |
  | `at_fcmult` | `0.0` |  |
  | `at_fcurr` | `` | Foreign currency field |
  | `at_iban` | `` |  |
  | `at_inputby` | `TEST` |  |
  | `at_job` | `` |  |
  | `at_memo` | `` |  |
  | `at_name` | `Main Bank Current Account` |  |
  | `at_number` | `` |  |
  | `at_payee` | `` |  |
  | `at_payname` | `` |  |
  | `at_payslp` | `0.0` |  |
  | `at_postgrp` | `0.0` |  |
  | `at_project` | `` |  |
  | `at_pstdate` | `2024-05-01T00:00:00` |  |
  | `at_pysprn` | `0.0` |  |
  | `at_refer` | `test` |  |
  | `at_remit` | `0.0` |  |
  | `at_remove` | `0.0` |  |
  | `at_sort` | `` |  |
  | `at_srcco` | `Z` |  |
  | `at_sysdate` | `2024-05-01T00:00:00` |  |
  | `at_tperiod` | `1.0` | From nominal calendar |
  | `at_type` | `8.0` |  |
  | `at_unique` | `_7FL0TIH1C` | Base-36 unique ID |
  | `at_value` | `4000.0` |  |
  | `at_vattycd` | `` |  |

**atype**
  *1 row(s) modified*

**nbank**
  *2 row(s) modified*

**nextid**
  *3 row(s) modified*

**zlock**
  *1 row(s) modified*

---

### create foreign currency bank account

**abatch**
  *3 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ab_account` | `FC001` |  |
  | `ab_centre` | `` |  |
  | `ab_complet` | `0.0` |  |
  | `ab_entry` | `P200000429` | From atype counter |
  | `ab_type` | `P2` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ab_account` | `FC001` |  |
  | `ab_centre` | `` |  |
  | `ab_complet` | `0.0` |  |
  | `ab_entry` | `P500000731` | From atype counter |
  | `ab_type` | `P5` |  |

  Row 3:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ab_account` | `FC001` |  |
  | `ab_centre` | `` |  |
  | `ab_complet` | `0.0` |  |
  | `ab_entry` | `R100000232` | From atype counter |
  | `ab_type` | `R1` |  |

**nacnt**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `na_acnt` | `FC001` |  |
  | `na_allwjob` | `0.0` |  |
  | `na_allwprj` | `0.0` |  |
  | `na_balc01` | `0.0` |  |
  | `na_balc02` | `0.0` |  |
  | `na_balc03` | `0.0` |  |
  | `na_balc04` | `0.0` |  |
  | `na_balc05` | `0.0` |  |
  | `na_balc06` | `0.0` |  |
  | `na_balc07` | `0.0` |  |
  | `na_balc08` | `0.0` |  |
  | `na_balc09` | `0.0` |  |
  | `na_balc10` | `0.0` |  |
  | `na_balc11` | `0.0` |  |
  | `na_balc12` | `0.0` |  |
  | `na_balc13` | `0.0` |  |
  | `na_balc14` | `0.0` |  |
  | `na_balc15` | `0.0` |  |
  | `na_balc16` | `0.0` |  |
  | `na_balc17` | `0.0` |  |
  | `na_balc18` | `0.0` |  |
  | `na_balc19` | `0.0` |  |
  | `na_balc20` | `0.0` |  |
  | `na_balc21` | `0.0` |  |
  | `na_balc22` | `0.0` |  |
  | `na_balc23` | `0.0` |  |
  | `na_balc24` | `0.0` |  |
  | `na_balp01` | `0.0` |  |
  | `na_balp02` | `0.0` |  |
  | `na_balp03` | `0.0` |  |
  | `na_balp04` | `0.0` |  |
  | `na_balp05` | `0.0` |  |
  | `na_balp06` | `0.0` |  |
  | `na_balp07` | `0.0` |  |
  | `na_balp08` | `0.0` |  |
  | `na_balp09` | `0.0` |  |
  | `na_balp10` | `0.0` |  |
  | `na_balp11` | `0.0` |  |
  | `na_balp12` | `0.0` |  |
  | `na_balp13` | `0.0` |  |
  | `na_balp14` | `0.0` |  |
  | `na_balp15` | `0.0` |  |
  | `na_balp16` | `0.0` |  |
  | `na_balp17` | `0.0` |  |
  | `na_balp18` | `0.0` |  |
  | `na_balp19` | `0.0` |  |
  | `na_balp20` | `0.0` |  |
  | `na_balp21` | `0.0` |  |
  | `na_balp22` | `0.0` |  |
  | `na_balp23` | `0.0` |  |
  | `na_balp24` | `0.0` |  |
  | `na_cntr` | `` |  |
  | `na_comm` | `0.0` |  |
  | `na_desc` | `FC Bank` |  |
  | `na_extcode` | `` |  |
  | `na_fbalc01` | `0.0` |  |
  | `na_fbalc02` | `0.0` |  |
  | `na_fbalc03` | `0.0` |  |
  | `na_fbalc04` | `0.0` |  |
  | `na_fbalc05` | `0.0` |  |
  | `na_fbalc06` | `0.0` |  |
  | `na_fbalc07` | `0.0` |  |
  | `na_fbalc08` | `0.0` |  |
  | `na_fbalc09` | `0.0` |  |
  | `na_fbalc10` | `0.0` |  |
  | `na_fbalc11` | `0.0` |  |
  | `na_fbalc12` | `0.0` |  |
  | `na_fbalc13` | `0.0` |  |
  | `na_fbalc14` | `0.0` |  |
  | `na_fbalc15` | `0.0` |  |
  | `na_fbalc16` | `0.0` |  |
  | `na_fbalc17` | `0.0` |  |
  | `na_fbalc18` | `0.0` |  |
  | `na_fbalc19` | `0.0` |  |
  | `na_fbalc20` | `0.0` |  |
  | `na_fbalc21` | `0.0` |  |
  | `na_fbalc22` | `0.0` |  |
  | `na_fbalc23` | `0.0` |  |
  | `na_fbalc24` | `0.0` |  |
  | `na_fbalp01` | `0.0` |  |
  | `na_fbalp02` | `0.0` |  |
  | `na_fbalp03` | `0.0` |  |
  | `na_fbalp04` | `0.0` |  |
  | `na_fbalp05` | `0.0` |  |
  | `na_fbalp06` | `0.0` |  |
  | `na_fbalp07` | `0.0` |  |
  | `na_fbalp08` | `0.0` |  |
  | `na_fbalp09` | `0.0` |  |
  | `na_fbalp10` | `0.0` |  |
  | `na_fbalp11` | `0.0` |  |
  | `na_fbalp12` | `0.0` |  |
  | `na_fbalp13` | `0.0` |  |
  | `na_fbalp14` | `0.0` |  |
  | `na_fbalp15` | `0.0` |  |
  | `na_fbalp16` | `0.0` |  |
  | `na_fbalp17` | `0.0` |  |
  | `na_fbalp18` | `0.0` |  |
  | `na_fbalp19` | `0.0` |  |
  | `na_fbalp20` | `0.0` |  |
  | `na_fbalp21` | `0.0` |  |
  | `na_fbalp22` | `0.0` |  |
  | `na_fbalp23` | `0.0` |  |
  | `na_fbalp24` | `0.0` |  |
  | `na_fcdec` | `2.0` | Foreign currency field |
  | `na_fcmult` | `0.0` |  |
  | `na_fcrate` | `1.190476` | Foreign currency field |
  | `na_fcurr` | `EUR` | Foreign currency field |
  | `na_fprycr` | `0.0` |  |
  | `na_fprydr` | `0.0` |  |
  | `na_fptdcr` | `0.0` |  |
  | `na_fptddr` | `0.0` |  |
  | `na_fytdcr` | `0.0` |  |
  | `na_fytddr` | `0.0` |  |
  | `na_job` | `` |  |
  | `na_key1` | `` |  |
  | `na_key2` | `` |  |
  | `na_key3` | `` |  |
  | `na_key4` | `` |  |
  | `na_memo` | `` |  |
  | `na_open` | `0.0` |  |
  | `na_post` | `0.0` |  |
  | `na_project` | `` |  |
  | `na_prycr` | `0.0` |  |
  | `na_prydr` | `0.0` |  |
  | `na_ptdcr` | `0.0` |  |
  | `na_ptddr` | `0.0` |  |
  | `na_redist` | `0.0` |  |
  | `na_repkey1` | `` |  |
  | `na_repkey2` | `` |  |
  | `na_repkey3` | `` |  |
  | `na_repkey4` | `` |  |
  | `na_repkey5` | `` |  |
  | `na_subt` | `` |  |
  | `na_type` | `` |  |
  | `na_ytdcr` | `0.0` |  |
  | `na_ytddr` | `0.0` |  |
  | `sq_private` | `0.0` |  |

**nbank**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `nk_acnt` | `FC001` |  |
  | `nk_addr1` | `bank` |  |
  | `nk_addr2` | `` |  |
  | `nk_addr3` | `` |  |
  | `nk_addr4` | `` |  |
  | `nk_bic` | `` |  |
  | `nk_bkname` | `euro bank` |  |
  | `nk_chqrep` | `` |  |
  | `nk_cntr` | `` |  |
  | `nk_contact` | `` |  |
  | `nk_curbal` | `0.0` |  |
  | `nk_desc` | `FC Bank` |  |
  | `nk_dwnlck` | `` |  |
  | `nk_ecb` | `` |  |
  | `nk_ecbdwn` | `0.0` |  |
  | `nk_ecbpay` | `0.0` |  |
  | `nk_ecbrec` | `0.0` |  |
  | `nk_email` | `` |  |
  | `nk_faxno` | `` |  |
  | `nk_fcdec` | `2.0` | Foreign currency field |
  | `nk_fcurr` | `EUR` | Foreign currency field |
  | `nk_iban` | `` |  |
  | `nk_key1` | `FC` |  |
  | `nk_key2` | `BANK` |  |
  | `nk_key3` | `` |  |
  | `nk_key4` | `` |  |
  | `nk_lstchq` | `1.0` |  |
  | `nk_lstpslp` | `1.0` |  |
  | `nk_lstrecl` | `1.0` |  |
  | `nk_lststno` | `1.0` |  |
  | `nk_matlock` | `` |  |
  | `nk_notice` | `0.0` |  |
  | `nk_number` | `99999999` |  |
  | `nk_ovrdrft` | `0.0` |  |
  | `nk_petty` | `0.0` |  |
  | `nk_private` | `0.0` |  |
  | `nk_pstcode` | `` |  |
  | `nk_recbal` | `0.0` |  |
  | `nk_reccfwd` | `0.0` |  |
  | `nk_reclnum` | `0.0` |  |
  | `nk_reclock` | `` |  |
  | `nk_recstfr` | `0.0` |  |
  | `nk_recstln` | `0.0` |  |
  | `nk_recstto` | `0.0` |  |
  | `nk_sepa` | `0.0` |  |
  | `nk_sepctry` | `` |  |
  | `nk_sort` | `99-99-99` |  |
  | `nk_teleno` | `` |  |
  | `nk_title` | `` |  |
  | `nk_wwwpage` | `` |  |
  | `sq_memo` | `` |  |

**nextid**
  *3 row(s) modified*

---

### foreign currency payment

*Nominal Posting
*

**aentry**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `ae_acnt` | `FC001` |  |
  | `ae_batchid` | `0.0` |  |
  | `ae_brwptr` | `` |  |
  | `ae_cbtype` | `P1` |  |
  | `ae_cntr` | `` |  |
  | `ae_comment` | `test` |  |
  | `ae_complet` | `1.0` |  |
  | `ae_entref` | `test` |  |
  | `ae_entry` | `P100000770` | From atype counter |
  | `ae_frstat` | `0.0` |  |
  | `ae_lstdate` | `2024-05-20T00:00:00` |  |
  | `ae_payid` | `0.0` |  |
  | `ae_postgrp` | `0.0` |  |
  | `ae_recbal` | `0.0` |  |
  | `ae_reclnum` | `0.0` |  |
  | `ae_remove` | `0.0` |  |
  | `ae_statln` | `0.0` |  |
  | `ae_tmpstat` | `0.0` |  |
  | `ae_tostat` | `0.0` |  |
  | `ae_value` | `-5000.0` |  |
  | `sq_amtime` | `` |  |
  | `sq_amuser` | `` |  |
  | `sq_crdate` | `2026-03-31T00:00:00` |  |
  | `sq_crtime` | `21:14:06` |  |
  | `sq_cruser` | `TEST` |  |

**anoml**
  *2 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ax_comment` | `test` |  |
  | `ax_date` | `2024-05-20T00:00:00` |  |
  | `ax_done` | `Y` | NL transfer complete |
  | `ax_fcdec` | `2.0` | Foreign currency field |
  | `ax_fcmult` | `0.0` |  |
  | `ax_fcrate` | `1.190476` | Foreign currency field |
  | `ax_fcurr` | `EUR` | Foreign currency field |
  | `ax_fvalue` | `5000.0` |  |
  | `ax_job` | `` |  |
  | `ax_jrnl` | `3454.0` | From nparm.np_nexjrnl |
  | `ax_nacnt` | `C110` |  |
  | `ax_ncntr` | `` |  |
  | `ax_nlpdate` | `2024-05-20T00:00:00` |  |
  | `ax_project` | `` |  |
  | `ax_source` | `A` |  |
  | `ax_srcco` | `Z` |  |
  | `ax_tref` | `test` |  |
  | `ax_unique` | `_7FQ19II9J` | Base-36 unique ID |
  | `ax_value` | `42.0` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ax_comment` | `test` |  |
  | `ax_date` | `2024-05-20T00:00:00` |  |
  | `ax_done` | `Y` | NL transfer complete |
  | `ax_fcdec` | `2.0` | Foreign currency field |
  | `ax_fcmult` | `0.0` |  |
  | `ax_fcrate` | `1.190476` | Foreign currency field |
  | `ax_fcurr` | `EUR` | Foreign currency field |
  | `ax_fvalue` | `-5000.0` |  |
  | `ax_job` | `` |  |
  | `ax_jrnl` | `3454.0` | From nparm.np_nexjrnl |
  | `ax_nacnt` | `FC001` |  |
  | `ax_ncntr` | `` |  |
  | `ax_nlpdate` | `2024-05-20T00:00:00` |  |
  | `ax_project` | `` |  |
  | `ax_source` | `A` |  |
  | `ax_srcco` | `Z` |  |
  | `ax_tref` | `test` |  |
  | `ax_unique` | `_7FQ19II9J` | Base-36 unique ID |
  | `ax_value` | `-42.0` |  |

**atran**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `at_account` | `C110` |  |
  | `at_acnt` | `FC001` |  |
  | `at_atpycd` | `` |  |
  | `at_bacprn` | `0.0` |  |
  | `at_bic` | `` |  |
  | `at_bsname` | `` |  |
  | `at_bsref` | `` |  |
  | `at_cash` | `0.0` |  |
  | `at_cbtype` | `P1` |  |
  | `at_ccauth` | `` |  |
  | `at_ccdno` | `` |  |
  | `at_ccdprn` | `0.0` |  |
  | `at_chqlst` | `0.0` |  |
  | `at_chqprn` | `0.0` |  |
  | `at_cntr` | `` |  |
  | `at_comment` | `test` |  |
  | `at_disc` | `0.0` |  |
  | `at_ecb` | `0.0` |  |
  | `at_ecbpayd` | `2024-05-20T00:00:00` |  |
  | `at_ecbtype` | `` |  |
  | `at_entry` | `P100000770` | From atype counter |
  | `at_fcdec` | `2.0` | Foreign currency field |
  | `at_fcexch` | `1.190476` | Foreign currency field |
  | `at_fcmult` | `0.0` |  |
  | `at_fcurr` | `EUR` | Foreign currency field |
  | `at_iban` | `` |  |
  | `at_inputby` | `TEST` |  |
  | `at_job` | `` |  |
  | `at_memo` | `` |  |
  | `at_name` | `Trade Debtors` |  |
  | `at_number` | `` |  |
  | `at_payee` | `` |  |
  | `at_payname` | `` |  |
  | `at_payslp` | `0.0` |  |
  | `at_postgrp` | `0.0` |  |
  | `at_project` | `` |  |
  | `at_pstdate` | `2024-05-20T00:00:00` |  |
  | `at_pysprn` | `0.0` |  |
  | `at_refer` | `test` |  |
  | `at_remit` | `0.0` |  |
  | `at_remove` | `0.0` |  |
  | `at_sort` | `` |  |
  | `at_srcco` | `Z` |  |
  | `at_sysdate` | `2026-03-31T00:00:00` |  |
  | `at_tperiod` | `1.0` | From nominal calendar |
  | `at_type` | `1.0` |  |
  | `at_unique` | `_7FQ19II9J` | Base-36 unique ID |
  | `at_value` | `-5000.0` |  |
  | `at_vattycd` | `` |  |

**atype**
  *1 row(s) modified*

**idtab**
  *1 row(s) modified*

**nacnt**
  *2 row(s) modified*

**nbank**
  *1 row(s) modified*

**nextid**
  *6 row(s) modified*

**nhist**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `nh_bal` | `-42.0` |  |
  | `nh_budg` | `0.0` |  |
  | `nh_fbal` | `0.0` |  |
  | `nh_job` | `` |  |
  | `nh_nacnt` | `K999` |  |
  | `nh_ncntr` | `` |  |
  | `nh_nsubt` | `01` |  |
  | `nh_ntype` | `30` |  |
  | `nh_period` | `5.0` | From nominal calendar |
  | `nh_project` | `` |  |
  | `nh_ptdcr` | `-42.0` |  |
  | `nh_ptddr` | `0.0` |  |
  | `nh_rbudg` | `0.0` |  |
  | `nh_rectype` | `1.0` |  |
  | `nh_year` | `2024.0` |  |

  *1 row(s) modified*

**njmemo**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `nj_binrep` | `0.0` |  |
  | `nj_image` | `` |  |
  | `nj_journal` | `3454.0` | From nparm.np_nexjrnl |
  | `nj_memo` | `ÿ<<JOURNAL_DATA_ONLY>>ÿ` |  |
  | `nj_txtrep` | `Cashbook Ledger Transfer` |  |

**nsubt**
  *2 row(s) modified*

**ntype**
  *2 row(s) modified*

**zlock**
  *1 row(s) modified*

---

### foreign currency receipt

**aentry**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `ae_acnt` | `FC001` |  |
  | `ae_batchid` | `0.0` |  |
  | `ae_brwptr` | `` |  |
  | `ae_cbtype` | `R1` |  |
  | `ae_cntr` | `` |  |
  | `ae_comment` | `test` |  |
  | `ae_complet` | `0.0` |  |
  | `ae_entref` | `test` |  |
  | `ae_entry` | `R100000233` | From atype counter |
  | `ae_frstat` | `0.0` |  |
  | `ae_lstdate` | `2024-05-01T00:00:00` |  |
  | `ae_payid` | `0.0` |  |
  | `ae_postgrp` | `0.0` |  |
  | `ae_recbal` | `0.0` |  |
  | `ae_reclnum` | `0.0` |  |
  | `ae_remove` | `0.0` |  |
  | `ae_statln` | `0.0` |  |
  | `ae_tmpstat` | `0.0` |  |
  | `ae_tostat` | `0.0` |  |
  | `ae_value` | `10000.0` |  |
  | `sq_amtime` | `` |  |
  | `sq_amuser` | `` |  |
  | `sq_crdate` | `2026-03-31T00:00:00` |  |
  | `sq_crtime` | `21:06:48` |  |
  | `sq_cruser` | `TEST` |  |

**anoml**
  *2 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ax_comment` | `test` |  |
  | `ax_date` | `2024-05-01T00:00:00` |  |
  | `ax_done` | `Y` | NL transfer complete |
  | `ax_fcdec` | `2.0` | Foreign currency field |
  | `ax_fcmult` | `0.0` |  |
  | `ax_fcrate` | `1.190476` | Foreign currency field |
  | `ax_fcurr` | `EUR` | Foreign currency field |
  | `ax_fvalue` | `10000.0` |  |
  | `ax_job` | `` |  |
  | `ax_jrnl` | `3453.0` | From nparm.np_nexjrnl |
  | `ax_nacnt` | `FC001` |  |
  | `ax_ncntr` | `` |  |
  | `ax_nlpdate` | `2024-05-01T00:00:00` |  |
  | `ax_project` | `` |  |
  | `ax_source` | `A` |  |
  | `ax_srcco` | `Z` |  |
  | `ax_tref` | `test` |  |
  | `ax_unique` | `_7FQ1994R2` | Base-36 unique ID |
  | `ax_value` | `84.0` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ax_comment` | `test` |  |
  | `ax_date` | `2024-05-01T00:00:00` |  |
  | `ax_done` | `Y` | NL transfer complete |
  | `ax_fcdec` | `2.0` | Foreign currency field |
  | `ax_fcmult` | `0.0` |  |
  | `ax_fcrate` | `1.190476` | Foreign currency field |
  | `ax_fcurr` | `EUR` | Foreign currency field |
  | `ax_fvalue` | `-10000.0` |  |
  | `ax_job` | `SM` |  |
  | `ax_jrnl` | `3453.0` | From nparm.np_nexjrnl |
  | `ax_nacnt` | `S120` |  |
  | `ax_ncntr` | `ADM` |  |
  | `ax_nlpdate` | `2024-05-01T00:00:00` |  |
  | `ax_project` | `DVD1` |  |
  | `ax_source` | `A` |  |
  | `ax_srcco` | `Z` |  |
  | `ax_tref` | `test` |  |
  | `ax_unique` | `_7FQ1994R2` | Base-36 unique ID |
  | `ax_value` | `-84.0` |  |

**atran**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `at_account` | `S120    ADM` |  |
  | `at_acnt` | `FC001` |  |
  | `at_atpycd` | `` |  |
  | `at_bacprn` | `0.0` |  |
  | `at_bic` | `` |  |
  | `at_bsname` | `` |  |
  | `at_bsref` | `` |  |
  | `at_cash` | `0.0` |  |
  | `at_cbtype` | `R1` |  |
  | `at_ccauth` | `` |  |
  | `at_ccdno` | `` |  |
  | `at_ccdprn` | `0.0` |  |
  | `at_chqlst` | `0.0` |  |
  | `at_chqprn` | `0.0` |  |
  | `at_cntr` | `` |  |
  | `at_comment` | `test` |  |
  | `at_disc` | `0.0` |  |
  | `at_ecb` | `0.0` |  |
  | `at_ecbpayd` | `2024-05-01T00:00:00` |  |
  | `at_ecbtype` | `` |  |
  | `at_entry` | `R100000233` | From atype counter |
  | `at_fcdec` | `2.0` | Foreign currency field |
  | `at_fcexch` | `1.190476` | Foreign currency field |
  | `at_fcmult` | `0.0` |  |
  | `at_fcurr` | `EUR` | Foreign currency field |
  | `at_iban` | `` |  |
  | `at_inputby` | `TEST` |  |
  | `at_job` | `SM` |  |
  | `at_memo` | `` |  |
  | `at_name` | `Travel Expenses / Subsistence` |  |
  | `at_number` | `` |  |
  | `at_payee` | `` |  |
  | `at_payname` | `` |  |
  | `at_payslp` | `0.0` |  |
  | `at_postgrp` | `0.0` |  |
  | `at_project` | `DVD1` |  |
  | `at_pstdate` | `2024-05-01T00:00:00` |  |
  | `at_pysprn` | `1.0` |  |
  | `at_refer` | `test` |  |
  | `at_remit` | `0.0` |  |
  | `at_remove` | `0.0` |  |
  | `at_sort` | `` |  |
  | `at_srcco` | `Z` |  |
  | `at_sysdate` | `2026-03-31T00:00:00` |  |
  | `at_tperiod` | `1.0` | From nominal calendar |
  | `at_type` | `2.0` |  |
  | `at_unique` | `_7FQ1994R2` | Base-36 unique ID |
  | `at_value` | `10000.0` |  |
  | `at_vattycd` | `` |  |

**atype**
  *1 row(s) modified*

**idtab**
  *1 row(s) modified*

**nacnt**
  *2 row(s) modified*

**nbank**
  *1 row(s) modified*

**nextid**
  *6 row(s) modified*

**nhist**
  *2 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `nh_bal` | `84.0` |  |
  | `nh_budg` | `0.0` |  |
  | `nh_fbal` | `0.0` |  |
  | `nh_job` | `` |  |
  | `nh_nacnt` | `M999` |  |
  | `nh_ncntr` | `` |  |
  | `nh_nsubt` | `01` |  |
  | `nh_ntype` | `35` |  |
  | `nh_period` | `5.0` | From nominal calendar |
  | `nh_project` | `` |  |
  | `nh_ptdcr` | `0.0` |  |
  | `nh_ptddr` | `84.0` |  |
  | `nh_rbudg` | `0.0` |  |
  | `nh_rectype` | `1.0` |  |
  | `nh_year` | `2024.0` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `nh_bal` | `-84.0` |  |
  | `nh_budg` | `0.0` |  |
  | `nh_fbal` | `0.0` |  |
  | `nh_job` | `SM` |  |
  | `nh_nacnt` | `S120` |  |
  | `nh_ncntr` | `ADM` |  |
  | `nh_nsubt` | `03` |  |
  | `nh_ntype` | `45` |  |
  | `nh_period` | `5.0` | From nominal calendar |
  | `nh_project` | `DVD1` |  |
  | `nh_ptdcr` | `-84.0` |  |
  | `nh_ptddr` | `0.0` |  |
  | `nh_rbudg` | `0.0` |  |
  | `nh_rectype` | `1.0` |  |
  | `nh_year` | `2024.0` |  |

**njmemo**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `nj_binrep` | `0.0` |  |
  | `nj_image` | `` |  |
  | `nj_journal` | `3453.0` | From nparm.np_nexjrnl |
  | `nj_memo` | `ÿ<<JOURNAL_DATA_ONLY>>ÿ` |  |
  | `nj_txtrep` | `Cashbook Ledger Transfer` |  |

**nsubt**
  *2 row(s) modified*

**ntype**
  *2 row(s) modified*

**zlock**
  *1 row(s) modified*

---

## Customer Master (sname)

### New Customer

*Create a new customer account in sname.*

**nextid**
  *2 row(s) modified*

**sname**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `sn_account` | `A1224` |  |
  | `sn_acknow` | `0.0` |  |
  | `sn_addr1` | `Test` |  |
  | `sn_addr2` | `` |  |
  | `sn_addr3` | `` |  |
  | `sn_addr4` | `` |  |
  | `sn_adjsvcd` | `` |  |
  | `sn_analsys` | `ANAL` |  |
  | `sn_atpycd` | `` |  |
  | `sn_bana` | `` |  |
  | `sn_bankac` | `` |  |
  | `sn_banksor` | `` |  |
  | `sn_bic` | `` |  |
  | `sn_branch` | `0.0` |  |
  | `sn_cmgroup` | `` |  |
  | `sn_contac2` | `order cont` |  |
  | `sn_contact` | `ac contact` |  |
  | `sn_cprfl` | `NOSTAT` |  |
  | `sn_crdcrno` | `` |  |
  | `sn_crdnotes` | `` |  |
  | `sn_crdrate` | `0.0` |  |
  | `sn_crdscor` | `` |  |
  | `sn_crlim` | `0.0` |  |
  | `sn_ctry` | `GB` |  |
  | `sn_currbal` | `0.0` |  |
  | `sn_custloc` | `` |  |
  | `sn_custype` | `S` |  |
  | `sn_delinst` | `` |  |
  | `sn_delt` | `` |  |
  | `sn_desp` | `` |  |
  | `sn_dl_flag` | `0.0` |  |
  | `sn_dl_pubid` | `0.0` |  |
  | `sn_dltmail` | `0.0` |  |
  | `sn_docmail` | `0.0` |  |
  | `sn_dormant` | `0.0` |  |
  | `sn_dwar` | `MAIN` |  |
  | `sn_email` | `email` |  |
  | `sn_emailoa` | `0.0` |  |
  | `sn_emailst` | `0.0` |  |
  | `sn_eori` | `` |  |
  | `sn_epasswd` | `` |  |
  | `sn_estore` | `` |  |
  | `sn_extra1` | `` |  |
  | `sn_extra2` | `` |  |
  | `sn_faxno` | `` |  |
  | `sn_fcreate` | `2026-04-06T00:00:00` | Foreign currency field |
  | `sn_frnvat` | `0.0` |  |
  | `sn_iban` | `` |  |
  | `sn_invceac` | `` |  |
  | `sn_job` | `A001` |  |
  | `sn_key1` | `TEST` |  |
  | `sn_key2` | `` |  |
  | `sn_key3` | `` |  |
  | `sn_key4` | `` |  |
  | `sn_luptime` | `` |  |
  | `sn_memo` | `` |  |
  | `sn_model` | `0.0` |  |
  | `sn_mtrn` | `` |  |
  | `sn_name` | `Test` |  |
  | `sn_nextpay` | `0.0` |  |
  | `sn_nrthire` | `0.0` |  |
  | `sn_ntrn` | `` |  |
  | `sn_ordmail` | `` |  |
  | `sn_ordrbal` | `0.0` |  |
  | `sn_ovravmt` | `0.0` |  |
  | `sn_priorty` | `1.0` |  |
  | `sn_project` | `S020` |  |
  | `sn_pstcode` | `SW19 8SE` |  |
  | `sn_rana` | `` |  |
  | `sn_region` | `A` |  |
  | `sn_route` | `` |  |
  | `sn_sana` | `` |  |
  | `sn_sepayee` | `` |  |
  | `sn_sepctry` | `` |  |
  | `sn_seppstc` | `` |  |
  | `sn_sepstnm` | `` |  |
  | `sn_septown` | `` |  |
  | `sn_sgrp` | `` |  |
  | `sn_stmntac` | `` |  |
  | `sn_stop` | `0.0` |  |
  | `sn_teleno` | `` |  |
  | `sn_terrtry` | `001` |  |
  | `sn_tprfl` | `STD` |  |
  | `sn_trnover` | `0.0` |  |
  | `sn_vendor` | `` |  |
  | `sn_vrn` | `` |  |
  | `sn_wwwpage` | `` |  |

**zcontacts**
  *2 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `sq_amtime` | `` |  |
  | `sq_amuser` | `` |  |
  | `sq_date` | `2026-04-06T00:00:00` |  |
  | `sq_time` | `20:46:01` |  |
  | `sq_user` | `TEST` |  |
  | `zc_account` | `A1224` |  |
  | `zc_allwfax` | `1.0` |  |
  | `zc_allwmal` | `1.0` |  |
  | `zc_allwmob` | `1.0` |  |
  | `zc_allwph` | `1.0` |  |
  | `zc_attr1` | `` |  |
  | `zc_attr2` | `` |  |
  | `zc_attr3` | `` |  |
  | `zc_attr4` | `` |  |
  | `zc_attr5` | `` |  |
  | `zc_attr6` | `` |  |
  | `zc_contact` | `ac contact` |  |
  | `zc_email` | `email` |  |
  | `zc_fax` | `` |  |
  | `zc_fornam` | `` |  |
  | `zc_mobile` | `` |  |
  | `zc_module` | `S` |  |
  | `zc_optcont` | `1.0` |  |
  | `zc_phone` | `` |  |
  | `zc_pos` | `` |  |
  | `zc_surname` | `` |  |
  | `zc_title` | `` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `sq_amtime` | `` |  |
  | `sq_amuser` | `` |  |
  | `sq_date` | `2026-04-06T00:00:00` |  |
  | `sq_time` | `20:46:01` |  |
  | `sq_user` | `TEST` |  |
  | `zc_account` | `A1224` |  |
  | `zc_allwfax` | `0.0` |  |
  | `zc_allwmal` | `0.0` |  |
  | `zc_allwmob` | `0.0` |  |
  | `zc_allwph` | `0.0` |  |
  | `zc_attr1` | `` |  |
  | `zc_attr2` | `` |  |
  | `zc_attr3` | `` |  |
  | `zc_attr4` | `` |  |
  | `zc_attr5` | `` |  |
  | `zc_attr6` | `` |  |
  | `zc_contact` | `order cont` |  |
  | `zc_email` | `` |  |
  | `zc_fax` | `` |  |
  | `zc_fornam` | `` |  |
  | `zc_mobile` | `` |  |
  | `zc_module` | `S` |  |
  | `zc_optcont` | `2.0` |  |
  | `zc_phone` | `` |  |
  | `zc_pos` | `` |  |
  | `zc_surname` | `` |  |
  | `zc_title` | `` |  |

---

## Nominal Ledger Journals

### Nominal Journal

*Manual nominal journal entry. Creates: ntran (debit + credit), nacnt updates.*

**idtab**
  *1 row(s) modified*

**nacnt**
  *3 row(s) modified*

**ndetl**
  *2 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `nd_account` | `S120     ADM` |  |
  | `nd_comm` | `test` |  |
  | `nd_cramnt` | `0.0` |  |
  | `nd_dramnt` | `100.0` |  |
  | `nd_fcdec` | `0.0` |  |
  | `nd_fcmult` | `0.0` |  |
  | `nd_fcrate` | `0.0` |  |
  | `nd_fcurr` | `` | Foreign currency field |
  | `nd_fvalue` | `0.0` |  |
  | `nd_job` | `` |  |
  | `nd_project` | `` |  |
  | `nd_recno` | `1.0` |  |
  | `nd_ref` | `JNL0000058` |  |
  | `nd_taxdate` | `2024-05-01T00:00:00` |  |
  | `nd_tfcdec` | `0.0` |  |
  | `nd_tfcmult` | `0.0` |  |
  | `nd_tfcrate` | `0.0` |  |
  | `nd_tfcurr` | `` | Foreign currency field |
  | `nd_tfvalue` | `0.0` |  |
  | `nd_vatcde` | `1` |  |
  | `nd_vattyp` | `P` |  |
  | `nd_vatval` | `16.67` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `nd_account` | `E325     TEC` |  |
  | `nd_comm` | `test` |  |
  | `nd_cramnt` | `100.0` |  |
  | `nd_dramnt` | `0.0` |  |
  | `nd_fcdec` | `0.0` |  |
  | `nd_fcmult` | `0.0` |  |
  | `nd_fcrate` | `0.0` |  |
  | `nd_fcurr` | `` | Foreign currency field |
  | `nd_fvalue` | `0.0` |  |
  | `nd_job` | `` |  |
  | `nd_project` | `` |  |
  | `nd_recno` | `2.0` |  |
  | `nd_ref` | `JNL0000058` |  |
  | `nd_tfcdec` | `0.0` |  |
  | `nd_tfcmult` | `0.0` |  |
  | `nd_tfcrate` | `0.0` |  |
  | `nd_tfcurr` | `` | Foreign currency field |
  | `nd_tfvalue` | `0.0` |  |
  | `nd_vatcde` | `N` |  |
  | `nd_vattyp` | `N` |  |
  | `nd_vatval` | `0.0` |  |

  *29 row(s) modified*

**nextid**
  *7 row(s) modified*

**nhead**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `nh_crtot` | `100.0` |  |
  | `nh_date` | `2024-05-01T00:00:00` |  |
  | `nh_days` | `0.0` |  |
  | `nh_drtot` | `100.0` |  |
  | `nh_fcdec` | `0.0` |  |
  | `nh_fcmult` | `0.0` |  |
  | `nh_fcrate` | `0.0` |  |
  | `nh_fcrtot` | `0.0` |  |
  | `nh_fcurr` | `` | Foreign currency field |
  | `nh_fdrtot` | `0.0` |  |
  | `nh_freq` | `` |  |
  | `nh_inp` | `TEST` |  |
  | `nh_journal` | `3448.0` | From nparm.np_nexjrnl |
  | `nh_ldate` | `2024-05-01T00:00:00` |  |
  | `nh_memo` | `` |  |
  | `nh_narr` | `test` |  |
  | `nh_periods` | `0.0` | From nominal calendar |
  | `nh_plast` | `5.0` |  |
  | `nh_recur` | `0.0` |  |
  | `nh_ref` | `JNL0000058` |  |
  | `nh_retain` | `1.0` |  |
  | `nh_rev` | `1.0` |  |
  | `nh_times` | `1.0` |  |
  | `nh_vatanal` | `1.0` |  |
  | `nh_ylast` | `2024.0` |  |

**nhist**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `nh_bal` | `-100.0` |  |
  | `nh_budg` | `0.0` |  |
  | `nh_fbal` | `0.0` |  |
  | `nh_job` | `` |  |
  | `nh_nacnt` | `E325` |  |
  | `nh_ncntr` | `TEC` |  |
  | `nh_nsubt` | `02` |  |
  | `nh_ntype` | `15` |  |
  | `nh_period` | `5.0` | From nominal calendar |
  | `nh_project` | `` |  |
  | `nh_ptdcr` | `-100.0` |  |
  | `nh_ptddr` | `0.0` |  |
  | `nh_rbudg` | `0.0` |  |
  | `nh_rectype` | `1.0` |  |
  | `nh_year` | `2024.0` |  |

  *2 row(s) modified*

**njmemo**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `nj_binrep` | `1.0` |  |
  | `nj_image` | `` |  |
  | `nj_journal` | `3448.0` | From nparm.np_nexjrnl |
  | `nj_memo` | `ÿ<<JOURNAL_DATA_ONLY>>ÿ` |  |
  | `nj_txtrep` | `` |  |

**nparm**
  *1 row(s) modified*

**nsubt**
  *2 row(s) modified*

**ntype**
  *2 row(s) modified*

**nvat**
  *2 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `nv_acnt` | `S120` |  |
  | `nv_advance` | `0.0` |  |
  | `nv_cntr` | `ADM` |  |
  | `nv_comment` | `test` |  |
  | `nv_crdate` | `2024-05-01T00:00:00` |  |
  | `nv_date` | `2024-05-01T00:00:00` |  |
  | `nv_ref` | `JNL0000058` |  |
  | `nv_taxdate` | `2024-05-01T00:00:00` |  |
  | `nv_type` | `I` |  |
  | `nv_value` | `100.0` |  |
  | `nv_vatcode` | `1` |  |
  | `nv_vatctry` | `H` |  |
  | `nv_vatrate` | `20.0` |  |
  | `nv_vattype` | `P` |  |
  | `nv_vatval` | `16.67` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `nv_acnt` | `E325` |  |
  | `nv_advance` | `0.0` |  |
  | `nv_cntr` | `TEC` |  |
  | `nv_comment` | `test` |  |
  | `nv_crdate` | `2024-05-01T00:00:00` |  |
  | `nv_date` | `2024-05-01T00:00:00` |  |
  | `nv_ref` | `JNL0000058` |  |
  | `nv_type` | `I` |  |
  | `nv_value` | `-100.0` |  |
  | `nv_vatcode` | `N` |  |
  | `nv_vatctry` | `H` |  |
  | `nv_vatrate` | `0.0` |  |
  | `nv_vattype` | `N` |  |
  | `nv_vatval` | `0.0` |  |

**zpool**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `sp_cby` | `TEST` |  |
  | `sp_cdate` | `2026-04-01T00:00:00` |  |
  | `sp_ctime` | `14:23` |  |
  | `sp_desc` | `sdf` |  |
  | `sp_file` | `SFS` |  |
  | `sp_origin` | `` |  |
  | `sp_pby` | `` |  |
  | `sp_platfrm` | `32BIT` |  |
  | `sp_printer` | `PDF:` |  |
  | `sp_ptime` | `` |  |
  | `sp_rephite` | `0.0` |  |
  | `sp_repwide` | `0.0` |  |

---

### Nominal Journal - not posted

**sequser**
  *1 row(s) modified*

**ndetl**
  *2 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `nd_account` | `GA110` |  |
  | `nd_comm` | `test]` |  |
  | `nd_cramnt` | `0.0` |  |
  | `nd_dramnt` | `120.0` |  |
  | `nd_fcdec` | `0.0` |  |
  | `nd_fcmult` | `0.0` |  |
  | `nd_fcrate` | `0.0` |  |
  | `nd_fcurr` | `` | Foreign currency field |
  | `nd_fvalue` | `0.0` |  |
  | `nd_job` | `` |  |
  | `nd_project` | `U999` |  |
  | `nd_recno` | `1.0` |  |
  | `nd_ref` | `INT00522` |  |
  | `nd_taxdate` | `2026-04-06T00:00:00` |  |
  | `nd_tfcdec` | `0.0` |  |
  | `nd_tfcmult` | `0.0` |  |
  | `nd_tfcrate` | `0.0` |  |
  | `nd_tfcurr` | `` | Foreign currency field |
  | `nd_tfvalue` | `0.0` |  |
  | `nd_vatcde` | `2` |  |
  | `nd_vattyp` | `P` |  |
  | `nd_vatval` | `20.0` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `nd_account` | `GA045` |  |
  | `nd_comm` | `test]` |  |
  | `nd_cramnt` | `120.0` |  |
  | `nd_dramnt` | `0.0` |  |
  | `nd_fcdec` | `0.0` |  |
  | `nd_fcmult` | `0.0` |  |
  | `nd_fcrate` | `0.0` |  |
  | `nd_fcurr` | `` | Foreign currency field |
  | `nd_fvalue` | `0.0` |  |
  | `nd_job` | `` |  |
  | `nd_project` | `` |  |
  | `nd_recno` | `2.0` |  |
  | `nd_ref` | `INT00522` |  |
  | `nd_tfcdec` | `0.0` |  |
  | `nd_tfcmult` | `0.0` |  |
  | `nd_tfcrate` | `0.0` |  |
  | `nd_tfcurr` | `` | Foreign currency field |
  | `nd_tfvalue` | `0.0` |  |
  | `nd_vatcde` | `N` |  |
  | `nd_vattyp` | `N` |  |
  | `nd_vatval` | `0.0` |  |

**nextid**
  *2 row(s) modified*

**nhead**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `nh_crtot` | `120.0` |  |
  | `nh_date` | `2026-04-06T00:00:00` |  |
  | `nh_days` | `0.0` |  |
  | `nh_drtot` | `120.0` |  |
  | `nh_fcdec` | `0.0` |  |
  | `nh_fcmult` | `0.0` |  |
  | `nh_fcrate` | `0.0` |  |
  | `nh_fcrtot` | `0.0` |  |
  | `nh_fcurr` | `` | Foreign currency field |
  | `nh_fdrtot` | `0.0` |  |
  | `nh_freq` | `` |  |
  | `nh_inp` | `TEST` |  |
  | `nh_journal` | `0.0` | From nparm.np_nexjrnl |
  | `nh_memo` | `` |  |
  | `nh_narr` | `test12` |  |
  | `nh_periods` | `0.0` | From nominal calendar |
  | `nh_plast` | `0.0` |  |
  | `nh_recur` | `0.0` |  |
  | `nh_ref` | `INT00522` |  |
  | `nh_retain` | `0.0` |  |
  | `nh_rev` | `0.0` |  |
  | `nh_times` | `0.0` |  |
  | `nh_vatanal` | `1.0` |  |
  | `nh_ylast` | `0.0` |  |

**nparm**
  *1 row(s) modified*

---

## Nominal Account Master (nname/nacnt)

### Cashbook transfer

*Payment to nominal account (no ledger). Creates: aentry, atran, ntran, anoml, nacnt, nbank.*

**anoml**
  *101 row(s) modified*

**idtab**
  *1 row(s) modified*

**nacnt**
  *13 row(s) modified*

**ndetail**
  *37 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `nt_acnt` | `C310` |  |
  | `nt_cdesc` | `` |  |
  | `nt_cmnt` | `WAGES TRANSFER` |  |
  | `nt_cntr` | `` |  |
  | `nt_consol` | `0.0` |  |
  | `nt_distrib` | `0.0` |  |
  | `nt_entr` | `2024-05-01T00:00:00` |  |
  | `nt_fcdec` | `0.0` |  |
  | `nt_fcmult` | `0.0` |  |
  | `nt_fcrate` | `0.0` |  |
  | `nt_fcurr` | `` | Foreign currency field |
  | `nt_fvalue` | `0.0` |  |
  | `nt_inp` | `TEST` |  |
  | `nt_job` | `` |  |
  | `nt_jrnl` | `3441.0` | From nparm.np_nexjrnl |
  | `nt_period` | `5.0` | From nominal calendar |
  | `nt_perpost` | `0.0` |  |
  | `nt_posttyp` | `W` |  |
  | `nt_prevyr` | `0.0` |  |
  | `nt_project` | `` |  |
  | `nt_pstgrp` | `1.0` |  |
  | `nt_pstid` | `_4J9H1FE00` |  |
  | `nt_recjrnl` | `0.0` | From nparm.np_nexjrnl |
  | `nt_rectify` | `0.0` |  |
  | `nt_recurr` | `0.0` |  |
  | `nt_ref` | `` |  |
  | `nt_rvrse` | `0.0` |  |
  | `nt_srcco` | `Z` |  |
  | `nt_subt` | `03` |  |
  | `nt_trnref` | `` |  |
  | `nt_trtype` | `A` |  |
  | `nt_type` | `10` |  |
  | `nt_value` | `-2381.49` |  |
  | `nt_vatanal` | `0.0` |  |
  | `nt_year` | `2024.0` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `nt_acnt` | `C310` |  |
  | `nt_cdesc` | `` |  |
  | `nt_cmnt` | `WAGES TRANSFER` |  |
  | `nt_cntr` | `` |  |
  | `nt_consol` | `0.0` |  |
  | `nt_distrib` | `0.0` |  |
  | `nt_entr` | `2024-05-01T00:00:00` |  |
  | `nt_fcdec` | `0.0` |  |
  | `nt_fcmult` | `0.0` |  |
  | `nt_fcrate` | `0.0` |  |
  | `nt_fcurr` | `` | Foreign currency field |
  | `nt_fvalue` | `0.0` |  |
  | `nt_inp` | `TEST` |  |
  | `nt_job` | `` |  |
  | `nt_jrnl` | `3441.0` | From nparm.np_nexjrnl |
  | `nt_period` | `5.0` | From nominal calendar |
  | `nt_perpost` | `0.0` |  |
  | `nt_posttyp` | `W` |  |
  | `nt_prevyr` | `0.0` |  |
  | `nt_project` | `` |  |
  | `nt_pstgrp` | `2.0` |  |
  | `nt_pstid` | `_4J9H1FE00` |  |
  | `nt_recjrnl` | `0.0` | From nparm.np_nexjrnl |
  | `nt_rectify` | `0.0` |  |
  | `nt_recurr` | `0.0` |  |
  | `nt_ref` | `` |  |
  | `nt_rvrse` | `0.0` |  |
  | `nt_srcco` | `Z` |  |
  | `nt_subt` | `03` |  |
  | `nt_trnref` | `` |  |
  | `nt_trtype` | `A` |  |
  | `nt_type` | `10` |  |
  | `nt_value` | `-1041.24` |  |
  | `nt_vatanal` | `0.0` |  |
  | `nt_year` | `2024.0` |  |

  Row 3:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `nt_acnt` | `C310` |  |
  | `nt_cdesc` | `` |  |
  | `nt_cmnt` | `WAGES TRANSFER` |  |
  | `nt_cntr` | `` |  |
  | `nt_consol` | `0.0` |  |
  | `nt_distrib` | `0.0` |  |
  | `nt_entr` | `2024-05-01T00:00:00` |  |
  | `nt_fcdec` | `0.0` |  |
  | `nt_fcmult` | `0.0` |  |
  | `nt_fcrate` | `0.0` |  |
  | `nt_fcurr` | `` | Foreign currency field |
  | `nt_fvalue` | `0.0` |  |
  | `nt_inp` | `TEST` |  |
  | `nt_job` | `` |  |
  | `nt_jrnl` | `3441.0` | From nparm.np_nexjrnl |
  | `nt_period` | `5.0` | From nominal calendar |
  | `nt_perpost` | `0.0` |  |
  | `nt_posttyp` | `W` |  |
  | `nt_prevyr` | `0.0` |  |
  | `nt_project` | `` |  |
  | `nt_pstgrp` | `3.0` |  |
  | `nt_pstid` | `_4J9H1FE00` |  |
  | `nt_recjrnl` | `0.0` | From nparm.np_nexjrnl |
  | `nt_rectify` | `0.0` |  |
  | `nt_recurr` | `0.0` |  |
  | `nt_ref` | `` |  |
  | `nt_rvrse` | `0.0` |  |
  | `nt_srcco` | `Z` |  |
  | `nt_subt` | `03` |  |
  | `nt_trnref` | `` |  |
  | `nt_trtype` | `A` |  |
  | `nt_type` | `10` |  |
  | `nt_value` | `-2131.83` |  |
  | `nt_vatanal` | `0.0` |  |
  | `nt_year` | `2024.0` |  |

**nextid**
  *4 row(s) modified*

**nhist**
  *13 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `nh_bal` | `-2273.99` |  |
  | `nh_budg` | `0.0` |  |
  | `nh_fbal` | `0.0` |  |
  | `nh_job` | `` |  |
  | `nh_nacnt` | `C110` |  |
  | `nh_ncntr` | `` |  |
  | `nh_nsubt` | `01` |  |
  | `nh_ntype` | `10` |  |
  | `nh_period` | `5.0` | From nominal calendar |
  | `nh_project` | `` |  |
  | `nh_ptdcr` | `-2297.98` |  |
  | `nh_ptddr` | `23.99` |  |
  | `nh_rbudg` | `0.0` |  |
  | `nh_rectype` | `1.0` |  |
  | `nh_year` | `2024.0` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `nh_bal` | `-27387.95` |  |
  | `nh_budg` | `0.0` |  |
  | `nh_fbal` | `0.0` |  |
  | `nh_job` | `` |  |
  | `nh_nacnt` | `C310` |  |
  | `nh_ncntr` | `` |  |
  | `nh_nsubt` | `03` |  |
  | `nh_ntype` | `10` |  |
  | `nh_period` | `5.0` | From nominal calendar |
  | `nh_project` | `` |  |
  | `nh_ptdcr` | `-27387.95` |  |
  | `nh_ptddr` | `0.0` |  |
  | `nh_rbudg` | `0.0` |  |
  | `nh_rectype` | `1.0` |  |
  | `nh_year` | `2024.0` |  |

  Row 3:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `nh_bal` | `40.0` |  |
  | `nh_budg` | `0.0` |  |
  | `nh_fbal` | `0.0` |  |
  | `nh_job` | `` |  |
  | `nh_nacnt` | `C315` |  |
  | `nh_ncntr` | `` |  |
  | `nh_nsubt` | `03` |  |
  | `nh_ntype` | `10` |  |
  | `nh_period` | `5.0` | From nominal calendar |
  | `nh_project` | `` |  |
  | `nh_ptdcr` | `0.0` |  |
  | `nh_ptddr` | `40.0` |  |
  | `nh_rbudg` | `0.0` |  |
  | `nh_rectype` | `1.0` |  |
  | `nh_year` | `2024.0` |  |

**njmemo**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `nj_binrep` | `1.0` |  |
  | `nj_image` | `` |  |
  | `nj_journal` | `3441.0` | From nparm.np_nexjrnl |
  | `nj_memo` | `ÿ<<JOURNAL_DATA_ONLY>>ÿ` |  |
  | `nj_txtrep` | `` |  |

**nsubt**
  *6 row(s) modified*

**ntype**
  *4 row(s) modified*

---

### New Nominal Account

*Create a new nominal account in nacnt/nname.*

**nacnt**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `na_acnt` | `A1224` |  |
  | `na_allwjob` | `2.0` |  |
  | `na_allwprj` | `2.0` |  |
  | `na_balc01` | `0.0` |  |
  | `na_balc02` | `0.0` |  |
  | `na_balc03` | `0.0` |  |
  | `na_balc04` | `0.0` |  |
  | `na_balc05` | `0.0` |  |
  | `na_balc06` | `0.0` |  |
  | `na_balc07` | `0.0` |  |
  | `na_balc08` | `0.0` |  |
  | `na_balc09` | `0.0` |  |
  | `na_balc10` | `0.0` |  |
  | `na_balc11` | `0.0` |  |
  | `na_balc12` | `0.0` |  |
  | `na_balc13` | `0.0` |  |
  | `na_balc14` | `0.0` |  |
  | `na_balc15` | `0.0` |  |
  | `na_balc16` | `0.0` |  |
  | `na_balc17` | `0.0` |  |
  | `na_balc18` | `0.0` |  |
  | `na_balc19` | `0.0` |  |
  | `na_balc20` | `0.0` |  |
  | `na_balc21` | `0.0` |  |
  | `na_balc22` | `0.0` |  |
  | `na_balc23` | `0.0` |  |
  | `na_balc24` | `0.0` |  |
  | `na_balp01` | `0.0` |  |
  | `na_balp02` | `0.0` |  |
  | `na_balp03` | `0.0` |  |
  | `na_balp04` | `0.0` |  |
  | `na_balp05` | `0.0` |  |
  | `na_balp06` | `0.0` |  |
  | `na_balp07` | `0.0` |  |
  | `na_balp08` | `0.0` |  |
  | `na_balp09` | `0.0` |  |
  | `na_balp10` | `0.0` |  |
  | `na_balp11` | `0.0` |  |
  | `na_balp12` | `0.0` |  |
  | `na_balp13` | `0.0` |  |
  | `na_balp14` | `0.0` |  |
  | `na_balp15` | `0.0` |  |
  | `na_balp16` | `0.0` |  |
  | `na_balp17` | `0.0` |  |
  | `na_balp18` | `0.0` |  |
  | `na_balp19` | `0.0` |  |
  | `na_balp20` | `0.0` |  |
  | `na_balp21` | `0.0` |  |
  | `na_balp22` | `0.0` |  |
  | `na_balp23` | `0.0` |  |
  | `na_balp24` | `0.0` |  |
  | `na_cntr` | `` |  |
  | `na_comm` | `1.0` |  |
  | `na_desc` | `Test` |  |
  | `na_extcode` | `` |  |
  | `na_fbalc01` | `0.0` |  |
  | `na_fbalc02` | `0.0` |  |
  | `na_fbalc03` | `0.0` |  |
  | `na_fbalc04` | `0.0` |  |
  | `na_fbalc05` | `0.0` |  |
  | `na_fbalc06` | `0.0` |  |
  | `na_fbalc07` | `0.0` |  |
  | `na_fbalc08` | `0.0` |  |
  | `na_fbalc09` | `0.0` |  |
  | `na_fbalc10` | `0.0` |  |
  | `na_fbalc11` | `0.0` |  |
  | `na_fbalc12` | `0.0` |  |
  | `na_fbalc13` | `0.0` |  |
  | `na_fbalc14` | `0.0` |  |
  | `na_fbalc15` | `0.0` |  |
  | `na_fbalc16` | `0.0` |  |
  | `na_fbalc17` | `0.0` |  |
  | `na_fbalc18` | `0.0` |  |
  | `na_fbalc19` | `0.0` |  |
  | `na_fbalc20` | `0.0` |  |
  | `na_fbalc21` | `0.0` |  |
  | `na_fbalc22` | `0.0` |  |
  | `na_fbalc23` | `0.0` |  |
  | `na_fbalc24` | `0.0` |  |
  | `na_fbalp01` | `0.0` |  |
  | `na_fbalp02` | `0.0` |  |
  | `na_fbalp03` | `0.0` |  |
  | `na_fbalp04` | `0.0` |  |
  | `na_fbalp05` | `0.0` |  |
  | `na_fbalp06` | `0.0` |  |
  | `na_fbalp07` | `0.0` |  |
  | `na_fbalp08` | `0.0` |  |
  | `na_fbalp09` | `0.0` |  |
  | `na_fbalp10` | `0.0` |  |
  | `na_fbalp11` | `0.0` |  |
  | `na_fbalp12` | `0.0` |  |
  | `na_fbalp13` | `0.0` |  |
  | `na_fbalp14` | `0.0` |  |
  | `na_fbalp15` | `0.0` |  |
  | `na_fbalp16` | `0.0` |  |
  | `na_fbalp17` | `0.0` |  |
  | `na_fbalp18` | `0.0` |  |
  | `na_fbalp19` | `0.0` |  |
  | `na_fbalp20` | `0.0` |  |
  | `na_fbalp21` | `0.0` |  |
  | `na_fbalp22` | `0.0` |  |
  | `na_fbalp23` | `0.0` |  |
  | `na_fbalp24` | `0.0` |  |
  | `na_fcdec` | `0.0` |  |
  | `na_fcmult` | `0.0` |  |
  | `na_fcrate` | `0.0` |  |
  | `na_fcurr` | `` | Foreign currency field |
  | `na_fprycr` | `0.0` |  |
  | `na_fprydr` | `0.0` |  |
  | `na_fptdcr` | `0.0` |  |
  | `na_fptddr` | `0.0` |  |
  | `na_fytdcr` | `0.0` |  |
  | `na_fytddr` | `0.0` |  |
  | `na_job` | `A001` |  |
  | `na_key1` | `TEST` |  |
  | `na_key2` | `` |  |
  | `na_key3` | `` |  |
  | `na_key4` | `` |  |
  | `na_memo` | `` |  |
  | `na_open` | `1.0` |  |
  | `na_post` | `0.0` |  |
  | `na_project` | `S020` |  |
  | `na_prycr` | `0.0` |  |
  | `na_prydr` | `0.0` |  |
  | `na_ptdcr` | `0.0` |  |
  | `na_ptddr` | `0.0` |  |
  | `na_redist` | `0.0` |  |
  | `na_repkey1` | `001` |  |
  | `na_repkey2` | `` |  |
  | `na_repkey3` | `` |  |
  | `na_repkey4` | `` |  |
  | `na_repkey5` | `` |  |
  | `na_subt` | `00` |  |
  | `na_type` | `A` |  |
  | `na_ytdcr` | `0.0` |  |
  | `na_ytddr` | `0.0` |  |
  | `sq_private` | `0.0` |  |

**nextid**
  *1 row(s) modified*

---

### Nominal journal (template not posted - no vat)

*Manual nominal journal entry. Creates: ntran (debit + credit), nacnt updates.*

**ndetl**
  *2 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `nd_account` | `C210` |  |
  | `nd_comm` | `sdfs` |  |
  | `nd_cramnt` | `0.0` |  |
  | `nd_dramnt` | `100.0` |  |
  | `nd_fcdec` | `0.0` |  |
  | `nd_fcmult` | `0.0` |  |
  | `nd_fcrate` | `0.0` |  |
  | `nd_fcurr` | `` | Foreign currency field |
  | `nd_fvalue` | `0.0` |  |
  | `nd_job` | `` |  |
  | `nd_project` | `` |  |
  | `nd_recno` | `1.0` |  |
  | `nd_ref` | `JNL0000060` |  |
  | `nd_tfcdec` | `0.0` |  |
  | `nd_tfcmult` | `0.0` |  |
  | `nd_tfcrate` | `0.0` |  |
  | `nd_tfcurr` | `` | Foreign currency field |
  | `nd_tfvalue` | `0.0` |  |
  | `nd_vatcde` | `` |  |
  | `nd_vattyp` | `` |  |
  | `nd_vatval` | `0.0` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `nd_account` | `C210` |  |
  | `nd_comm` | `sdfs` |  |
  | `nd_cramnt` | `100.0` |  |
  | `nd_dramnt` | `0.0` |  |
  | `nd_fcdec` | `0.0` |  |
  | `nd_fcmult` | `0.0` |  |
  | `nd_fcrate` | `0.0` |  |
  | `nd_fcurr` | `` | Foreign currency field |
  | `nd_fvalue` | `0.0` |  |
  | `nd_job` | `` |  |
  | `nd_project` | `` |  |
  | `nd_recno` | `2.0` |  |
  | `nd_ref` | `JNL0000060` |  |
  | `nd_tfcdec` | `0.0` |  |
  | `nd_tfcmult` | `0.0` |  |
  | `nd_tfcrate` | `0.0` |  |
  | `nd_tfcurr` | `` | Foreign currency field |
  | `nd_tfvalue` | `0.0` |  |
  | `nd_vatcde` | `` |  |
  | `nd_vattyp` | `` |  |
  | `nd_vatval` | `0.0` |  |

**nextid**
  *2 row(s) modified*

**nhead**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `nh_crtot` | `100.0` |  |
  | `nh_date` | `2024-05-01T00:00:00` |  |
  | `nh_days` | `0.0` |  |
  | `nh_drtot` | `100.0` |  |
  | `nh_fcdec` | `0.0` |  |
  | `nh_fcmult` | `0.0` |  |
  | `nh_fcrate` | `0.0` |  |
  | `nh_fcrtot` | `0.0` |  |
  | `nh_fcurr` | `` | Foreign currency field |
  | `nh_fdrtot` | `0.0` |  |
  | `nh_freq` | `` |  |
  | `nh_inp` | `TEST` |  |
  | `nh_journal` | `0.0` | From nparm.np_nexjrnl |
  | `nh_memo` | `` |  |
  | `nh_narr` | `test3` |  |
  | `nh_periods` | `0.0` | From nominal calendar |
  | `nh_plast` | `0.0` |  |
  | `nh_recur` | `0.0` |  |
  | `nh_ref` | `JNL0000060` |  |
  | `nh_retain` | `1.0` |  |
  | `nh_rev` | `0.0` |  |
  | `nh_times` | `0.0` |  |
  | `nh_vatanal` | `0.0` |  |
  | `nh_ylast` | `0.0` |  |

**nparm**
  *1 row(s) modified*

---

### Nominal journal (template not posted)

*Manual nominal journal entry. Creates: ntran (debit + credit), nacnt updates.*

**ndetl**
  *2 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `nd_account` | `E330     TEC` |  |
  | `nd_comm` | `test` |  |
  | `nd_cramnt` | `0.0` |  |
  | `nd_dramnt` | `100.0` |  |
  | `nd_fcdec` | `0.0` |  |
  | `nd_fcmult` | `0.0` |  |
  | `nd_fcrate` | `0.0` |  |
  | `nd_fcurr` | `` | Foreign currency field |
  | `nd_fvalue` | `0.0` |  |
  | `nd_job` | `` |  |
  | `nd_project` | `` |  |
  | `nd_recno` | `1.0` |  |
  | `nd_ref` | `JNL0000059` |  |
  | `nd_taxdate` | `2024-05-01T00:00:00` |  |
  | `nd_tfcdec` | `0.0` |  |
  | `nd_tfcmult` | `0.0` |  |
  | `nd_tfcrate` | `0.0` |  |
  | `nd_tfcurr` | `` | Foreign currency field |
  | `nd_tfvalue` | `0.0` |  |
  | `nd_vatcde` | `1` |  |
  | `nd_vattyp` | `P` |  |
  | `nd_vatval` | `16.67` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `nd_account` | `A150` |  |
  | `nd_comm` | `test` |  |
  | `nd_cramnt` | `100.0` |  |
  | `nd_dramnt` | `0.0` |  |
  | `nd_fcdec` | `0.0` |  |
  | `nd_fcmult` | `0.0` |  |
  | `nd_fcrate` | `0.0` |  |
  | `nd_fcurr` | `` | Foreign currency field |
  | `nd_fvalue` | `0.0` |  |
  | `nd_job` | `` |  |
  | `nd_project` | `` |  |
  | `nd_recno` | `2.0` |  |
  | `nd_ref` | `JNL0000059` |  |
  | `nd_taxdate` | `2024-05-01T00:00:00` |  |
  | `nd_tfcdec` | `0.0` |  |
  | `nd_tfcmult` | `0.0` |  |
  | `nd_tfcrate` | `0.0` |  |
  | `nd_tfcurr` | `` | Foreign currency field |
  | `nd_tfvalue` | `0.0` |  |
  | `nd_vatcde` | `1` |  |
  | `nd_vattyp` | `S` |  |
  | `nd_vatval` | `16.67` |  |

**nextid**
  *2 row(s) modified*

**nhead**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `nh_crtot` | `100.0` |  |
  | `nh_date` | `2024-05-01T00:00:00` |  |
  | `nh_days` | `0.0` |  |
  | `nh_drtot` | `100.0` |  |
  | `nh_fcdec` | `0.0` |  |
  | `nh_fcmult` | `0.0` |  |
  | `nh_fcrate` | `0.0` |  |
  | `nh_fcrtot` | `0.0` |  |
  | `nh_fcurr` | `` | Foreign currency field |
  | `nh_fdrtot` | `0.0` |  |
  | `nh_freq` | `` |  |
  | `nh_inp` | `TEST` |  |
  | `nh_journal` | `0.0` | From nparm.np_nexjrnl |
  | `nh_memo` | `` |  |
  | `nh_narr` | `test2` |  |
  | `nh_periods` | `0.0` | From nominal calendar |
  | `nh_plast` | `0.0` |  |
  | `nh_recur` | `0.0` |  |
  | `nh_ref` | `JNL0000059` |  |
  | `nh_retain` | `1.0` |  |
  | `nh_rev` | `0.0` |  |
  | `nh_times` | `0.0` |  |
  | `nh_vatanal` | `1.0` |  |
  | `nh_ylast` | `0.0` |  |

**nparm**
  *1 row(s) modified*

---

### Sales transfer

*Payment to nominal account (no ledger). Creates: aentry, atran, ntran, anoml, nacnt, nbank.*

**idtab**
  *1 row(s) modified*

**nacnt**
  *7 row(s) modified*

**nextid**
  *3 row(s) modified*

**nhist**
  *5 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `nh_bal` | `-100.0` |  |
  | `nh_budg` | `0.0` |  |
  | `nh_fbal` | `0.0` |  |
  | `nh_job` | `` |  |
  | `nh_nacnt` | `K110` |  |
  | `nh_ncntr` | `SAL` |  |
  | `nh_nsubt` | `01` |  |
  | `nh_ntype` | `30` |  |
  | `nh_period` | `5.0` | From nominal calendar |
  | `nh_project` | `` |  |
  | `nh_ptdcr` | `-100.0` |  |
  | `nh_ptddr` | `0.0` |  |
  | `nh_rbudg` | `0.0` |  |
  | `nh_rectype` | `1.0` |  |
  | `nh_year` | `2024.0` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `nh_bal` | `-6568.31` |  |
  | `nh_budg` | `0.0` |  |
  | `nh_fbal` | `0.0` |  |
  | `nh_job` | `` |  |
  | `nh_nacnt` | `K120` |  |
  | `nh_ncntr` | `` |  |
  | `nh_nsubt` | `02` |  |
  | `nh_ntype` | `30` |  |
  | `nh_period` | `5.0` | From nominal calendar |
  | `nh_project` | `` |  |
  | `nh_ptdcr` | `-6568.31` |  |
  | `nh_ptddr` | `0.0` |  |
  | `nh_rbudg` | `0.0` |  |
  | `nh_rectype` | `1.0` |  |
  | `nh_year` | `2024.0` |  |

  Row 3:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `nh_bal` | `-4828.13` |  |
  | `nh_budg` | `0.0` |  |
  | `nh_fbal` | `0.0` |  |
  | `nh_job` | `` |  |
  | `nh_nacnt` | `K120` |  |
  | `nh_ncntr` | `LSG` |  |
  | `nh_nsubt` | `02` |  |
  | `nh_ntype` | `30` |  |
  | `nh_period` | `5.0` | From nominal calendar |
  | `nh_project` | `` |  |
  | `nh_ptdcr` | `-4828.13` |  |
  | `nh_ptddr` | `0.0` |  |
  | `nh_rbudg` | `0.0` |  |
  | `nh_rectype` | `1.0` |  |
  | `nh_year` | `2024.0` |  |

  *2 row(s) modified*

**njmemo**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `nj_binrep` | `0.0` |  |
  | `nj_image` | `` |  |
  | `nj_journal` | `3443.0` | From nparm.np_nexjrnl |
  | `nj_memo` | `ÿ<<JOURNAL_DATA_ONLY>>ÿ` |  |
  | `nj_txtrep` | `Sales Ledger Transfer` |  |

**nsubt**
  *5 row(s) modified*

**ntype**
  *3 row(s) modified*

**snoml**
  *35 row(s) modified*

---

### Stock Transfer

*Payment to nominal account (no ledger). Creates: aentry, atran, ntran, anoml, nacnt, nbank.*

**cnoml**
  *34 row(s) modified*

**idtab**
  *1 row(s) modified*

**nacnt**
  *3 row(s) modified*

**nextid**
  *3 row(s) modified*

**nhist**
  *3 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `nh_bal` | `-9117.68` |  |
  | `nh_budg` | `0.0` |  |
  | `nh_fbal` | `0.0` |  |
  | `nh_job` | `` |  |
  | `nh_nacnt` | `C210` |  |
  | `nh_ncntr` | `` |  |
  | `nh_nsubt` | `02` |  |
  | `nh_ntype` | `10` |  |
  | `nh_period` | `5.0` | From nominal calendar |
  | `nh_project` | `` |  |
  | `nh_ptdcr` | `-9663.68` |  |
  | `nh_ptddr` | `546.0` |  |
  | `nh_rbudg` | `0.0` |  |
  | `nh_rectype` | `1.0` |  |
  | `nh_year` | `2024.0` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `nh_bal` | `-546.0` |  |
  | `nh_budg` | `0.0` |  |
  | `nh_fbal` | `0.0` |  |
  | `nh_job` | `` |  |
  | `nh_nacnt` | `M510` |  |
  | `nh_ncntr` | `` |  |
  | `nh_nsubt` | `02` |  |
  | `nh_ntype` | `35` |  |
  | `nh_period` | `5.0` | From nominal calendar |
  | `nh_project` | `` |  |
  | `nh_ptdcr` | `-546.0` |  |
  | `nh_ptddr` | `0.0` |  |
  | `nh_rbudg` | `0.0` |  |
  | `nh_rectype` | `1.0` |  |
  | `nh_year` | `2024.0` |  |

  Row 3:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `nh_bal` | `9663.68` |  |
  | `nh_budg` | `0.0` |  |
  | `nh_fbal` | `0.0` |  |
  | `nh_job` | `` |  |
  | `nh_nacnt` | `M520` |  |
  | `nh_ncntr` | `` |  |
  | `nh_nsubt` | `02` |  |
  | `nh_ntype` | `35` |  |
  | `nh_period` | `5.0` | From nominal calendar |
  | `nh_project` | `` |  |
  | `nh_ptdcr` | `0.0` |  |
  | `nh_ptddr` | `9663.68` |  |
  | `nh_rbudg` | `0.0` |  |
  | `nh_rectype` | `1.0` |  |
  | `nh_year` | `2024.0` |  |

**njmemo**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `nj_binrep` | `0.0` |  |
  | `nj_image` | `` |  |
  | `nj_journal` | `3445.0` | From nparm.np_nexjrnl |
  | `nj_memo` | `ÿ<<JOURNAL_DATA_ONLY>>ÿ` |  |
  | `nj_txtrep` | `Stock Transfer` |  |

**nsubt**
  *2 row(s) modified*

**ntype**
  *2 row(s) modified*

---

### purchase transfer

*Payment to nominal account (no ledger). Creates: aentry, atran, ntran, anoml, nacnt, nbank.*

**idtab**
  *1 row(s) modified*

**nacnt**
  *14 row(s) modified*

**nextid**
  *3 row(s) modified*

**nhist**
  *4 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `nh_bal` | `23587.59` |  |
  | `nh_budg` | `0.0` |  |
  | `nh_fbal` | `0.0` |  |
  | `nh_job` | `` |  |
  | `nh_nacnt` | `M210` |  |
  | `nh_ncntr` | `` |  |
  | `nh_nsubt` | `01` |  |
  | `nh_ntype` | `35` |  |
  | `nh_period` | `5.0` | From nominal calendar |
  | `nh_project` | `` |  |
  | `nh_ptdcr` | `0.0` |  |
  | `nh_ptddr` | `23587.59` |  |
  | `nh_rbudg` | `0.0` |  |
  | `nh_rectype` | `1.0` |  |
  | `nh_year` | `2024.0` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `nh_bal` | `73751.11` |  |
  | `nh_budg` | `0.0` |  |
  | `nh_fbal` | `0.0` |  |
  | `nh_job` | `` |  |
  | `nh_nacnt` | `M310` |  |
  | `nh_ncntr` | `` |  |
  | `nh_nsubt` | `01` |  |
  | `nh_ntype` | `35` |  |
  | `nh_period` | `5.0` | From nominal calendar |
  | `nh_project` | `` |  |
  | `nh_ptdcr` | `0.0` |  |
  | `nh_ptddr` | `73751.11` |  |
  | `nh_rbudg` | `0.0` |  |
  | `nh_rectype` | `1.0` |  |
  | `nh_year` | `2024.0` |  |

  Row 3:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `nh_bal` | `2622.4` |  |
  | `nh_budg` | `0.0` |  |
  | `nh_fbal` | `0.0` |  |
  | `nh_job` | `` |  |
  | `nh_nacnt` | `M315` |  |
  | `nh_ncntr` | `` |  |
  | `nh_nsubt` | `01` |  |
  | `nh_ntype` | `35` |  |
  | `nh_period` | `5.0` | From nominal calendar |
  | `nh_project` | `` |  |
  | `nh_ptdcr` | `0.0` |  |
  | `nh_ptddr` | `2622.4` |  |
  | `nh_rbudg` | `0.0` |  |
  | `nh_rectype` | `1.0` |  |
  | `nh_year` | `2024.0` |  |

  *10 row(s) modified*

**njmemo**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `nj_binrep` | `0.0` |  |
  | `nj_image` | `` |  |
  | `nj_journal` | `3442.0` | From nparm.np_nexjrnl |
  | `nj_memo` | `ÿ<<JOURNAL_DATA_ONLY>>ÿ` |  |
  | `nj_txtrep` | `Purchase Ledger Transfer` |  |

**nsubt**
  *4 row(s) modified*

**ntype**
  *3 row(s) modified*

**pnoml**
  *95 row(s) modified*

---

## Purchase Ledger Transactions

### Purchase Invoice

*Purchase invoice posting. Creates: ptran, pnoml, ntran, nacnt, pname balance.*

**dmcomp**
  *1 row(s) modified*

**idtab**
  *1 row(s) modified*

**nacnt**
  *3 row(s) modified*

**nextid**
  *6 row(s) modified*

**nhist**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `nh_bal` | `100.0` |  |
  | `nh_budg` | `0.0` |  |
  | `nh_fbal` | `0.0` |  |
  | `nh_job` | `VM` |  |
  | `nh_nacnt` | `M310` |  |
  | `nh_ncntr` | `` |  |
  | `nh_nsubt` | `01` |  |
  | `nh_ntype` | `35` |  |
  | `nh_period` | `5.0` | From nominal calendar |
  | `nh_project` | `VM1` |  |
  | `nh_ptdcr` | `0.0` |  |
  | `nh_ptddr` | `100.0` |  |
  | `nh_rbudg` | `0.0` |  |
  | `nh_rectype` | `1.0` |  |
  | `nh_year` | `2024.0` |  |

  *2 row(s) modified*

**njmemo**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `nj_binrep` | `0.0` |  |
  | `nj_image` | `` |  |
  | `nj_journal` | `3449.0` | From nparm.np_nexjrnl |
  | `nj_memo` | `ÿ<<JOURNAL_DATA_ONLY>>ÿ` |  |
  | `nj_txtrep` | `Purchase Ledger Transfer` |  |

**nsubt**
  *3 row(s) modified*

**ntype**
  *2 row(s) modified*

**panal**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `pa_account` | `CAR0001` |  |
  | `pa_adjsv` | `0.0` |  |
  | `pa_advance` | `N` |  |
  | `pa_ancode` | `M310` |  |
  | `pa_anvat` | `1` |  |
  | `pa_box1` | `0.0` |  |
  | `pa_box2` | `0.0` |  |
  | `pa_box4` | `1.0` |  |
  | `pa_box6` | `0.0` |  |
  | `pa_box7` | `1.0` |  |
  | `pa_box9` | `0.0` |  |
  | `pa_commod` | `` |  |
  | `pa_cost` | `0.0` |  |
  | `pa_country` | `GB` |  |
  | `pa_crdate` | `2024-05-01T00:00:00` |  |
  | `pa_ctryorg` | `` |  |
  | `pa_daccnt` | `CAR0001` |  |
  | `pa_delterm` | `` |  |
  | `pa_domrc` | `0.0` |  |
  | `pa_facatg` | `` |  |
  | `pa_fadesc` | `` |  |
  | `pa_fasset` | `` |  |
  | `pa_fasub` | `` |  |
  | `pa_fccost` | `0.0` |  |
  | `pa_fcdec` | `2.0` | Foreign currency field |
  | `pa_fcurr` | `` | Foreign currency field |
  | `pa_fcval` | `0.0` |  |
  | `pa_fcvat` | `0.0` |  |
  | `pa_interbr` | `0.0` |  |
  | `pa_jccode` | `` |  |
  | `pa_jcstdoc` | `` |  |
  | `pa_jline` | `` |  |
  | `pa_job` | `VM` |  |
  | `pa_jphase` | `` |  |
  | `pa_netmass` | `0.0` |  |
  | `pa_nrthire` | `0.0` |  |
  | `pa_project` | `VM1` |  |
  | `pa_pvaimp` | `0.0` |  |
  | `pa_qty` | `0.0` |  |
  | `pa_regctry` | `` |  |
  | `pa_regvat` | `` |  |
  | `pa_sentvat` | `0.0` |  |
  | `pa_setdisc` | `0.0` |  |
  | `pa_ssdfval` | `0.0` |  |
  | `pa_ssdpost` | `0.0` |  |
  | `pa_ssdpre` | `0.0` |  |
  | `pa_ssdsupp` | `0.0` |  |
  | `pa_ssdval` | `0.0` |  |
  | `pa_supanal` | `` |  |
  | `pa_suppqty` | `0.0` |  |
  | `pa_suptype` | `PR` |  |
  | `pa_taxdate` | `2024-05-01T00:00:00` |  |
  | `pa_transac` | `` |  |
  | `pa_transpt` | `` |  |
  | `pa_trdate` | `2024-05-01T00:00:00` |  |
  | `pa_trref` | `p inv` |  |
  | `pa_trtype` | `I` |  |
  | `pa_trvalue` | `100.0` |  |
  | `pa_vatctry` | `H` |  |
  | `pa_vatrate` | `20.0` |  |
  | `pa_vatset1` | `0.0` |  |
  | `pa_vatset2` | `0.0` |  |
  | `pa_vattype` | `P` |  |
  | `pa_vatval` | `20.0` |  |

**pname**
  *1 row(s) modified*

**pnoml**
  *3 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `px_cdesc` | `` |  |
  | `px_comment` | `Carters Limited` |  |
  | `px_date` | `2024-05-01T00:00:00` |  |
  | `px_done` | `Y` | NL transfer complete |
  | `px_fcdec` | `0.0` |  |
  | `px_fcmult` | `0.0` |  |
  | `px_fcrate` | `0.0` |  |
  | `px_fcurr` | `` | Foreign currency field |
  | `px_fvalue` | `0.0` |  |
  | `px_job` | `VM` |  |
  | `px_jrnl` | `3449.0` | From nparm.np_nexjrnl |
  | `px_nacnt` | `M310` |  |
  | `px_ncntr` | `` |  |
  | `px_nlpdate` | `2024-05-01T00:00:00` |  |
  | `px_project` | `VM1` |  |
  | `px_srcco` | `Z` |  |
  | `px_tref` | `Purchases - Maintenance       p inv` |  |
  | `px_type` | `I` |  |
  | `px_unique` | `_7FL0V1TSB` | Base-36 unique ID |
  | `px_value` | `100.0` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `px_cdesc` | `` |  |
  | `px_comment` | `Carters Limited` |  |
  | `px_date` | `2024-05-01T00:00:00` |  |
  | `px_done` | `Y` | NL transfer complete |
  | `px_fcdec` | `0.0` |  |
  | `px_fcmult` | `0.0` |  |
  | `px_fcrate` | `0.0` |  |
  | `px_fcurr` | `` | Foreign currency field |
  | `px_fvalue` | `0.0` |  |
  | `px_job` | `` |  |
  | `px_jrnl` | `3449.0` | From nparm.np_nexjrnl |
  | `px_nacnt` | `E225` |  |
  | `px_ncntr` | `` |  |
  | `px_nlpdate` | `2024-05-01T00:00:00` |  |
  | `px_project` | `` |  |
  | `px_srcco` | `Z` |  |
  | `px_tref` | `Purchases - Maintenance       p inv` |  |
  | `px_type` | `I` |  |
  | `px_unique` | `_7FL0V1TSB` | Base-36 unique ID |
  | `px_value` | `20.0` |  |

  Row 3:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `px_cdesc` | `` |  |
  | `px_comment` | `Carters Limited` |  |
  | `px_date` | `2024-05-01T00:00:00` |  |
  | `px_done` | `Y` | NL transfer complete |
  | `px_fcdec` | `0.0` |  |
  | `px_fcmult` | `0.0` |  |
  | `px_fcrate` | `0.0` |  |
  | `px_fcurr` | `` | Foreign currency field |
  | `px_fvalue` | `0.0` |  |
  | `px_job` | `` |  |
  | `px_jrnl` | `3449.0` | From nparm.np_nexjrnl |
  | `px_nacnt` | `E110` |  |
  | `px_ncntr` | `` |  |
  | `px_nlpdate` | `2024-05-01T00:00:00` |  |
  | `px_project` | `` |  |
  | `px_srcco` | `Z` |  |
  | `px_tref` | `p inv` |  |
  | `px_type` | `I` |  |
  | `px_unique` | `_7FL0V1TSB` | Base-36 unique ID |
  | `px_value` | `-120.0` |  |

**ptran**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `pt_account` | `CAR0001` |  |
  | `pt_adjsv` | `0.0` |  |
  | `pt_adval` | `0.0` |  |
  | `pt_advance` | `N` |  |
  | `pt_apadoc` | `` |  |
  | `pt_cbtype` | `` |  |
  | `pt_crdate` | `2024-05-01T00:00:00` |  |
  | `pt_dueday` | `2024-05-31T00:00:00` |  |
  | `pt_entry` | `` |  |
  | `pt_eurind` | `` |  |
  | `pt_euro` | `0.0` |  |
  | `pt_fadval` | `0.0` |  |
  | `pt_fcbal` | `0.0` |  |
  | `pt_fcdec` | `0.0` |  |
  | `pt_fcmult` | `0.0` |  |
  | `pt_fcrate` | `0.0` |  |
  | `pt_fcurr` | `` | Foreign currency field |
  | `pt_fcval` | `0.0` |  |
  | `pt_fcvat` | `0.0` |  |
  | `pt_held` | `N` |  |
  | `pt_memo` | `Analysis of Invoice p inv                Dated 01/05/2024...` |  |
  | `pt_nlpdate` | `2024-05-01T00:00:00` |  |
  | `pt_origcur` | `` |  |
  | `pt_paid` | `` |  |
  | `pt_payadvl` | `0.0` |  |
  | `pt_payflag` | `0.0` |  |
  | `pt_plimage` | `` |  |
  | `pt_pyroute` | `0.0` |  |
  | `pt_rcode` | `` |  |
  | `pt_revchrg` | `0.0` |  |
  | `pt_ruser` | `` |  |
  | `pt_set1` | `0.0` |  |
  | `pt_set1day` | `0.0` |  |
  | `pt_set2` | `0.0` |  |
  | `pt_set2day` | `0.0` |  |
  | `pt_supref` | `` |  |
  | `pt_suptype` | `PR` |  |
  | `pt_trbal` | `120.0` |  |
  | `pt_trdate` | `2024-05-01T00:00:00` |  |
  | `pt_trref` | `p inv` |  |
  | `pt_trtype` | `I` |  |
  | `pt_trvalue` | `120.0` |  |
  | `pt_unique` | `_7FL0V1TSB` | Base-36 unique ID |
  | `pt_vatset1` | `0.0` |  |
  | `pt_vatset2` | `0.0` |  |
  | `pt_vatval` | `20.0` |  |

**zcontacts**
  *2 row(s) modified*

---

### Purchase payment bacs

*Purchase invoice posting. Creates: ptran, pnoml, ntran, nacnt, pname balance.*

**aentry**
  *1 row(s) modified*

**anoml**
  *2 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ax_comment` | `Carters Limited               BACS` |  |
  | `ax_date` | `2024-05-01T00:00:00` |  |
  | `ax_done` | `Y` | NL transfer complete |
  | `ax_fcdec` | `0.0` |  |
  | `ax_fcmult` | `0.0` |  |
  | `ax_fcrate` | `0.0` |  |
  | `ax_fcurr` | `` | Foreign currency field |
  | `ax_fvalue` | `0.0` |  |
  | `ax_job` | `` |  |
  | `ax_jrnl` | `3450.0` | From nparm.np_nexjrnl |
  | `ax_nacnt` | `C310` |  |
  | `ax_ncntr` | `` |  |
  | `ax_nlpdate` | `2024-05-01T00:00:00` |  |
  | `ax_project` | `` |  |
  | `ax_source` | `P` |  |
  | `ax_srcco` | `Z` |  |
  | `ax_tref` | `test` |  |
  | `ax_unique` | `_7FL0VGKLX` | Base-36 unique ID |
  | `ax_value` | `-599.0` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ax_comment` | `Carters Limited               BACS` |  |
  | `ax_date` | `2024-05-01T00:00:00` |  |
  | `ax_done` | `Y` | NL transfer complete |
  | `ax_fcdec` | `0.0` |  |
  | `ax_fcmult` | `0.0` |  |
  | `ax_fcrate` | `0.0` |  |
  | `ax_fcurr` | `` | Foreign currency field |
  | `ax_fvalue` | `0.0` |  |
  | `ax_job` | `` |  |
  | `ax_jrnl` | `3450.0` | From nparm.np_nexjrnl |
  | `ax_nacnt` | `E110` |  |
  | `ax_ncntr` | `` |  |
  | `ax_nlpdate` | `2024-05-01T00:00:00` |  |
  | `ax_project` | `` |  |
  | `ax_source` | `P` |  |
  | `ax_srcco` | `Z` |  |
  | `ax_tref` | `test` |  |
  | `ax_unique` | `_7FL0VGKLX` | Base-36 unique ID |
  | `ax_value` | `599.0` |  |

**atran**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `at_account` | `CAR0001` |  |
  | `at_acnt` | `C310` |  |
  | `at_atpycd` | `` |  |
  | `at_bacprn` | `0.0` |  |
  | `at_bic` | `` |  |
  | `at_bsname` | `` |  |
  | `at_bsref` | `` |  |
  | `at_cash` | `0.0` |  |
  | `at_cbtype` | `P2` |  |
  | `at_ccauth` | `` |  |
  | `at_ccdno` | `` |  |
  | `at_ccdprn` | `0.0` |  |
  | `at_chqlst` | `0.0` |  |
  | `at_chqprn` | `0.0` |  |
  | `at_cntr` | `` |  |
  | `at_comment` | `` |  |
  | `at_disc` | `0.0` |  |
  | `at_ecb` | `0.0` |  |
  | `at_ecbtype` | `` |  |
  | `at_entry` | `P200000427` | From atype counter |
  | `at_fcdec` | `2.0` | Foreign currency field |
  | `at_fcexch` | `1.0` | Foreign currency field |
  | `at_fcmult` | `0.0` |  |
  | `at_fcurr` | `` | Foreign currency field |
  | `at_iban` | `` |  |
  | `at_inputby` | `TEST` |  |
  | `at_job` | `` |  |
  | `at_memo` | `` |  |
  | `at_name` | `Carters Limited` |  |
  | `at_number` | `` |  |
  | `at_payee` | `` |  |
  | `at_payname` | `Carters (UK) Limited` |  |
  | `at_payslp` | `0.0` |  |
  | `at_postgrp` | `7.0` |  |
  | `at_project` | `` |  |
  | `at_pstdate` | `2024-05-01T00:00:00` |  |
  | `at_pysprn` | `0.0` |  |
  | `at_refer` | `test` |  |
  | `at_remit` | `0.0` |  |
  | `at_remove` | `0.0` |  |
  | `at_sort` | `` |  |
  | `at_srcco` | `Z` |  |
  | `at_sysdate` | `2024-05-01T00:00:00` |  |
  | `at_tperiod` | `1.0` | From nominal calendar |
  | `at_type` | `5.0` |  |
  | `at_unique` | `_7FL0VGKLX` | Base-36 unique ID |
  | `at_value` | `-59900.0` |  |
  | `at_vattycd` | `` |  |

**dmcomp**
  *1 row(s) modified*

**idtab**
  *1 row(s) modified*

**nacnt**
  *2 row(s) modified*

**nbank**
  *1 row(s) modified*

**ndetail**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `nt_acnt` | `C310` |  |
  | `nt_cdesc` | `` |  |
  | `nt_cmnt` | `test` |  |
  | `nt_cntr` | `` |  |
  | `nt_consol` | `0.0` |  |
  | `nt_distrib` | `0.0` |  |
  | `nt_entr` | `2024-05-01T00:00:00` |  |
  | `nt_fcdec` | `0.0` |  |
  | `nt_fcmult` | `0.0` |  |
  | `nt_fcrate` | `0.0` |  |
  | `nt_fcurr` | `` | Foreign currency field |
  | `nt_fvalue` | `0.0` |  |
  | `nt_inp` | `TEST` |  |
  | `nt_job` | `` |  |
  | `nt_jrnl` | `3450.0` | From nparm.np_nexjrnl |
  | `nt_period` | `5.0` | From nominal calendar |
  | `nt_perpost` | `0.0` |  |
  | `nt_posttyp` | `P` |  |
  | `nt_prevyr` | `0.0` |  |
  | `nt_project` | `` |  |
  | `nt_pstgrp` | `1.0` |  |
  | `nt_pstid` | `_7FL0VH8YK` |  |
  | `nt_recjrnl` | `0.0` | From nparm.np_nexjrnl |
  | `nt_rectify` | `0.0` |  |
  | `nt_recurr` | `0.0` |  |
  | `nt_ref` | `` |  |
  | `nt_rvrse` | `0.0` |  |
  | `nt_srcco` | `Z` |  |
  | `nt_subt` | `03` |  |
  | `nt_trnref` | `Carters Limited               BACS` |  |
  | `nt_trtype` | `A` |  |
  | `nt_type` | `10` |  |
  | `nt_value` | `-599.0` |  |
  | `nt_vatanal` | `0.0` |  |
  | `nt_year` | `2024.0` |  |

**nextid**
  *7 row(s) modified*

**nhist**
  *2 row(s) modified*

**njmemo**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `nj_binrep` | `0.0` |  |
  | `nj_image` | `` |  |
  | `nj_journal` | `3450.0` | From nparm.np_nexjrnl |
  | `nj_memo` | `ÿ<<JOURNAL_DATA_ONLY>>ÿ` |  |
  | `nj_txtrep` | `Cashbook Ledger Transfer` |  |

**nsubt**
  *2 row(s) modified*

**ntype**
  *2 row(s) modified*

**palloc**
  *2 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `al_account` | `CAR0001` |  |
  | `al_acnt` | `C310` |  |
  | `al_adjsv` | `0.0` |  |
  | `al_advind` | `0.0` |  |
  | `al_advtran` | `0.0` |  |
  | `al_bacsid` | `0.0` |  |
  | `al_cheq` | `` |  |
  | `al_cnclchq` | `` |  |
  | `al_cntr` | `` |  |
  | `al_ctype` | `P` |  |
  | `al_date` | `2024-05-03T00:00:00` |  |
  | `al_dval` | `0.0` |  |
  | `al_fcurr` | `` | Foreign currency field |
  | `al_fdec` | `0.0` |  |
  | `al_fdval` | `0.0` |  |
  | `al_forigvl` | `0.0` |  |
  | `al_fval` | `59900.0` |  |
  | `al_origval` | `87923.38` |  |
  | `al_payday` | `2024-05-01T00:00:00` |  |
  | `al_payee` | `` |  |
  | `al_payflag` | `104.0` |  |
  | `al_payind` | `P` |  |
  | `al_preprd` | `0.0` |  |
  | `al_ref1` | `CART-0524-45456M90` |  |
  | `al_ref2` | `` |  |
  | `al_rem` | `` |  |
  | `al_type` | `I` |  |
  | `al_unique` | `4515.0` | Base-36 unique ID |
  | `al_val` | `599.0` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `al_account` | `CAR0001` |  |
  | `al_acnt` | `C310` |  |
  | `al_adjsv` | `0.0` |  |
  | `al_advind` | `0.0` |  |
  | `al_advtran` | `0.0` |  |
  | `al_bacsid` | `0.0` |  |
  | `al_cheq` | `S` |  |
  | `al_cnclchq` | `` |  |
  | `al_cntr` | `` |  |
  | `al_ctype` | `C` |  |
  | `al_date` | `2024-05-01T00:00:00` |  |
  | `al_dval` | `0.0` |  |
  | `al_fcurr` | `` | Foreign currency field |
  | `al_fdec` | `0.0` |  |
  | `al_fdval` | `0.0` |  |
  | `al_forigvl` | `0.0` |  |
  | `al_fval` | `0.0` |  |
  | `al_origval` | `-599.0` |  |
  | `al_payday` | `2024-05-01T00:00:00` |  |
  | `al_payee` | `Carters (UK) Limited` |  |
  | `al_payflag` | `104.0` |  |
  | `al_payind` | `A` |  |
  | `al_preprd` | `0.0` |  |
  | `al_ref1` | `test` |  |
  | `al_ref2` | `BACS` |  |
  | `al_rem` | `` |  |
  | `al_type` | `P` |  |
  | `al_unique` | `4554.0` | Base-36 unique ID |
  | `al_val` | `-599.0` |  |

**pname**
  *1 row(s) modified*

**ptran**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `pt_account` | `CAR0001` |  |
  | `pt_adjsv` | `0.0` |  |
  | `pt_adval` | `0.0` |  |
  | `pt_advance` | `N` |  |
  | `pt_apadoc` | `` |  |
  | `pt_cbtype` | `P2` |  |
  | `pt_crdate` | `2024-05-01T00:00:00` |  |
  | `pt_entry` | `P200000429` | From atype counter |
  | `pt_eurind` | `` |  |
  | `pt_euro` | `0.0` |  |
  | `pt_fadval` | `0.0` |  |
  | `pt_fcbal` | `0.0` |  |
  | `pt_fcdec` | `0.0` |  |
  | `pt_fcmult` | `0.0` |  |
  | `pt_fcrate` | `0.0` |  |
  | `pt_fcurr` | `` | Foreign currency field |
  | `pt_fcval` | `0.0` |  |
  | `pt_fcvat` | `0.0` |  |
  | `pt_held` | `` |  |
  | `pt_memo` | `Analysis of Payment test                  Amount       59...` |  |
  | `pt_nlpdate` | `2024-05-01T00:00:00` |  |
  | `pt_origcur` | `` |  |
  | `pt_paid` | `A` |  |
  | `pt_payadvl` | `0.0` |  |
  | `pt_payday` | `2024-05-01T00:00:00` |  |
  | `pt_payflag` | `104.0` |  |
  | `pt_plimage` | `` |  |
  | `pt_pyroute` | `0.0` |  |
  | `pt_rcode` | `` |  |
  | `pt_revchrg` | `0.0` |  |
  | `pt_ruser` | `` |  |
  | `pt_set1` | `0.0` |  |
  | `pt_set1day` | `0.0` |  |
  | `pt_set2` | `0.0` |  |
  | `pt_set2day` | `0.0` |  |
  | `pt_supref` | `BACS` |  |
  | `pt_suptype` | `` |  |
  | `pt_trbal` | `0.0` |  |
  | `pt_trdate` | `2024-05-01T00:00:00` |  |
  | `pt_trref` | `test` |  |
  | `pt_trtype` | `P` |  |
  | `pt_trvalue` | `-599.0` |  |
  | `pt_unique` | `_7FL0VGKLX` | Base-36 unique ID |
  | `pt_vatset1` | `0.0` |  |
  | `pt_vatset2` | `0.0` |  |
  | `pt_vatval` | `0.0` |  |

  *1 row(s) modified*

**zlock**
  *1 row(s) modified*

---

### Refund

**aentry**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `ae_acnt` | `C310` |  |
  | `ae_batchid` | `0.0` |  |
  | `ae_brwptr` | `` |  |
  | `ae_cbtype` | `R4` |  |
  | `ae_cntr` | `` |  |
  | `ae_comment` | `` |  |
  | `ae_complet` | `1.0` |  |
  | `ae_entref` | `test` |  |
  | `ae_entry` | `R400000016` | From atype counter |
  | `ae_frstat` | `0.0` |  |
  | `ae_lstdate` | `2024-05-01T00:00:00` |  |
  | `ae_payid` | `0.0` |  |
  | `ae_postgrp` | `0.0` |  |
  | `ae_recbal` | `0.0` |  |
  | `ae_reclnum` | `0.0` |  |
  | `ae_remove` | `0.0` |  |
  | `ae_statln` | `0.0` |  |
  | `ae_tmpstat` | `0.0` |  |
  | `ae_tostat` | `0.0` |  |
  | `ae_value` | `5000.0` |  |
  | `sq_amtime` | `` |  |
  | `sq_amuser` | `` |  |
  | `sq_crdate` | `2026-04-01T00:00:00` |  |
  | `sq_crtime` | `13:19:34` |  |
  | `sq_cruser` | `TEST` |  |

**anoml**
  *2 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ax_comment` | `Crown Venue Catering Ltd.     Refund` |  |
  | `ax_date` | `2024-05-01T00:00:00` |  |
  | `ax_done` | `` |  |
  | `ax_fcdec` | `0.0` |  |
  | `ax_fcmult` | `0.0` |  |
  | `ax_fcrate` | `0.0` |  |
  | `ax_fcurr` | `` | Foreign currency field |
  | `ax_fvalue` | `0.0` |  |
  | `ax_job` | `` |  |
  | `ax_jrnl` | `0.0` | From nparm.np_nexjrnl |
  | `ax_nacnt` | `C310` |  |
  | `ax_ncntr` | `` |  |
  | `ax_nlpdate` | `2024-05-01T00:00:00` |  |
  | `ax_project` | `` |  |
  | `ax_source` | `P` |  |
  | `ax_srcco` | `Z` |  |
  | `ax_tref` | `test` |  |
  | `ax_unique` | `_7FL0SK9JK` | Base-36 unique ID |
  | `ax_value` | `50.0` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ax_comment` | `Crown Venue Catering Ltd.     Refund` |  |
  | `ax_date` | `2024-05-01T00:00:00` |  |
  | `ax_done` | `` |  |
  | `ax_fcdec` | `0.0` |  |
  | `ax_fcmult` | `0.0` |  |
  | `ax_fcrate` | `0.0` |  |
  | `ax_fcurr` | `` | Foreign currency field |
  | `ax_fvalue` | `0.0` |  |
  | `ax_job` | `` |  |
  | `ax_jrnl` | `0.0` | From nparm.np_nexjrnl |
  | `ax_nacnt` | `E110` |  |
  | `ax_ncntr` | `` |  |
  | `ax_nlpdate` | `2024-05-01T00:00:00` |  |
  | `ax_project` | `` |  |
  | `ax_source` | `P` |  |
  | `ax_srcco` | `Z` |  |
  | `ax_tref` | `test` |  |
  | `ax_unique` | `_7FL0SK9JK` | Base-36 unique ID |
  | `ax_value` | `-50.0` |  |

**atran**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `at_account` | `CVC0001` |  |
  | `at_acnt` | `C310` |  |
  | `at_atpycd` | `` |  |
  | `at_bacprn` | `0.0` |  |
  | `at_bic` | `` |  |
  | `at_bsname` | `` |  |
  | `at_bsref` | `` |  |
  | `at_cash` | `0.0` |  |
  | `at_cbtype` | `R4` |  |
  | `at_ccauth` | `` |  |
  | `at_ccdno` | `` |  |
  | `at_ccdprn` | `0.0` |  |
  | `at_chqlst` | `0.0` |  |
  | `at_chqprn` | `0.0` |  |
  | `at_cntr` | `` |  |
  | `at_comment` | `` |  |
  | `at_disc` | `0.0` |  |
  | `at_ecb` | `0.0` |  |
  | `at_ecbtype` | `` |  |
  | `at_entry` | `R400000016` | From atype counter |
  | `at_fcdec` | `2.0` | Foreign currency field |
  | `at_fcexch` | `1.0` | Foreign currency field |
  | `at_fcmult` | `0.0` |  |
  | `at_fcurr` | `` | Foreign currency field |
  | `at_iban` | `` |  |
  | `at_inputby` | `TEST` |  |
  | `at_job` | `` |  |
  | `at_memo` | `` |  |
  | `at_name` | `Crown Venue Catering Ltd.` |  |
  | `at_number` | `` |  |
  | `at_payee` | `` |  |
  | `at_payname` | `Crown Venue Catering Ltd.` |  |
  | `at_payslp` | `0.0` |  |
  | `at_postgrp` | `0.0` |  |
  | `at_project` | `` |  |
  | `at_pstdate` | `2024-05-01T00:00:00` |  |
  | `at_pysprn` | `0.0` |  |
  | `at_refer` | `test` |  |
  | `at_remit` | `0.0` |  |
  | `at_remove` | `0.0` |  |
  | `at_sort` | `` |  |
  | `at_srcco` | `Z` |  |
  | `at_sysdate` | `2024-05-01T00:00:00` |  |
  | `at_tperiod` | `1.0` | From nominal calendar |
  | `at_type` | `6.0` |  |
  | `at_unique` | `_7FL0SK9JK` | Base-36 unique ID |
  | `at_value` | `5000.0` |  |
  | `at_vattycd` | `` |  |

**atype**
  *1 row(s) modified*

**nbank**
  *1 row(s) modified*

**nextid**
  *5 row(s) modified*

**palloc**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `al_account` | `CVC0001` |  |
  | `al_acnt` | `C310` |  |
  | `al_adjsv` | `0.0` |  |
  | `al_advind` | `0.0` |  |
  | `al_advtran` | `0.0` |  |
  | `al_bacsid` | `0.0` |  |
  | `al_cheq` | `` |  |
  | `al_cnclchq` | `` |  |
  | `al_cntr` | `` |  |
  | `al_ctype` | `O` |  |
  | `al_date` | `2024-05-01T00:00:00` |  |
  | `al_dval` | `0.0` |  |
  | `al_fcurr` | `` | Foreign currency field |
  | `al_fdec` | `0.0` |  |
  | `al_fdval` | `0.0` |  |
  | `al_forigvl` | `0.0` |  |
  | `al_fval` | `0.0` |  |
  | `al_origval` | `50.0` |  |
  | `al_payday` | `2024-05-01T00:00:00` |  |
  | `al_payee` | `Crown Venue Catering Ltd.` |  |
  | `al_payflag` | `0.0` |  |
  | `al_payind` | `P` |  |
  | `al_preprd` | `0.0` |  |
  | `al_ref1` | `test` |  |
  | `al_ref2` | `Refund` |  |
  | `al_rem` | `` |  |
  | `al_type` | `F` |  |
  | `al_unique` | `4552.0` | Base-36 unique ID |
  | `al_val` | `50.0` |  |

**pname**
  *1 row(s) modified*

**ptran**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `pt_account` | `CVC0001` |  |
  | `pt_adjsv` | `0.0` |  |
  | `pt_adval` | `0.0` |  |
  | `pt_advance` | `N` |  |
  | `pt_apadoc` | `` |  |
  | `pt_cbtype` | `R4` |  |
  | `pt_crdate` | `2024-05-01T00:00:00` |  |
  | `pt_entry` | `R400000016` | From atype counter |
  | `pt_eurind` | `` |  |
  | `pt_euro` | `0.0` |  |
  | `pt_fadval` | `0.0` |  |
  | `pt_fcbal` | `0.0` |  |
  | `pt_fcdec` | `0.0` |  |
  | `pt_fcmult` | `0.0` |  |
  | `pt_fcrate` | `0.0` |  |
  | `pt_fcurr` | `` | Foreign currency field |
  | `pt_fcval` | `0.0` |  |
  | `pt_fcvat` | `0.0` |  |
  | `pt_held` | `` |  |
  | `pt_memo` | `` |  |
  | `pt_nlpdate` | `2024-05-01T00:00:00` |  |
  | `pt_origcur` | `` |  |
  | `pt_paid` | `` |  |
  | `pt_payadvl` | `0.0` |  |
  | `pt_payflag` | `0.0` |  |
  | `pt_plimage` | `` |  |
  | `pt_pyroute` | `0.0` |  |
  | `pt_rcode` | `` |  |
  | `pt_revchrg` | `0.0` |  |
  | `pt_ruser` | `` |  |
  | `pt_set1` | `0.0` |  |
  | `pt_set1day` | `0.0` |  |
  | `pt_set2` | `0.0` |  |
  | `pt_set2day` | `0.0` |  |
  | `pt_supref` | `Refund` |  |
  | `pt_suptype` | `` |  |
  | `pt_trbal` | `50.0` |  |
  | `pt_trdate` | `2024-05-01T00:00:00` |  |
  | `pt_trref` | `test` |  |
  | `pt_trtype` | `F` |  |
  | `pt_trvalue` | `50.0` |  |
  | `pt_unique` | `_7FL0SK9JK` | Base-36 unique ID |
  | `pt_vatset1` | `0.0` |  |
  | `pt_vatset2` | `0.0` |  |
  | `pt_vatval` | `0.0` |  |

**zlock**
  *1 row(s) modified*

---

## Sales Ledger Transactions

### Adjustment

*Create a new customer account in sname.*

**dmaddr**
  *1 row(s) modified*

**dmcomp**
  *1 row(s) modified*

**idtab**
  *1 row(s) modified*

**nacnt**
  *2 row(s) modified*

**nextid**
  *4 row(s) modified*

**nsubt**
  *2 row(s) modified*

**ntype**
  *2 row(s) modified*

**sname**
  *1 row(s) modified*

**snoml**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `sx_cdesc` | `` |  |
  | `sx_comment` | `The Athenaeum                 Contra` |  |
  | `sx_date` | `2026-03-31T00:00:00` |  |
  | `sx_done` | `Y` | NL transfer complete |
  | `sx_fcdec` | `0.0` |  |
  | `sx_fcmult` | `0.0` |  |
  | `sx_fcrate` | `0.0` |  |
  | `sx_fcurr` | `` | Foreign currency field |
  | `sx_fvalue` | `0.0` |  |
  | `sx_job` | `` |  |
  | `sx_jrnl` | `48375.0` | From nparm.np_nexjrnl |
  | `sx_nacnt` | `DB020` |  |
  | `sx_ncntr` | `` |  |
  | `sx_nlpdate` | `2026-03-31T00:00:00` |  |
  | `sx_project` | `` |  |
  | `sx_srcco` | `I` |  |
  | `sx_tref` | `test                          adjust` |  |
  | `sx_type` | `A` |  |
  | `sx_unique` | `_7FQ18LFAN` | Base-36 unique ID |
  | `sx_value` | `100.0` |  |

**zcontacts**
  *2 row(s) modified*

---

### Sales Allocation

*Allocate receipt against invoice. Creates: salloc records.*

**dmcomp**
  *1 row(s) modified*

**dmcont**
  *5 row(s) modified*

**nextid**
  *1 row(s) modified*

**salloc**
  *4 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `al_account` | `ADA0001` |  |
  | `al_acnt` | `C310` |  |
  | `al_adjsv` | `0.0` |  |
  | `al_advind` | `0.0` |  |
  | `al_cntr` | `` |  |
  | `al_date` | `2024-01-31T00:00:00` |  |
  | `al_fcurr` | `` | Foreign currency field |
  | `al_fdec` | `0.0` |  |
  | `al_fval` | `0.0` |  |
  | `al_payday` | `2024-05-01T00:00:00` |  |
  | `al_payflag` | `92.0` |  |
  | `al_payind` | `A` |  |
  | `al_preprd` | `0.0` |  |
  | `al_ref1` | `REC-ADA-39393E` |  |
  | `al_ref2` | `BACS` |  |
  | `al_type` | `R` |  |
  | `al_unique` | `9068.0` | Base-36 unique ID |
  | `al_val` | `-10000.0` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `al_account` | `ADA0001` |  |
  | `al_acnt` | `C310` |  |
  | `al_adjsv` | `0.0` |  |
  | `al_advind` | `0.0` |  |
  | `al_cntr` | `` |  |
  | `al_date` | `2024-03-31T00:00:00` |  |
  | `al_fcurr` | `` | Foreign currency field |
  | `al_fdec` | `0.0` |  |
  | `al_fval` | `1000000.0` |  |
  | `al_payday` | `2024-05-01T00:00:00` |  |
  | `al_payflag` | `92.0` |  |
  | `al_payind` | `P` |  |
  | `al_preprd` | `0.0` |  |
  | `al_ref1` | `INV05178` |  |
  | `al_ref2` | `*CONSOLID*` |  |
  | `al_type` | `I` |  |
  | `al_unique` | `9188.0` | Base-36 unique ID |
  | `al_val` | `10000.0` |  |

  Row 3:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `al_account` | `ADA0001` |  |
  | `al_acnt` | `C310` |  |
  | `al_adjsv` | `0.0` |  |
  | `al_advind` | `0.0` |  |
  | `al_cntr` | `` |  |
  | `al_date` | `2024-05-01T00:00:00` |  |
  | `al_fcurr` | `` | Foreign currency field |
  | `al_fdec` | `0.0` |  |
  | `al_fval` | `0.0` |  |
  | `al_payday` | `2024-05-01T00:00:00` |  |
  | `al_payflag` | `92.0` |  |
  | `al_payind` | `A` |  |
  | `al_preprd` | `0.0` |  |
  | `al_ref1` | `re1` |  |
  | `al_ref2` | `ref2` |  |
  | `al_type` | `I` |  |
  | `al_unique` | `9283.0` | Base-36 unique ID |
  | `al_val` | `120.0` |  |

**sname**
  *1 row(s) modified*

**stran**
  *4 row(s) modified*

---

### Sales Credit Note

*Sales credit note posting.*

**dmcomp**
  *1 row(s) modified*

**dmcont**
  *5 row(s) modified*

**idtab**
  *1 row(s) modified*

**nacnt**
  *3 row(s) modified*

**nextid**
  *6 row(s) modified*

**nhist**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `nh_bal` | `100.0` |  |
  | `nh_budg` | `0.0` |  |
  | `nh_fbal` | `0.0` |  |
  | `nh_job` | `` |  |
  | `nh_nacnt` | `K126` |  |
  | `nh_ncntr` | `` |  |
  | `nh_nsubt` | `03` |  |
  | `nh_ntype` | `30` |  |
  | `nh_period` | `5.0` | From nominal calendar |
  | `nh_project` | `` |  |
  | `nh_ptdcr` | `0.0` |  |
  | `nh_ptddr` | `100.0` |  |
  | `nh_rbudg` | `0.0` |  |
  | `nh_rectype` | `1.0` |  |
  | `nh_year` | `2024.0` |  |

  *2 row(s) modified*

**njmemo**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `nj_binrep` | `0.0` |  |
  | `nj_image` | `` |  |
  | `nj_journal` | `3447.0` | From nparm.np_nexjrnl |
  | `nj_memo` | `ÿ<<JOURNAL_DATA_ONLY>>ÿ` |  |
  | `nj_txtrep` | `Sales Ledger Transfer` |  |

**nsubt**
  *3 row(s) modified*

**ntype**
  *3 row(s) modified*

**sanal**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `sa_account` | `ADA0001` |  |
  | `sa_adjsv` | `0.0` |  |
  | `sa_advance` | `N` |  |
  | `sa_ancode` | `CWASH001` |  |
  | `sa_anvat` | `1` |  |
  | `sa_box1` | `1.0` |  |
  | `sa_box6` | `1.0` |  |
  | `sa_box8` | `0.0` |  |
  | `sa_commod` | `` |  |
  | `sa_cost` | `-30.0` |  |
  | `sa_country` | `GB` |  |
  | `sa_crdate` | `2024-05-01T00:00:00` |  |
  | `sa_ctryorg` | `` |  |
  | `sa_cusanal` | `` |  |
  | `sa_custype` | `CPT` |  |
  | `sa_daccnt` | `ADA0001` |  |
  | `sa_delterm` | `` |  |
  | `sa_desc` | `Car Wash/Valet Contracts` |  |
  | `sa_discost` | `0.0` |  |
  | `sa_domrc` | `0.0` |  |
  | `sa_eslproc` | `0.0` |  |
  | `sa_eslsupp` | `0.0` |  |
  | `sa_esltrig` | `0.0` |  |
  | `sa_exten` | `` |  |
  | `sa_fccost` | `0.0` |  |
  | `sa_fcdec` | `2.0` | Foreign currency field |
  | `sa_fcurr` | `` | Foreign currency field |
  | `sa_fcval` | `0.0` |  |
  | `sa_fcvat` | `0.0` |  |
  | `sa_interbr` | `0.0` |  |
  | `sa_jccode` | `` |  |
  | `sa_jcstdoc` | `` |  |
  | `sa_jline` | `` |  |
  | `sa_job` | `` |  |
  | `sa_jphase` | `` |  |
  | `sa_netmass` | `0.0` |  |
  | `sa_nrthire` | `0.0` |  |
  | `sa_product` | `` |  |
  | `sa_project` | `` |  |
  | `sa_qty` | `0.0` |  |
  | `sa_regctry` | `` |  |
  | `sa_region` | `NE` |  |
  | `sa_regvat` | `` |  |
  | `sa_sentvat` | `0.0` |  |
  | `sa_serv` | `0.0` |  |
  | `sa_setdisc` | `0.0` |  |
  | `sa_ssdfval` | `0.0` |  |
  | `sa_ssdpost` | `0.0` |  |
  | `sa_ssdpre` | `0.0` |  |
  | `sa_ssdsupp` | `0.0` |  |
  | `sa_ssdval` | `0.0` |  |
  | `sa_suppqty` | `0.0` |  |
  | `sa_taxdate` | `2024-05-01T00:00:00` |  |
  | `sa_terr` | `EX1` |  |
  | `sa_transac` | `` |  |
  | `sa_transpt` | `` |  |
  | `sa_trdate` | `2024-05-01T00:00:00` |  |
  | `sa_trref` | `test` |  |
  | `sa_trtype` | `C` |  |
  | `sa_trvalue` | `-100.0` |  |
  | `sa_vatctry` | `H` |  |
  | `sa_vatrate` | `20.0` |  |
  | `sa_vattype` | `S` |  |
  | `sa_vatval` | `-20.0` |  |

**sname**
  *1 row(s) modified*

**snoml**
  *2 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `sx_cdesc` | `` |  |
  | `sx_comment` | `Adams Light Engineering Ltd   test` |  |
  | `sx_date` | `2024-05-01T00:00:00` |  |
  | `sx_done` | `Y` | NL transfer complete |
  | `sx_fcdec` | `0.0` |  |
  | `sx_fcmult` | `0.0` |  |
  | `sx_fcrate` | `0.0` |  |
  | `sx_fcurr` | `` | Foreign currency field |
  | `sx_fvalue` | `0.0` |  |
  | `sx_job` | `` |  |
  | `sx_jrnl` | `3447.0` | From nparm.np_nexjrnl |
  | `sx_nacnt` | `K126` |  |
  | `sx_ncntr` | `` |  |
  | `sx_nlpdate` | `2024-05-01T00:00:00` |  |
  | `sx_project` | `` |  |
  | `sx_srcco` | `Z` |  |
  | `sx_tref` | `Car Wash/Valet Contracts      test` |  |
  | `sx_type` | `C` |  |
  | `sx_unique` | `_7FL0UK3NK` | Base-36 unique ID |
  | `sx_value` | `-100.0` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `sx_cdesc` | `` |  |
  | `sx_comment` | `Adams Light Engineering Ltd   test` |  |
  | `sx_date` | `2024-05-01T00:00:00` |  |
  | `sx_done` | `Y` | NL transfer complete |
  | `sx_fcdec` | `0.0` |  |
  | `sx_fcmult` | `0.0` |  |
  | `sx_fcrate` | `0.0` |  |
  | `sx_fcurr` | `` | Foreign currency field |
  | `sx_fvalue` | `0.0` |  |
  | `sx_job` | `` |  |
  | `sx_jrnl` | `3447.0` | From nparm.np_nexjrnl |
  | `sx_nacnt` | `E220` |  |
  | `sx_ncntr` | `` |  |
  | `sx_nlpdate` | `2024-05-01T00:00:00` |  |
  | `sx_project` | `` |  |
  | `sx_srcco` | `Z` |  |
  | `sx_tref` | `Car Wash/Valet Contracts      test` |  |
  | `sx_type` | `C` |  |
  | `sx_unique` | `_7FL0UK3NK` | Base-36 unique ID |
  | `sx_value` | `-20.0` |  |

**stran**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `jxrenewal` | `0.0` |  |
  | `jxservid` | `0.0` |  |
  | `st_account` | `ADA0001` |  |
  | `st_adjsv` | `0.0` |  |
  | `st_advallc` | `0.0` |  |
  | `st_advance` | `N` |  |
  | `st_binrep` | `0.0` |  |
  | `st_cash` | `0.0` |  |
  | `st_cbtype` | `` |  |
  | `st_crdate` | `2024-05-01T00:00:00` |  |
  | `st_custref` | `test` |  |
  | `st_delacc` | `ADA0001` |  |
  | `st_dispute` | `0.0` |  |
  | `st_edi` | `0.0` |  |
  | `st_editx` | `0.0` |  |
  | `st_edivn` | `0.0` |  |
  | `st_entry` | `` |  |
  | `st_eurind` | `` |  |
  | `st_euro` | `0.0` |  |
  | `st_exttime` | `` |  |
  | `st_fadval` | `0.0` |  |
  | `st_fcbal` | `0.0` |  |
  | `st_fcdec` | `0.0` |  |
  | `st_fcmult` | `0.0` |  |
  | `st_fcrate` | `0.0` |  |
  | `st_fcurr` | `` | Foreign currency field |
  | `st_fcval` | `0.0` |  |
  | `st_fcvat` | `0.0` |  |
  | `st_fullamt` | `0.0` |  |
  | `st_fullcb` | `` |  |
  | `st_fullnar` | `` |  |
  | `st_gateid` | `0.0` |  |
  | `st_gatetr` | `0.0` |  |
  | `st_luptime` | `` |  |
  | `st_memo` | `Analysis of Cr.Note test                 Dated 01/05/2024...` |  |
  | `st_nlpdate` | `2024-05-01T00:00:00` |  |
  | `st_origcur` | `` |  |
  | `st_paid` | `` |  |
  | `st_payadvl` | `0.0` |  |
  | `st_payflag` | `0.0` |  |
  | `st_rcode` | `` |  |
  | `st_region` | `NE` |  |
  | `st_revchrg` | `0.0` |  |
  | `st_ruser` | `` |  |
  | `st_set1` | `0.0` |  |
  | `st_set1day` | `0.0` |  |
  | `st_set2` | `0.0` |  |
  | `st_set2day` | `0.0` |  |
  | `st_taxpoin` | `2024-05-01T00:00:00` |  |
  | `st_terr` | `EX1` |  |
  | `st_trbal` | `-120.0` |  |
  | `st_trdate` | `2024-05-01T00:00:00` |  |
  | `st_trref` | `test` |  |
  | `st_trtype` | `C` |  |
  | `st_trvalue` | `-120.0` |  |
  | `st_txtrep` | `` |  |
  | `st_type` | `CPT` |  |
  | `st_unique` | `_7FL0UK3NK` | Base-36 unique ID |
  | `st_vatval` | `-20.0` |  |

**zcontacts**
  *2 row(s) modified*

---

### Sales Invoice

*Sales invoice posting. Creates: stran, snoml, ntran, nacnt, sname balance.*

**dmcomp**
  *1 row(s) modified*

**dmcont**
  *5 row(s) modified*

**nextid**
  *3 row(s) modified*

**sanal**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `sa_account` | `ADA0001` |  |
  | `sa_adjsv` | `0.0` |  |
  | `sa_advance` | `N` |  |
  | `sa_ancode` | `ACCE01` |  |
  | `sa_anvat` | `1` |  |
  | `sa_box1` | `1.0` |  |
  | `sa_box6` | `1.0` |  |
  | `sa_box8` | `0.0` |  |
  | `sa_commod` | `` |  |
  | `sa_cost` | `50.0` |  |
  | `sa_country` | `GB` |  |
  | `sa_crdate` | `2024-05-01T00:00:00` |  |
  | `sa_ctryorg` | `` |  |
  | `sa_cusanal` | `SAL` |  |
  | `sa_custype` | `CPT` |  |
  | `sa_daccnt` | `ADA0001` |  |
  | `sa_delterm` | `` |  |
  | `sa_desc` | `Lease - Accessories` |  |
  | `sa_discost` | `0.0` |  |
  | `sa_domrc` | `0.0` |  |
  | `sa_eslproc` | `0.0` |  |
  | `sa_eslsupp` | `0.0` |  |
  | `sa_esltrig` | `0.0` |  |
  | `sa_exten` | `` |  |
  | `sa_fccost` | `0.0` |  |
  | `sa_fcdec` | `2.0` | Foreign currency field |
  | `sa_fcurr` | `` | Foreign currency field |
  | `sa_fcval` | `0.0` |  |
  | `sa_fcvat` | `0.0` |  |
  | `sa_interbr` | `0.0` |  |
  | `sa_jccode` | `` |  |
  | `sa_jcstdoc` | `` |  |
  | `sa_jline` | `` |  |
  | `sa_job` | `` |  |
  | `sa_jphase` | `` |  |
  | `sa_netmass` | `0.0` |  |
  | `sa_nrthire` | `0.0` |  |
  | `sa_product` | `` |  |
  | `sa_project` | `` |  |
  | `sa_qty` | `0.0` |  |
  | `sa_regctry` | `` |  |
  | `sa_region` | `NE` |  |
  | `sa_regvat` | `` |  |
  | `sa_sentvat` | `0.0` |  |
  | `sa_serv` | `0.0` |  |
  | `sa_setdisc` | `0.0` |  |
  | `sa_ssdfval` | `0.0` |  |
  | `sa_ssdpost` | `0.0` |  |
  | `sa_ssdpre` | `0.0` |  |
  | `sa_ssdsupp` | `0.0` |  |
  | `sa_ssdval` | `0.0` |  |
  | `sa_suppqty` | `0.0` |  |
  | `sa_taxdate` | `2024-05-01T00:00:00` |  |
  | `sa_terr` | `EX1` |  |
  | `sa_transac` | `` |  |
  | `sa_transpt` | `` |  |
  | `sa_trdate` | `2024-05-01T00:00:00` |  |
  | `sa_trref` | `re1` |  |
  | `sa_trtype` | `I` |  |
  | `sa_trvalue` | `100.0` |  |
  | `sa_vatctry` | `H` |  |
  | `sa_vatrate` | `20.0` |  |
  | `sa_vattype` | `S` |  |
  | `sa_vatval` | `20.0` |  |

**sname**
  *1 row(s) modified*

**snoml**
  *2 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `sx_cdesc` | `` |  |
  | `sx_comment` | `Adams Light Engineering Ltd   ref2` |  |
  | `sx_date` | `2024-05-01T00:00:00` |  |
  | `sx_done` | `` |  |
  | `sx_fcdec` | `0.0` |  |
  | `sx_fcmult` | `0.0` |  |
  | `sx_fcrate` | `0.0` |  |
  | `sx_fcurr` | `` | Foreign currency field |
  | `sx_fvalue` | `0.0` |  |
  | `sx_job` | `` |  |
  | `sx_jrnl` | `0.0` | From nparm.np_nexjrnl |
  | `sx_nacnt` | `K110` |  |
  | `sx_ncntr` | `SAL` |  |
  | `sx_nlpdate` | `2024-05-01T00:00:00` |  |
  | `sx_project` | `` |  |
  | `sx_srcco` | `Z` |  |
  | `sx_tref` | `Lease - Accessories           re1` |  |
  | `sx_type` | `I` |  |
  | `sx_unique` | `_7FL0TM90Z` | Base-36 unique ID |
  | `sx_value` | `100.0` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `sx_cdesc` | `` |  |
  | `sx_comment` | `Adams Light Engineering Ltd   ref2` |  |
  | `sx_date` | `2024-05-01T00:00:00` |  |
  | `sx_done` | `` |  |
  | `sx_fcdec` | `0.0` |  |
  | `sx_fcmult` | `0.0` |  |
  | `sx_fcrate` | `0.0` |  |
  | `sx_fcurr` | `` | Foreign currency field |
  | `sx_fvalue` | `0.0` |  |
  | `sx_job` | `` |  |
  | `sx_jrnl` | `0.0` | From nparm.np_nexjrnl |
  | `sx_nacnt` | `E220` |  |
  | `sx_ncntr` | `` |  |
  | `sx_nlpdate` | `2024-05-01T00:00:00` |  |
  | `sx_project` | `` |  |
  | `sx_srcco` | `Z` |  |
  | `sx_tref` | `Lease - Accessories           re1` |  |
  | `sx_type` | `I` |  |
  | `sx_unique` | `_7FL0TM90Z` | Base-36 unique ID |
  | `sx_value` | `20.0` |  |

**stran**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `jxrenewal` | `0.0` |  |
  | `jxservid` | `0.0` |  |
  | `st_account` | `ADA0001` |  |
  | `st_adjsv` | `0.0` |  |
  | `st_advallc` | `0.0` |  |
  | `st_advance` | `N` |  |
  | `st_binrep` | `0.0` |  |
  | `st_cash` | `0.0` |  |
  | `st_cbtype` | `` |  |
  | `st_crdate` | `2024-05-01T00:00:00` |  |
  | `st_custref` | `ref2` |  |
  | `st_delacc` | `ADA0001` |  |
  | `st_dispute` | `0.0` |  |
  | `st_dueday` | `2024-06-15T00:00:00` |  |
  | `st_edi` | `0.0` |  |
  | `st_editx` | `0.0` |  |
  | `st_edivn` | `0.0` |  |
  | `st_entry` | `` |  |
  | `st_eurind` | `` |  |
  | `st_euro` | `0.0` |  |
  | `st_exttime` | `` |  |
  | `st_fadval` | `0.0` |  |
  | `st_fcbal` | `0.0` |  |
  | `st_fcdec` | `0.0` |  |
  | `st_fcmult` | `0.0` |  |
  | `st_fcrate` | `0.0` |  |
  | `st_fcurr` | `` | Foreign currency field |
  | `st_fcval` | `0.0` |  |
  | `st_fcvat` | `0.0` |  |
  | `st_fullamt` | `0.0` |  |
  | `st_fullcb` | `` |  |
  | `st_fullnar` | `` |  |
  | `st_gateid` | `0.0` |  |
  | `st_gatetr` | `0.0` |  |
  | `st_luptime` | `` |  |
  | `st_memo` | `Analysis of Invoice re1                  Dated 01/05/2024...` |  |
  | `st_nlpdate` | `2024-05-01T00:00:00` |  |
  | `st_origcur` | `` |  |
  | `st_paid` | `` |  |
  | `st_payadvl` | `0.0` |  |
  | `st_payflag` | `0.0` |  |
  | `st_rcode` | `` |  |
  | `st_region` | `NE` |  |
  | `st_revchrg` | `0.0` |  |
  | `st_ruser` | `` |  |
  | `st_set1` | `0.0` |  |
  | `st_set1day` | `0.0` |  |
  | `st_set2` | `0.0` |  |
  | `st_set2day` | `0.0` |  |
  | `st_taxpoin` | `2024-05-01T00:00:00` |  |
  | `st_terr` | `EX1` |  |
  | `st_trbal` | `120.0` |  |
  | `st_trdate` | `2024-05-01T00:00:00` |  |
  | `st_trref` | `re1` |  |
  | `st_trtype` | `I` |  |
  | `st_trvalue` | `120.0` |  |
  | `st_txtrep` | `` |  |
  | `st_type` | `CPT` |  |
  | `st_unique` | `_7FL0TM90Z` | Base-36 unique ID |
  | `st_vatval` | `20.0` |  |

  *1630 row(s) modified*

**zcontacts**
  *2 row(s) modified*

---

### Sales receipt without  allocation

*Purchase invoice posting. Creates: ptran, pnoml, ntran, nacnt, pname balance.*

**aentry**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `ae_acnt` | `C310` |  |
  | `ae_batchid` | `0.0` |  |
  | `ae_brwptr` | `` |  |
  | `ae_cbtype` | `R2` |  |
  | `ae_cntr` | `` |  |
  | `ae_comment` | `` |  |
  | `ae_complet` | `1.0` |  |
  | `ae_entref` | `test` |  |
  | `ae_entry` | `R200000719` | From atype counter |
  | `ae_frstat` | `0.0` |  |
  | `ae_lstdate` | `2024-05-01T00:00:00` |  |
  | `ae_payid` | `0.0` |  |
  | `ae_postgrp` | `0.0` |  |
  | `ae_recbal` | `0.0` |  |
  | `ae_reclnum` | `0.0` |  |
  | `ae_remove` | `0.0` |  |
  | `ae_statln` | `0.0` |  |
  | `ae_tmpstat` | `0.0` |  |
  | `ae_tostat` | `0.0` |  |
  | `ae_value` | `50000.0` |  |
  | `sq_amtime` | `` |  |
  | `sq_amuser` | `` |  |
  | `sq_crdate` | `2026-04-01T00:00:00` |  |
  | `sq_crtime` | `14:43:14` |  |
  | `sq_cruser` | `TEST` |  |

**anoml**
  *2 row(s) added*

  Row 1:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ax_comment` | `Adams Light Engineering Ltd   BACS` |  |
  | `ax_date` | `2024-05-01T00:00:00` |  |
  | `ax_done` | `Y` | NL transfer complete |
  | `ax_fcdec` | `0.0` |  |
  | `ax_fcmult` | `0.0` |  |
  | `ax_fcrate` | `0.0` |  |
  | `ax_fcurr` | `` | Foreign currency field |
  | `ax_fvalue` | `0.0` |  |
  | `ax_job` | `` |  |
  | `ax_jrnl` | `3451.0` | From nparm.np_nexjrnl |
  | `ax_nacnt` | `C310` |  |
  | `ax_ncntr` | `` |  |
  | `ax_nlpdate` | `2024-05-01T00:00:00` |  |
  | `ax_project` | `` |  |
  | `ax_source` | `S` |  |
  | `ax_srcco` | `Z` |  |
  | `ax_tref` | `test` |  |
  | `ax_unique` | `_7FL0VJUWZ` | Base-36 unique ID |
  | `ax_value` | `500.0` |  |

  Row 2:
  | Field | Value | Notes |
  |-------|-------|-------|
  | `ax_comment` | `Adams Light Engineering Ltd   BACS` |  |
  | `ax_date` | `2024-05-01T00:00:00` |  |
  | `ax_done` | `Y` | NL transfer complete |
  | `ax_fcdec` | `0.0` |  |
  | `ax_fcmult` | `0.0` |  |
  | `ax_fcrate` | `0.0` |  |
  | `ax_fcurr` | `` | Foreign currency field |
  | `ax_fvalue` | `0.0` |  |
  | `ax_job` | `` |  |
  | `ax_jrnl` | `3451.0` | From nparm.np_nexjrnl |
  | `ax_nacnt` | `C110` |  |
  | `ax_ncntr` | `` |  |
  | `ax_nlpdate` | `2024-05-01T00:00:00` |  |
  | `ax_project` | `` |  |
  | `ax_source` | `S` |  |
  | `ax_srcco` | `Z` |  |
  | `ax_tref` | `test` |  |
  | `ax_unique` | `_7FL0VJUWZ` | Base-36 unique ID |
  | `ax_value` | `-500.0` |  |

**atran**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `at_account` | `ADA0001` |  |
  | `at_acnt` | `C310` |  |
  | `at_atpycd` | `` |  |
  | `at_bacprn` | `0.0` |  |
  | `at_bic` | `` |  |
  | `at_bsname` | `` |  |
  | `at_bsref` | `` |  |
  | `at_cash` | `0.0` |  |
  | `at_cbtype` | `R2` |  |
  | `at_ccauth` | `0` |  |
  | `at_ccdno` | `` |  |
  | `at_ccdprn` | `0.0` |  |
  | `at_chqlst` | `0.0` |  |
  | `at_chqprn` | `0.0` |  |
  | `at_cntr` | `` |  |
  | `at_comment` | `` |  |
  | `at_disc` | `0.0` |  |
  | `at_ecb` | `0.0` |  |
  | `at_ecbtype` | `` |  |
  | `at_entry` | `R200000719` | From atype counter |
  | `at_fcdec` | `2.0` | Foreign currency field |
  | `at_fcexch` | `1.0` | Foreign currency field |
  | `at_fcmult` | `0.0` |  |
  | `at_fcurr` | `` | Foreign currency field |
  | `at_iban` | `` |  |
  | `at_inputby` | `TEST` |  |
  | `at_job` | `` |  |
  | `at_memo` | `` |  |
  | `at_name` | `Adams Light Engineering Ltd` |  |
  | `at_number` | `` |  |
  | `at_payee` | `` |  |
  | `at_payname` | `` |  |
  | `at_payslp` | `0.0` |  |
  | `at_postgrp` | `0.0` |  |
  | `at_project` | `` |  |
  | `at_pstdate` | `2024-05-01T00:00:00` |  |
  | `at_pysprn` | `0.0` |  |
  | `at_refer` | `test` |  |
  | `at_remit` | `0.0` |  |
  | `at_remove` | `0.0` |  |
  | `at_sort` | `` |  |
  | `at_srcco` | `Z` |  |
  | `at_sysdate` | `2024-05-01T00:00:00` |  |
  | `at_tperiod` | `1.0` | From nominal calendar |
  | `at_type` | `4.0` |  |
  | `at_unique` | `_7FL0VJUWZ` | Base-36 unique ID |
  | `at_value` | `50000.0` |  |
  | `at_vattycd` | `` |  |

**atype**
  *1 row(s) modified*

**dmcomp**
  *1 row(s) modified*

**dmcont**
  *5 row(s) modified*

**idtab**
  *1 row(s) modified*

**nacnt**
  *2 row(s) modified*

**nbank**
  *1 row(s) modified*

**ndetail**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `nt_acnt` | `C310` |  |
  | `nt_cdesc` | `` |  |
  | `nt_cmnt` | `test` |  |
  | `nt_cntr` | `` |  |
  | `nt_consol` | `0.0` |  |
  | `nt_distrib` | `0.0` |  |
  | `nt_entr` | `2024-05-01T00:00:00` |  |
  | `nt_fcdec` | `0.0` |  |
  | `nt_fcmult` | `0.0` |  |
  | `nt_fcrate` | `0.0` |  |
  | `nt_fcurr` | `` | Foreign currency field |
  | `nt_fvalue` | `0.0` |  |
  | `nt_inp` | `TEST` |  |
  | `nt_job` | `` |  |
  | `nt_jrnl` | `3451.0` | From nparm.np_nexjrnl |
  | `nt_period` | `5.0` | From nominal calendar |
  | `nt_perpost` | `0.0` |  |
  | `nt_posttyp` | `S` |  |
  | `nt_prevyr` | `0.0` |  |
  | `nt_project` | `` |  |
  | `nt_pstgrp` | `1.0` |  |
  | `nt_pstid` | `_7FL0VJYCQ` |  |
  | `nt_recjrnl` | `0.0` | From nparm.np_nexjrnl |
  | `nt_rectify` | `0.0` |  |
  | `nt_recurr` | `0.0` |  |
  | `nt_ref` | `` |  |
  | `nt_rvrse` | `0.0` |  |
  | `nt_srcco` | `Z` |  |
  | `nt_subt` | `03` |  |
  | `nt_trnref` | `Adams Light Engineering Ltd   BACS` |  |
  | `nt_trtype` | `A` |  |
  | `nt_type` | `10` |  |
  | `nt_value` | `500.0` |  |
  | `nt_vatanal` | `0.0` |  |
  | `nt_year` | `2024.0` |  |

**nextid**
  *7 row(s) modified*

**nhist**
  *2 row(s) modified*

**njmemo**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `nj_binrep` | `0.0` |  |
  | `nj_image` | `` |  |
  | `nj_journal` | `3451.0` | From nparm.np_nexjrnl |
  | `nj_memo` | `ÿ<<JOURNAL_DATA_ONLY>>ÿ` |  |
  | `nj_txtrep` | `Cashbook Ledger Transfer` |  |

**nsubt**
  *2 row(s) modified*

**ntype**
  *1 row(s) modified*

**sname**
  *1 row(s) modified*

**stran**
  *1 row(s) added*

  | Field | Value | Notes |
  |-------|-------|-------|
  | `jxrenewal` | `0.0` |  |
  | `jxservid` | `0.0` |  |
  | `st_account` | `ADA0001` |  |
  | `st_adjsv` | `0.0` |  |
  | `st_advallc` | `0.0` |  |
  | `st_advance` | `N` |  |
  | `st_binrep` | `0.0` |  |
  | `st_cash` | `0.0` |  |
  | `st_cbtype` | `R2` |  |
  | `st_crdate` | `2024-05-01T00:00:00` |  |
  | `st_custref` | `BACS` |  |
  | `st_delacc` | `ADA0001` |  |
  | `st_dispute` | `0.0` |  |
  | `st_edi` | `0.0` |  |
  | `st_editx` | `0.0` |  |
  | `st_edivn` | `0.0` |  |
  | `st_entry` | `R200000719` | From atype counter |
  | `st_eurind` | `` |  |
  | `st_euro` | `0.0` |  |
  | `st_exttime` | `` |  |
  | `st_fadval` | `0.0` |  |
  | `st_fcbal` | `0.0` |  |
  | `st_fcdec` | `0.0` |  |
  | `st_fcmult` | `0.0` |  |
  | `st_fcrate` | `0.0` |  |
  | `st_fcurr` | `` | Foreign currency field |
  | `st_fcval` | `0.0` |  |
  | `st_fcvat` | `0.0` |  |
  | `st_fullamt` | `0.0` |  |
  | `st_fullcb` | `` |  |
  | `st_fullnar` | `` |  |
  | `st_gateid` | `0.0` |  |
  | `st_gatetr` | `0.0` |  |
  | `st_luptime` | `` |  |
  | `st_memo` | `` |  |
  | `st_nlpdate` | `2024-05-01T00:00:00` |  |
  | `st_origcur` | `` |  |
  | `st_paid` | `` |  |
  | `st_payadvl` | `0.0` |  |
  | `st_payflag` | `0.0` |  |
  | `st_rcode` | `` |  |
  | `st_region` | `` |  |
  | `st_revchrg` | `0.0` |  |
  | `st_ruser` | `` |  |
  | `st_set1` | `0.0` |  |
  | `st_set1day` | `0.0` |  |
  | `st_set2` | `0.0` |  |
  | `st_set2day` | `0.0` |  |
  | `st_terr` | `` |  |
  | `st_trbal` | `-500.0` |  |
  | `st_trdate` | `2024-05-01T00:00:00` |  |
  | `st_trref` | `test` |  |
  | `st_trtype` | `R` |  |
  | `st_trvalue` | `-500.0` |  |
  | `st_txtrep` | `` |  |
  | `st_type` | `` |  |
  | `st_unique` | `_7FL0VJUWZ` | Base-36 unique ID |
  | `st_vatval` | `0.0` |  |

**zlock**
  *1 row(s) modified*

---

## System Configuration

### Turn Real Tine on

*Payment to nominal account (no ledger). Creates: aentry, atran, ntran, anoml, nacnt, nbank.*

**seqco**
  *6 row(s) modified*

**seqsys**
  *1 row(s) modified*

**sequser**
  *1 row(s) modified*

**cparm**
  *1 row(s) modified*

**fparm**
  *1 row(s) modified*

**pparm**
  *1 row(s) modified*

**sparm**
  *1 row(s) modified*

---
