import { describe, it, expect } from 'vitest';
import {
  SqlInputValidationError,
  validateBankCode,
  validateAccountCode,
  validateEntryNumber,
  validateCbtype,
  validatePaymentRef,
  validateReference,
  validateBatchNumber,
} from '../src/opera/sql-input-validators.js';

describe('validateBankCode', () => {
  it('accepts alphanumeric + underscore + dash up to 12 chars', () => {
    expect(validateBankCode('BC010')).toBe('BC010');
    expect(validateBankCode('BANK_01')).toBe('BANK_01');
    expect(validateBankCode('A-B-C-1')).toBe('A-B-C-1');
    expect(validateBankCode('123456789012')).toBe('123456789012');
  });

  it('rejects empty / null / undefined', () => {
    expect(() => validateBankCode('')).toThrow(/required/);
    expect(() => validateBankCode(null)).toThrow(/required/);
    expect(() => validateBankCode(undefined)).toThrow(/required/);
  });

  it('rejects > 12 chars', () => {
    expect(() => validateBankCode('1234567890123')).toThrow(/max 12 chars/);
  });

  it("rejects SQL-injection payloads", () => {
    expect(() => validateBankCode("BC010';--")).toThrow(/not a valid/);
    expect(() => validateBankCode('BC; DROP')).toThrow(/not a valid/);
    expect(() => validateBankCode('BC--')).toThrow();
  });

  it('throws SqlInputValidationError class', () => {
    let err: unknown = null;
    try {
      validateBankCode('');
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(SqlInputValidationError);
    expect((err as SqlInputValidationError).statusCode).toBe(400);
  });
});

describe('validateAccountCode', () => {
  it('accepts alphanumeric + ./-/_ up to 16 chars', () => {
    expect(validateAccountCode('CUST001')).toBe('CUST001');
    expect(validateAccountCode('A.B-C/D_E')).toBe('A.B-C/D_E');
  });
  it('rejects > 16 chars', () => {
    expect(() => validateAccountCode('12345678901234567')).toThrow();
  });
  it('rejects spaces (account codes have no spaces)', () => {
    expect(() => validateAccountCode('CUST 001')).toThrow();
  });
});

describe('validateEntryNumber', () => {
  it('accepts up to 20 chars', () => {
    expect(validateEntryNumber('P100008036')).toBe('P100008036');
    expect(validateEntryNumber('PR00000534')).toBe('PR00000534');
  });
  it('rejects empty', () => {
    expect(() => validateEntryNumber('')).toThrow(/required/);
  });
  it('rejects forbidden tokens', () => {
    expect(() => validateEntryNumber("P1';DROP--")).toThrow();
  });
});

describe('validateCbtype', () => {
  it('accepts up to 4 alphanumeric chars and uppercases', () => {
    expect(validateCbtype('GC')).toBe('GC');
    expect(validateCbtype('gc01')).toBe('GC01');
  });
  it('returns empty when input is falsy', () => {
    expect(validateCbtype('')).toBe('');
    expect(validateCbtype(undefined)).toBe('');
  });
  it('rejects > 4 chars', () => {
    expect(() => validateCbtype('TOOLONG')).toThrow();
  });
});

describe('validatePaymentRef', () => {
  it('accepts alphanumeric + space + _./- up to 30 chars', () => {
    expect(validatePaymentRef('INV-0001')).toBe('INV-0001');
    expect(validatePaymentRef('Some payment 123')).toBe('Some payment 123');
  });
  it('rejects empty', () => {
    expect(() => validatePaymentRef('')).toThrow(/required/);
  });
});

describe('validateReference', () => {
  it('accepts empty / null / undefined as ""', () => {
    expect(validateReference(null)).toBe('');
    expect(validateReference(undefined)).toBe('');
    expect(validateReference('')).toBe('');
  });
  it('accepts ":" "&" "#" "," (free-form)', () => {
    expect(validateReference('Ref: 1,2,3 #A&B')).toBe('Ref: 1,2,3 #A&B');
  });
  it('rejects forbidden tokens', () => {
    expect(() => validateReference("R';--")).toThrow();
  });
});

describe('validateBatchNumber', () => {
  it('accepts digit-only strings up to 9 chars', () => {
    expect(validateBatchNumber('12345')).toBe(12345);
    expect(validateBatchNumber(7)).toBe(7);
  });
  it('rejects > 9 digits', () => {
    expect(() => validateBatchNumber('1234567890')).toThrow();
  });
  it('rejects non-digits', () => {
    expect(() => validateBatchNumber('1a2b')).toThrow();
  });
});
