import { describe, it, expect } from 'vitest';
import {
  generateOperaUniqueId,
  generateOperaUniqueIds,
} from '../src/opera/unique-id.js';

describe('generateOperaUniqueId', () => {
  it('matches Opera format: underscore + 9 base-36 characters', () => {
    for (let i = 0; i < 50; i++) {
      const id = generateOperaUniqueId();
      expect(id).toMatch(/^_[0-9A-Z]{9}$/);
    }
  });

  it('returns unique values across many calls (within same millisecond)', () => {
    const seen = new Set<string>();
    for (let i = 0; i < 200; i++) {
      seen.add(generateOperaUniqueId());
    }
    expect(seen.size).toBe(200);
  });

  it('IDs are alphabetically increasing within the same millisecond', () => {
    // Same-ms IDs differ only by the trailing sequence; the timestamp
    // portion is identical so the result is monotonic.
    const a = generateOperaUniqueId();
    const b = generateOperaUniqueId();
    expect(b > a).toBe(true);
  });
});

describe('generateOperaUniqueIds', () => {
  it('returns the requested count', () => {
    expect(generateOperaUniqueIds(0)).toEqual([]);
    expect(generateOperaUniqueIds(1).length).toBe(1);
    expect(generateOperaUniqueIds(50).length).toBe(50);
  });

  it('all returned IDs are distinct', () => {
    const ids = generateOperaUniqueIds(100);
    expect(new Set(ids).size).toBe(100);
  });

  it('all returned IDs match the format', () => {
    for (const id of generateOperaUniqueIds(20)) {
      expect(id).toMatch(/^_[0-9A-Z]{9}$/);
    }
  });
});
