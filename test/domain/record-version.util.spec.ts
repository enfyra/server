import { describe, expect, it } from 'vitest';
import {
  isVersionedTable,
  normalizeCreateVersion,
  normalizeUpdateVersion,
  VERSIONED_TABLE_NAMES,
} from '../../src/shared/utils/record-version.util';

describe('isVersionedTable', () => {
  it('covers every record whose source can be lost', () => {
    expect([...VERSIONED_TABLE_NAMES].sort()).toEqual(
      [
        'enfyra_extension',
        'enfyra_flow_step',
        'enfyra_helper',
        'enfyra_oauth_config',
        'enfyra_post_hook',
        'enfyra_pre_hook',
        'enfyra_route_handler',
        'enfyra_websocket',
        'enfyra_websocket_event',
      ].sort(),
    );
  });

  it('does not treat a plain metadata table as versioned', () => {
    expect(isVersionedTable('enfyra_menu')).toBe(false);
    expect(isVersionedTable('enfyra_table')).toBe(false);
  });
});

describe('normalizeCreateVersion', () => {
  it('lets the column default supply the first version when omitted', () => {
    expect(normalizeCreateVersion(undefined)).toBeUndefined();
    expect(normalizeCreateVersion(null)).toBeUndefined();
    expect(normalizeCreateVersion('')).toBeUndefined();
  });

  it('keeps a supplied version', () => {
    expect(normalizeCreateVersion('2.0.0')).toBe('2.0.0');
    expect(normalizeCreateVersion('1.0.0')).toBe('1.0.0');
  });

  it('rejects a malformed version', () => {
    expect(() => normalizeCreateVersion('v1')).toThrow(/valid version/);
    expect(() => normalizeCreateVersion('1')).toThrow(/valid version/);
    expect(() => normalizeCreateVersion(2)).toThrow(/valid version/);
  });
});

describe('normalizeUpdateVersion', () => {
  it('requires a version on update', () => {
    expect(() => normalizeUpdateVersion(undefined, '1.0.0')).toThrow(
      /version is required/,
    );
    expect(() => normalizeUpdateVersion('', '1.0.0')).toThrow(
      /version is required/,
    );
  });

  it('accepts a strictly greater version', () => {
    expect(normalizeUpdateVersion('1.0.1', '1.0.0')).toBe('1.0.1');
    expect(normalizeUpdateVersion('2.0.0', '1.0.0')).toBe('2.0.0');
    expect(normalizeUpdateVersion('3.0.2', '3.0.1')).toBe('3.0.2');
  });

  it('rejects an equal version', () => {
    expect(() => normalizeUpdateVersion('1.0.0', '1.0.0')).toThrow(
      /greater than the current version/,
    );
  });

  it('rejects a lower version', () => {
    expect(() => normalizeUpdateVersion('1.0.0', '2.0.0')).toThrow(
      /greater than the current version/,
    );
    expect(() => normalizeUpdateVersion('2.2.9', '2.2.10')).toThrow(
      /greater than the current version/,
    );
  });

  it('compares numeric segments as numbers', () => {
    expect(normalizeUpdateVersion('2.2.10', '2.2.9')).toBe('2.2.10');
  });

  it('rejects a malformed version', () => {
    expect(() => normalizeUpdateVersion('abc', '1.0.0')).toThrow(
      /valid version/,
    );
  });

  it('normalizes a leading v prefix', () => {
    expect(normalizeUpdateVersion('v1.0.1', '1.0.0')).toBe('1.0.1');
  });
});
