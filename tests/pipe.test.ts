import { expect, test, describe } from 'bun:test';

import {
  defineSchema,
  definePipe,
  defineParameters,
  stringParam,
  int64Param,
  enumParam,
  query,
  string,
  int64,
  count,
} from '../src';

describe('Pipe Definition', () => {
  const testSchema = defineSchema({
    id: string('id', { jsonPath: '$.id' }),
    tenantId: string('tenantId', { jsonPath: '$.tenantId' }),
    time: int64('time', { jsonPath: '$.time' }),
    event: string('event', { jsonPath: '$.event' }),
  });

  test('should create parameter definitions with correct types', () => {
    const tenantParam = stringParam('tenantId', { required: true });
    const limitParam = int64Param('limit', { default: 100 });
    const statusParam = enumParam('status', ['active', 'inactive'] as const, {
      default: 'active',
    });

    expect(tenantParam.name).toBe('tenantId');
    expect(tenantParam.type).toBe('String');
    expect(tenantParam.required).toBe(true);

    expect(limitParam.name).toBe('limit');
    expect(limitParam.type).toBe('Int64');
    expect(limitParam.default).toBe(100);

    expect(statusParam.name).toBe('status');
    expect(statusParam.type).toBe('String');
    expect(statusParam.default).toBe('active');
  });

  test('should create pipe with proper configuration', () => {
    const parameters = defineParameters({
      tenantId: stringParam('tenantId', { required: true }),
      limit: int64Param('limit', { default: 100 }),
    });

    const pipe = definePipe({
      name: 'get_events__v1',
      version: 1,
      schema: testSchema,
      parameters,
    }).endpoint((q, params) =>
      query(testSchema)
        .select('id', 'event')
        .selectRaw(`${count()} AS count`)
        .from('events__v1')
        .where(`tenantId = {{ String(tenantId, required=True) }}`)
        .limit(params.limit ?? 100)
    );

    expect(pipe.name).toBe('get_events__v1');
    expect(pipe.version).toBe(1);
    expect(pipe.parameters).toBe(parameters);
  });

  test('should generate valid pipe SQL', () => {
    const parameters = defineParameters({
      tenantId: stringParam('tenantId', { required: true }),
      start: int64Param('start', { required: true }),
      limit: int64Param('limit', { default: 100 }),
    });

    const pipe = definePipe({
      name: 'get_events__v1',
      version: 1,
      schema: testSchema,
      parameters,
    }).endpoint((q, params) =>
      query(testSchema)
        .select('id', 'event')
        .from('events__v1')
        .where(`tenantId = {{ String(tenantId, required=True) }}`)
        .and(`time >= {{ Int64(start, required=True) }}`)
        .limit(params.limit ?? 100)
    );

    const sql = pipe.sql({
      tenantId: 'tenant-123',
      start: 1234567890,
      limit: 50,
    });

    expect(sql).toContain('VERSION 1');
    expect(sql).toContain('NODE endpoint');
    expect(sql).toContain('SQL >');
    expect(sql).toContain('SELECT id, event');
    expect(sql).toContain('FROM events__v1');
    expect(sql).toContain(
      'WHERE tenantId = {{ String(tenantId, required=True) }}'
    );
    expect(sql).toContain('AND time >= {{ Int64(start, required=True) }}');
    expect(sql).toContain('LIMIT 50');
  });

  test('should handle enum parameters with proper type inference', () => {
    const granularityParam = enumParam(
      'granularity',
      ['1h', '1d', '1w'] as const,
      { default: '1h' }
    );

    expect(granularityParam.schema.parse('1h')).toBe('1h');
    expect(granularityParam.schema.parse('1d')).toBe('1d');
    expect(granularityParam.schema.parse('1w')).toBe('1w');

    // Should throw for invalid values
    expect(() => granularityParam.schema.parse('invalid')).toThrow();
  });

  test('should validate parameter requirements', () => {
    const requiredParam = stringParam('required', { required: true });
    const optionalParam = stringParam('optional');

    expect(requiredParam.required).toBe(true);
    expect(optionalParam.required).toBeUndefined();
  });
});
