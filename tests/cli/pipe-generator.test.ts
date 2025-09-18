import { expect, test, describe } from 'bun:test';
import { generatePipeFile } from '../../src/cli/generators/pipe';
import { stringParam, int64Param, float64Param, booleanParam, dateTimeParam, enumParam } from '../../src/pipe';
import type { PipeConfig, QueryParameters } from '../../src/types';

// Mock pipe configurations for testing
function createMockPipe<T extends QueryParameters>(
  name: string,
  parameters: T,
  sqlGenerator: (params: any) => string
): PipeConfig<T> {
  return {
    name,
    version: 1,
    parameters,
    sql: sqlGenerator
  };
}

describe('Pipe Generator', () => {
  test('should generate pipe with String parameters', () => {
    const pipe = createMockPipe(
      'test_string_pipe__v1',
      {
        tenantId: stringParam('tenantId', { required: true }),
        status: stringParam('status', { default: 'active' }),
        optional: stringParam('optional')
      },
      (params) => `SELECT * FROM events WHERE tenantId = ${params.tenantId} AND status = ${params.status} AND optional = ${params.optional}`
    );

    const result = generatePipeFile(pipe);

    expect(result).toContain('VERSION 1');
    expect(result).toContain('NODE endpoint');
    expect(result).toContain('SQL >');
    expect(result).toContain('{{ String(tenantId, required=True) }}');
    expect(result).toContain("{{ String(status, 'active') }}");
    expect(result).toContain('{{ String(optional) }}');
  });

  test('should generate pipe with Int64 parameters', () => {
    const pipe = createMockPipe(
      'test_int_pipe__v1',
      {
        userId: int64Param('userId', { required: true }),
        limit: int64Param('limit', { default: 100 }),
        offset: int64Param('offset')
      },
      (params) => `SELECT * FROM users WHERE id = ${params.userId} LIMIT ${params.limit} OFFSET ${params.offset}`
    );

    const result = generatePipeFile(pipe);

    expect(result).toContain('{{ Int64(userId, required=True) }}');
    expect(result).toContain('{{ Int64(limit, 100) }}');
    expect(result).toContain('{{ Int64(offset) }}');
  });

  test('should generate pipe with Float64 parameters', () => {
    const pipe = createMockPipe(
      'test_float_pipe__v1',
      {
        threshold: float64Param('threshold', { required: true }),
        multiplier: float64Param('multiplier', { default: 1.5 })
      },
      (params) => `SELECT * FROM metrics WHERE value > ${params.threshold} * ${params.multiplier}`
    );

    const result = generatePipeFile(pipe);

    expect(result).toContain('{{ Float64(threshold, required=True) }}');
    expect(result).toContain('{{ Float64(multiplier, 1.5) }}');
  });

  test('should generate pipe with Boolean parameters', () => {
    const pipe = createMockPipe(
      'test_bool_pipe__v1',
      {
        isActive: booleanParam('isActive', { required: true }),
        includeDeleted: booleanParam('includeDeleted', { default: false })
      },
      (params) => `SELECT * FROM records WHERE active = ${params.isActive} AND include_deleted = ${params.includeDeleted}`
    );

    const result = generatePipeFile(pipe);

    expect(result).toContain('{{ Boolean(isActive, required=True) }}');
    expect(result).toContain('{{ Boolean(includeDeleted, false) }}');
  });

  test('should generate pipe with DateTime parameters', () => {
    const pipe = createMockPipe(
      'test_datetime_pipe__v1',
      {
        startTime: dateTimeParam('startTime', { required: true }),
        endTime: dateTimeParam('endTime', { default: '2024-01-01T00:00:00Z' })
      },
      (params) => `SELECT * FROM events WHERE timestamp >= ${params.startTime} AND timestamp <= ${params.endTime}`
    );

    const result = generatePipeFile(pipe);

    expect(result).toContain('{{ DateTime(startTime, required=True) }}');
    expect(result).toContain("{{ DateTime(endTime, '2024-01-01T00:00:00Z') }}");
  });

  test('should generate pipe with enum parameters', () => {
    const pipe = createMockPipe(
      'test_enum_pipe__v1',
      {
        granularity: enumParam('granularity', ['1h', '1d', '1w'] as const, { default: '1h' }),
        eventType: enumParam('eventType', ['click', 'view', 'purchase'] as const, { required: true })
      },
      (params) => `SELECT * FROM events WHERE type = ${params.eventType} AND granularity = ${params.granularity}`
    );

    const result = generatePipeFile(pipe);

    expect(result).toContain("{{ String(granularity, '1h') }}");
    expect(result).toContain('{{ String(eventType, required=True) }}');
  });

  test('should generate pipe with mixed parameter types', () => {
    const pipe = createMockPipe(
      'test_mixed_pipe__v1',
      {
        tenantId: stringParam('tenantId', { required: true }),
        startTime: int64Param('startTime', { required: true }),
        limit: int64Param('limit', { default: 1000 }),
        threshold: float64Param('threshold', { default: 0.5 }),
        isActive: booleanParam('isActive', { default: true }),
        status: enumParam('status', ['pending', 'completed'] as const)
      },
      (params) => `
        SELECT *
        FROM analytics
        WHERE tenant_id = ${params.tenantId}
          AND timestamp >= ${params.startTime}
          AND score > ${params.threshold}
          AND active = ${params.isActive}
          AND status = ${params.status}
        LIMIT ${params.limit}
      `
    );

    const result = generatePipeFile(pipe);

    expect(result).toContain('{{ String(tenantId, required=True) }}');
    expect(result).toContain('{{ Int64(startTime, required=True) }}');
    expect(result).toContain('{{ Int64(limit, 1000) }}');
    expect(result).toContain('{{ Float64(threshold, 0.5) }}');
    expect(result).toContain('{{ Boolean(isActive, true) }}');
    expect(result).toContain('{{ String(status) }}');
  });

  test('should generate pipe without version', () => {
    const pipe: PipeConfig<any> = {
      name: 'test_no_version_pipe',
      parameters: {
        id: stringParam('id', { required: true })
      },
      sql: (params) => `SELECT * FROM table WHERE id = ${params.id}`
    };

    const result = generatePipeFile(pipe);

    expect(result).not.toContain('VERSION');
    expect(result).toContain('NODE endpoint');
    expect(result).toContain('{{ String(id, required=True) }}');
  });

  test('should handle complex SQL with multiple parameter references', () => {
    const pipe = createMockPipe(
      'test_complex_sql__v1',
      {
        tenantId: stringParam('tenantId', { required: true }),
        limit: int64Param('limit', { default: 100 })
      },
      (params) => `
        WITH filtered_events AS (
          SELECT *
          FROM events
          WHERE tenant_id = ${params.tenantId}
        )
        SELECT 
          tenant_id,
          count() as event_count
        FROM filtered_events
        WHERE tenant_id = ${params.tenantId}
        GROUP BY tenant_id
        LIMIT ${params.limit}
      `
    );

    const result = generatePipeFile(pipe);

    // Should replace all instances of the parameter
    const templateOccurrences = (result.match(/\{\{ String\(tenantId, required=True\) \}\}/g) || []).length;
    expect(templateOccurrences).toBe(2); // Two occurrences in the SQL
    expect(result).toContain('{{ Int64(limit, 100) }}');
  });

  test('should handle parameters with null defaults', () => {
    const pipe = createMockPipe(
      'test_null_defaults__v1',
      {
        optionalString: stringParam('optionalString', { default: null as any }),
        optionalInt: int64Param('optionalInt', { default: null as any })
      },
      (params) => `SELECT * FROM table WHERE col1 = ${params.optionalString} AND col2 = ${params.optionalInt}`
    );

    const result = generatePipeFile(pipe);

    expect(result).toContain("{{ String(optionalString, null) }}");
    expect(result).toContain('{{ Int64(optionalInt, null) }}');
  });
});