import { expect, test, describe } from 'bun:test';
import { z } from 'zod';
import { Tinybird, defineSchema, defineDataSource, defineIngest, string, int64, createZodSchemaFromParameters, stringParam, int64Param } from '../src';

describe('Tinybird Client', () => {
  const testSchema = defineSchema({
    id: string('id'),
    name: string('name'),
    count: int64('count'),
  });

  const testDataSource = defineDataSource({
    name: 'test_table',
    schema: testSchema,
    engine: 'MergeTree',
  });

  test('should create client with noop mode', () => {
    const client = new Tinybird({
      noop: true,
      datasources: {
        test: testDataSource,
      },
    });

    expect(client).toBeInstanceOf(Tinybird);
  });

  test('should create client with token', () => {
    const client = new Tinybird({
      token: 'test-token',
      datasources: {
        test: testDataSource,
      },
    });

    expect(client).toBeInstanceOf(Tinybird);
  });

  test('should create query builder from datasource', () => {
    const client = new Tinybird({
      noop: true,
      datasources: {
        test: testDataSource,
      },
    });

    const qb = client.from('test');
    expect(qb).toBeDefined();
    expect(typeof qb.select).toBe('function');
    expect(typeof qb.from).toBe('function');
  });

  test('should throw error for unknown datasource', () => {
    const client = new Tinybird({
      noop: true,
      datasources: {
        test: testDataSource,
      },
    });

    expect(() => client.from('unknown' as any)).toThrow('Unknown datasource: unknown');
  });

  test('should create pipe function with type safety', () => {
    const client = new Tinybird({
      noop: true,
    });

    const pipeFunction = client.pipe({
      pipe: 'test_pipe',
      parameters: z.object({
        tenantId: z.string(),
        limit: z.number().optional(),
      }),
      data: z.object({
        id: z.string(),
        name: z.string(),
      }),
    });

    expect(typeof pipeFunction).toBe('function');
  });

  test('should handle noop mode for pipe execution', async () => {
    const client = new Tinybird({
      noop: true,
    });

    const pipeFunction = client.pipe({
      pipe: 'test_pipe',
      parameters: z.object({
        tenantId: z.string(),
      }),
      data: z.object({
        id: z.string(),
        name: z.string(),
      }),
    });

    const result = await pipeFunction({
      tenantId: 'test-tenant',
    });

    expect(result.data).toEqual([]);
    expect(result.meta).toEqual([]);
  });

  test('should validate parameters correctly', () => {
    const client = new Tinybird({
      noop: true,
    });

    const pipeFunction = client.pipe({
      pipe: 'test_pipe',
      parameters: z.object({
        tenantId: z.string(),
        limit: z.number().min(1),
      }),
      data: z.object({
        id: z.string(),
      }),
    });

    // This should not throw in noop mode, but validates the parameters
    expect(() => 
      pipeFunction({
        tenantId: 'test-tenant',
        limit: 10,
      })
    ).not.toThrow();
  });

  test('should create Zod schema from TinyKit parameters', () => {
    const parameters = {
      tenantId: stringParam('tenantId', { required: true }),
      limit: int64Param('limit', { default: 100 }),
    };

    const zodSchema = createZodSchemaFromParameters(parameters);
    
    // Should parse valid data
    const validData = zodSchema.parse({
      tenantId: 'test-tenant',
      limit: 50,
    });
    expect(validData.tenantId).toBe('test-tenant');
    expect(validData.limit).toBe(50);

    // Should use default value
    const defaultData = zodSchema.parse({
      tenantId: 'test-tenant',
    });
    expect(defaultData.tenantId).toBe('test-tenant');
    expect(defaultData.limit).toBe(100);

    // Should throw for missing required parameter
    expect(() => zodSchema.parse({})).toThrow();
  });

  test('should handle ingest with validation results', async () => {
    const client = new Tinybird({
      noop: true,
    });

    const ingestDef = defineIngest({
      datasource: 'test_table',
      schema: defineSchema({
        id: string('id'),
        name: string('name'),
      }),
    });

    const ingestFunction = client.ingest(ingestDef);

    const result = await ingestFunction([
      { id: '1', name: 'test1' },
      { id: '2', name: 'test2' },
    ]);

    expect(result.successful_rows).toBe(2);
    expect(result.quarantined_rows).toBe(0);
  });
});