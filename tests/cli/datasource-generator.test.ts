import { expect, test, describe } from 'bun:test';
import { z } from 'zod';
import {
  generateDatasourceFile,
  extractDatasourceName,
} from '../../src/cli/generators/datasource';
import {
  defineDataSource,
  defineSchema,
  string,
  int64,
  boolean,
  dateTime,
  uuid,
  array,
} from '../../src/schema';

describe('Datasource Generator', () => {
  test('should generate basic datasource file', () => {
    const schema = defineSchema({
      id: string('id', { jsonPath: '$.id' }),
      name: string('name', { jsonPath: '$.name' }),
      timestamp: int64('timestamp', { jsonPath: '$.timestamp' }),
    });

    const dataSource = defineDataSource({
      name: 'users__v1',
      version: 1,
      schema,
      engine: 'MergeTree',
      sortingKey: ['timestamp', 'id'],
    });

    const result = generateDatasourceFile(dataSource);

    expect(result).toContain('VERSION 1');
    expect(result).toContain('SCHEMA >');
    expect(result).toContain('`id` String `json:$.id`');
    expect(result).toContain('`name` String `json:$.name`');
    expect(result).toContain('`timestamp` Int64 `json:$.timestamp`');
    expect(result).toContain('ENGINE "MergeTree"');
    expect(result).toContain('ENGINE_SORTING_KEY "timestamp,id"');
  });

  test('should generate datasource with complex types', () => {
    const schema = defineSchema({
      id: uuid('id', { jsonPath: '$.id' }),
      isActive: boolean('isActive', { jsonPath: '$.isActive' }),
      createdAt: dateTime('createdAt', { jsonPath: '$.createdAt' }),
      tags: array('tags', z.string(), {
        jsonPath: '$.tags',
        innerType: 'String',
      }),
      optionalField: {
        name: 'optionalField',
        type: 'Nullable(String)' as any,
        jsonPath: '$.optional',
        schema: z.string().nullable(),
        nullable: true,
      },
    });

    const dataSource = defineDataSource({
      name: 'complex_table__v2',
      version: 2,
      schema,
      engine: 'ReplacingMergeTree',
      sortingKey: ['createdAt', 'id'],
      partitionBy: 'toYYYYMM(createdAt)',
      ttl: 'createdAt + INTERVAL 30 DAY',
    });

    const result = generateDatasourceFile(dataSource);

    expect(result).toContain('VERSION 2');
    expect(result).toContain('`id` UUID `json:$.id`');
    expect(result).toContain('`isActive` Boolean `json:$.isActive`');
    expect(result).toContain('`createdAt` DateTime64 `json:$.createdAt`');
    expect(result).toContain('`tags` Array(String) `json:$.tags`');
    expect(result).toContain(
      '`optionalField` Nullable(String) `json:$.optional`'
    );
    expect(result).toContain('ENGINE "ReplacingMergeTree"');
    expect(result).toContain('ENGINE_SORTING_KEY "createdAt,id"');
    expect(result).toContain('ENGINE_PARTITION_KEY "toYYYYMM(createdAt)"');
    expect(result).toContain('ENGINE_TTL "createdAt + INTERVAL 30 DAY"');
  });

  test('should generate datasource without version', () => {
    const schema = defineSchema({
      id: string('id'),
      value: int64('value'),
    });

    const dataSource = defineDataSource({
      name: 'simple_table',
      schema,
      engine: 'MergeTree',
    });

    const result = generateDatasourceFile(dataSource);

    expect(result).not.toContain('VERSION');
    expect(result).toContain('SCHEMA >');
    expect(result).toContain('ENGINE "MergeTree"');
  });

  test('should generate datasource with comments', () => {
    const schema = defineSchema({
      id: string('id', {
        jsonPath: '$.id',
        comment: 'Unique identifier for the record',
      }),
      userId: string('userId', {
        jsonPath: '$.user_id',
        comment: 'Reference to user table',
      }),
      timestamp: int64('timestamp', {
        jsonPath: '$.ts',
        comment: 'Unix timestamp in milliseconds',
      }),
    });

    const dataSource = defineDataSource({
      name: 'events_with_comments__v1',
      schema,
      engine: 'MergeTree',
    });

    const result = generateDatasourceFile(dataSource);

    expect(result).toContain('`id` String `json:$.id`,');
    expect(result).toContain('# Unique identifier for the record');
    expect(result).toContain('`userId` String `json:$.user_id`,');
    expect(result).toContain('# Reference to user table');
    expect(result).toContain('`timestamp` Int64 `json:$.ts`');
    expect(result).toContain('# Unix timestamp in milliseconds');
  });

  test('should generate SummingMergeTree datasource', () => {
    const schema = defineSchema({
      date: dateTime('date'),
      userId: string('userId'),
      metric: int64('metric'),
    });

    const dataSource = defineDataSource({
      name: 'metrics__v1',
      schema,
      engine: 'SummingMergeTree',
      sortingKey: ['date', 'userId'],
    });

    const result = generateDatasourceFile(dataSource);

    expect(result).toContain('ENGINE "SummingMergeTree"');
    expect(result).toContain('ENGINE_SORTING_KEY "date,userId"');
  });

  test('should generate AggregatingMergeTree datasource', () => {
    const schema = defineSchema({
      timestamp: dateTime('timestamp'),
      category: string('category'),
      aggregatedValue: int64('aggregatedValue'),
    });

    const dataSource = defineDataSource({
      name: 'aggregated_metrics__v1',
      schema,
      engine: 'AggregatingMergeTree',
      sortingKey: ['timestamp', 'category'],
      partitionBy: 'toYYYYMM(timestamp)',
    });

    const result = generateDatasourceFile(dataSource);

    expect(result).toContain('ENGINE "AggregatingMergeTree"');
    expect(result).toContain('ENGINE_PARTITION_KEY "toYYYYMM(timestamp)"');
  });

  test('should extract datasource name correctly', () => {
    const schema = defineSchema({
      id: string('id'),
    });

    const dataSource1 = defineDataSource({
      name: 'events__v1',
      schema,
      engine: 'MergeTree',
    });

    const dataSource2 = defineDataSource({
      name: 'simple_table',
      schema,
      engine: 'MergeTree',
    });

    const dataSource3 = defineDataSource({
      name: 'complex_name__v999',
      schema,
      engine: 'MergeTree',
    });

    expect(extractDatasourceName(dataSource1)).toBe('events');
    expect(extractDatasourceName(dataSource2)).toBe('simple_table');
    expect(extractDatasourceName(dataSource3)).toBe('complex_name');
  });

  test('should handle datasource with minimal configuration', () => {
    const schema = defineSchema({
      id: string('id'),
    });

    const dataSource = defineDataSource({
      name: 'minimal__v1',
      schema,
      engine: 'MergeTree',
    });

    const result = generateDatasourceFile(dataSource);

    expect(result).toContain('`id` String');
    expect(result).toContain('ENGINE "MergeTree"');
    expect(result).not.toContain('ENGINE_SORTING_KEY');
    expect(result).not.toContain('ENGINE_PARTITION_KEY');
    expect(result).not.toContain('ENGINE_TTL');
  });

  test('should handle large schema with many columns', () => {
    const schema = defineSchema({
      id: string('id'),
      userId: string('userId'),
      sessionId: string('sessionId'),
      timestamp: int64('timestamp'),
      eventType: string('eventType'),
      page: string('page'),
      referrer: string('referrer'),
      userAgent: string('userAgent'),
      ip: string('ip'),
      country: string('country'),
      city: string('city'),
      isBot: boolean('isBot'),
      duration: int64('duration'),
      revenue: int64('revenue'),
    });

    const dataSource = defineDataSource({
      name: 'analytics_events__v1',
      schema,
      engine: 'MergeTree',
      sortingKey: ['timestamp', 'userId', 'sessionId'],
      partitionBy: 'toYYYYMM(timestamp)',
    });

    const result = generateDatasourceFile(dataSource);

    // Check that all columns are present
    Object.values(schema).forEach((column) => {
      expect(result).toContain(`\`${column.name}\``);
    });

    expect(result).toContain('ENGINE_SORTING_KEY "timestamp,userId,sessionId"');
    expect(result).toContain('ENGINE_PARTITION_KEY "toYYYYMM(timestamp)"');
  });
});
