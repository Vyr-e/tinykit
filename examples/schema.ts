import { z } from 'zod';
import {
  defineSchema,
  defineDataSource,
  uuid,
  string,
  float64,
  boolean,
  dateTime,
  array,
  map,
  tuple,
  nested,
  lowCardinality,
  nullable,
  json,
  ipv4,
  generateCreateTableSQL,
} from '../src';

const advancedSchema = defineSchema({
  // Basic types
  id: uuid('id'),
  event_name: string('event_name'),
  timestamp: dateTime('timestamp'),
  is_active: boolean('is_active'),
  value: float64('value'),

  // Advanced types
  user_id: lowCardinality('user_id', z.string()),
  country_code: nullable('country_code', z.string()),

  // Nested and complex types
  user_properties: map('user_properties', z.string(), z.any()),
  tags: array('tags', z.string()),
  location: tuple('location', z.tuple([z.number(), z.number()])),
  device: nested('device', z.object({
    deviceType: z.enum(['desktop', 'mobile', 'tablet']),
    os: z.string(),
    version: z.string(),
  })),

  // Special types
  raw_data: json('raw_data'),
  ip_address: ipv4('ip_address'),
});

const advancedDataSource = defineDataSource({
  name: 'advanced_schema_example__v1',
  schema: advancedSchema,
  engine: 'MergeTree',
  sortingKey: ['timestamp', 'event_name', 'user_id'],
  partitionBy: 'toYYYYMM(timestamp)',
  ttl: 'timestamp + INTERVAL 30 DAY',
});

console.log('=== Advanced Schema Example ===');
console.log(generateCreateTableSQL(advancedDataSource));