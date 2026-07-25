export { 
  Tinybird, 
  type Config,
  TinybirdError,
  TinybirdTimeoutError,
  TinybirdUnauthorizedError,
  TinybirdRetryExhaustedError,
  TinybirdValidationError
} from './client.js';
export { defineDataSource, defineSchema, generateCreateTableSQL } from './schema.js';
export { definePipe, defineParameters, PipeBuilder } from './pipe.js';
export { query } from './query.js';
export type { SQLExpression, InferParameterTemplates } from './types.js';
export { 
  defineIngest, 
  streamingIngest, 
  syncIngest, 
  batchIngest,
  robustIngest,
  createIngestionReport,
  handleIngestionErrors,
  type IngestError,
  type CSVIngestOptions,
  type ParquetIngestOptions
} from './ingest.js';

export {
  string,
  int32,
  int64,
  float64,
  boolean,
  dateTime,
  date,
  uuid,
  array,
  map,
  tuple,
  nested,
  lowCardinality,
  nullable,
  json,
  ipv4,
  ipv6,
} from './schema.js';

export {
  stringParam,
  int64Param,
  float64Param,
  dateTimeParam,
  dateParam,
  booleanParam,
  enumParam,
} from './pipe.js';

export {
  count,
  sum,
  avg,
  min,
  max,
  toStartOfMinute,
  toStartOfHour,
  toStartOfDay,
  toStartOfWeek,
  toStartOfMonth,
  fromUnixTimestamp64Milli,
  toUnixTimestamp64Milli,
  timeGranularity,
  conditional,
  param,
  eq,
  neq,
  gt,
  gte,
  lt,
  lte,
  rowNumber,
  rank,
  denseRank,
  lag,
  lead,
  firstValue,
  lastValue,
} from './query.js';

export * from './types.js';

// Helper to convert TinyKit parameters to Zod schema for tb.buildPipe()
export { createZodSchemaFromParameters } from './client.js';
