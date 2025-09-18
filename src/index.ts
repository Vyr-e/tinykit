export { 
  Tinybird, 
  type Config,
  TinybirdError,
  TinybirdTimeoutError,
  TinybirdUnauthorizedError,
  TinybirdRetryExhaustedError,
  TinybirdValidationError
} from './client';
export { defineDataSource, defineSchema, generateCreateTableSQL } from './schema';
export { definePipe, defineParameters, PipeBuilder } from './pipe';
export { query } from './query';
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
} from './ingest';

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
} from './schema';

export {
  stringParam,
  int64Param,
  float64Param,
  dateTimeParam,
  dateParam,
  booleanParam,
  enumParam,
} from './pipe';

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
} from './query';

export * from './types';

// Helper to convert TinyKit parameters to Zod schema for tb.buildPipe()
export { createZodSchemaFromParameters } from './client';