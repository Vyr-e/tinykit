import { z } from 'zod';

export type SqlType =
  | 'String'
  | 'Int8' | 'Int16' | 'Int32' | 'Int64'
  | 'UInt8' | 'UInt16' | 'UInt32' | 'UInt64'
  | 'Float32' | 'Float64'
  | 'DateTime' | 'DateTime64'
  | 'Date'
  | 'Boolean';

export type ColumnDefinition<T = any> = {
  name: string;
  type: SqlType;
  jsonPath?: string;
  nullable?: boolean;
  comment?: string;
  schema: z.ZodSchema<T>;
};

export type SchemaDefinition = Record<string, ColumnDefinition>;

export type DataSourceConfig<TSchema extends SchemaDefinition = SchemaDefinition> = {
  name: string;
  schema: TSchema;
  engine: 'MergeTree' | 'ReplacingMergeTree' | 'SummingMergeTree' | 'AggregatingMergeTree';
  sortingKey?: (keyof TSchema)[];
  partitionBy?: string;
  ttl?: string;
  version?: number;
};

export type QueryParameter<T = any> = {
  name: string;
  type: 'String' | 'Int64' | 'Float64' | 'DateTime' | 'Date' | 'Boolean';
  required?: boolean;
  default?: T;
  schema: z.ZodSchema<T>;
};

export type QueryParameters = Record<string, QueryParameter>;

export type Granularity = '1m' | '1h' | '1d' | '1w' | '1M';

export type InferSchemaType<T extends SchemaDefinition> = {
  [K in keyof T]: z.infer<T[K]['schema']>;
};

export type InferParametersType<T extends QueryParameters> = {
  [K in keyof T as T[K]['required'] extends true ? K : never]: z.infer<T[K]['schema']>;
} & {
  [K in keyof T as T[K]['required'] extends true ? never : K]?: z.infer<T[K]['schema']>;
};

export type PipeConfig<
  TParams extends QueryParameters = QueryParameters,
  TOutput extends z.ZodSchema = z.ZodSchema
> = {
  name: string;
  version?: number;
  parameters: TParams;
  outputSchema: TOutput;
  sql: (params: InferParametersType<TParams>) => string;
};

export type PipeResult<T> = {
  meta: Array<{ name: string; type: string }>;
  data: T[];
  statistics?: {
    elapsed: number;
    rows_read: number;
    bytes_read: number;
  };
};

export type PipeErrorResponse = {
  error: string;
  message?: string;
  code?: string;
};