import { z } from 'zod';

/**
 * Defines the possible SQL data types that can be used in a Tinybird schema.
 */
export type SqlType =
  | 'String'
  | 'Int8'
  | 'Int16'
  | 'Int32'
  | 'Int64'
  | 'UInt8'
  | 'UInt16'
  | 'UInt32'
  | 'UInt64'
  | 'Float32'
  | 'Float64'
  | 'DateTime'
  | 'DateTime64'
  | `DateTime64(${number})`
  | 'Date'
  | 'Boolean'
  | 'UUID'
  | `Array(${string})`
  | `Map(${string}, ${string})`
  | `Tuple(${string})`
  | `Nested(${string})`
  | `LowCardinality(${string})`
  | `Nullable(${string})`
  | 'JSON'
  | 'IPv4'
  | 'IPv6';

/**
 * Represents the definition of a single column in a Tinybird DataSource.
 * @template T The TypeScript type of the column's data.
 */
export type ColumnDefinition<T = any> = {
  /** The name of the column in the database. */
  name: string;
  /** The SQL data type of the column. */
  type: SqlType;
  /** The JSON path to extract this column's data from an incoming event. */
  jsonPath?: string;
  /** Whether the column is nullable. */
  nullable?: boolean;
  /** A description or comment for the column. */
  comment?: string;
  /** The Zod schema for validating the column's data. */
  schema: z.ZodSchema<T>;
};

/**
 * A record of column definitions that make up a Tinybird DataSource schema.
 * The keys are the desired property names in the resulting TypeScript type.
 */
export type SchemaDefinition = Record<string, ColumnDefinition>;

/**
 * Configuration for a Tinybird DataSource.
 * @template TSchema The schema definition for the DataSource.
 */
export type DataSourceConfig<
  TSchema extends SchemaDefinition = SchemaDefinition
> = {
  /** The name of the DataSource in Tinybird. */
  name: string;
  /** The schema definition for the DataSource. */
  schema: TSchema;
  /** The table engine to use for the DataSource. */
  engine:
    | 'MergeTree'
    | 'ReplacingMergeTree'
    | 'SummingMergeTree'
    | 'AggregatingMergeTree';
  /** An array of column names to use as the sorting key. */
  sortingKey?: (keyof TSchema)[];
  /** The partitioning scheme for the table. */
  partitionBy?: string;
  /** The Time-to-Live (TTL) expression for data in the table. */
  ttl?: string;
  /** The version of the DataSource, used for migrations. */
  version?: number;
};

/**
 * Represents a single parameter for a Tinybird Pipe.
 * @template T The TypeScript type of the parameter.
 */
export type QueryParameter<T = any> = {
  /** The name of the parameter. */
  name: string;
  /** The data type of the parameter in Tinybird. */
  type: 'String' | 'Int64' | 'Float64' | 'DateTime' | 'Date' | 'Boolean';
  /** Whether the parameter is required. */
  required?: boolean;
  /** The default value for the parameter if not provided. */
  default?: T;
  /** The Zod schema for validating the parameter. */
  schema: z.ZodSchema<T>;
};

/**
 * A record of parameters for a Tinybird Pipe.
 * The keys are the names of the parameters.
 */
export type QueryParameters = Record<string, QueryParameter>;

/**
 * Defines the possible time granularities for time-based queries.
 */
export type Granularity = '1m' | '1h' | '1d' | '1w' | '1M';

/**
 * Infers a TypeScript type from a TinyKit schema definition.
 * @template T The schema definition.
 */
export type InferSchemaType<T extends SchemaDefinition> = {
  [K in keyof T]: z.infer<T[K]['schema']>;
};

// Helper: is the `default` property required on this parameter type?
type HasRequiredDefault<P> = 'default' extends keyof P
  ? Pick<P, 'default'> extends Required<Pick<P, 'default'>>
    ? true
    : false
  : false;

/**
 * Infers the TypeScript type for the parameters of a Pipe, respecting optionality.
 * Parameters with default values or marked as not required are optional.
 * @template T The query parameters definition.
 */
export type InferParametersType<T extends QueryParameters> = {
  // Required parameters (explicitly marked as required: true AND no default value)
  [K in keyof T as T[K]['required'] extends true
    ? HasRequiredDefault<T[K]> extends true
      ? never
      : K
    : never]: z.infer<T[K]['schema']>;
} & {
  // Optional parameters (not required OR has default value)
  [K in keyof T as T[K]['required'] extends true
    ? HasRequiredDefault<T[K]> extends true
      ? K
      : never
    : K]?: z.infer<T[K]['schema']>;
};

/**
 * Infers the TypeScript type for the parameters of a Pipe, with all defaults applied.
 * This is used internally in the query builder endpoint.
 * @template T The query parameters definition.
 */
export type InferParametersWithDefaults<T extends QueryParameters> = {
  [K in keyof T]: z.infer<T[K]['schema']>;
};

/**
 * Configuration for a Tinybird Pipe.
 * @template TParams The parameters definition for the Pipe.
 * @template TName The name of the Pipe.
 */
export type PipeConfig<
  TParams extends QueryParameters = QueryParameters,
  TName extends string = string
> = {
  /** The name of the Pipe in Tinybird. */
  name: TName;
  /** The version of the Pipe, used for migrations. */
  version?: number;
  /** The parameters definition for the Pipe. */
  parameters: TParams;
  /** A function that generates the SQL for the Pipe. */
  sql: (params: InferParametersType<TParams> | {}) => string;
  /** A flag to indicate if the pipe was defined with .raw(). */
  isRaw?: boolean;
  /** The raw SQL string, if the pipe was defined with .raw(). */
  rawSql?: string;
};

/**
 * Represents the successful result of a Tinybird Pipe query.
 * @template T The type of the data rows.
 */
export type PipeResult<T> = {
  /** Metadata about the columns in the result set. */
  meta: Array<{ name: string; type: string }>;
  /** The array of data rows returned by the query. */
  data: T[];
  /** Statistics about the query execution. */
  statistics?: {
    elapsed: number;
    rows_read: number;
    bytes_read: number;
  };
};

/**
 * Represents an error response from a Tinybird API call.
 */
export type PipeErrorResponse = {
  /** The error message. */
  error: string;
  message?: string;
  code?: string;
};

/**
 * (Experimental) Infers a TypeScript return type from a raw SQL SELECT statement.
 * @template T The SQL string.
 */
export type InferSQLReturnType<T extends string> =
  T extends `SELECT ${infer Columns} FROM ${string}`
    ? ParseColumns<Columns>
    : unknown;

type ParseColumns<T extends string> = T extends `${infer Col}, ${infer Rest}`
  ? { [K in ParseColumnName<Col>]: ParseColumnType<Col> } & ParseColumns<Rest>
  : { [K in ParseColumnName<T>]: ParseColumnType<T> };

type ParseColumnName<T extends string> = T extends `${string} as ${infer Alias}`
  ? Alias
  : T extends `${infer Name}(${string})`
  ? Name
  : T;

type ParseColumnType<T extends string> = T extends `count(${string})`
  ? number
  : T extends `${string}Distinct(${string})`
  ? number
  : string;
