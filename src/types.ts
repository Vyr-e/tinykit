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

declare const pipeOutputType: unique symbol;

/**
 * Configuration for a Tinybird Pipe.
 * @template TParams The parameters definition for the Pipe.
 * @template TName The name of the Pipe.
 * @template TOutput The inferred output row for the Pipe.
 */
export type PipeConfig<
  TParams extends QueryParameters = QueryParameters,
  TName extends string = string,
  TOutput = unknown
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
  /** Carries the output row through the type system without adding runtime data. */
  readonly [pipeOutputType]?: TOutput;
};

/** Infers the output row carried by a Pipe configuration. */
export type InferPipeOutput<TPipe> = TPipe extends PipeConfig<
  QueryParameters,
  string,
  infer TOutput
>
  ? TOutput
  : never;

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

type SQLWhitespace = ' ' | '\n' | '\r' | '\t';
type SQLQuote = "'" | '"' | '`';

type TrimSQLLeft<T extends string> =
  T extends `${SQLWhitespace}${infer Rest}` ? TrimSQLLeft<Rest> : T;

type TrimSQLRight<T extends string> =
  T extends `${infer Rest}${SQLWhitespace}` ? TrimSQLRight<Rest> : T;

type TrimSQL<T extends string> = TrimSQLLeft<TrimSQLRight<T>>;

type SplitAfterSelect<T extends string> =
  T extends `${infer _Before}SELECT ${infer After}`
    ? After
    : T extends `${infer _Before}select ${infer After}`
    ? After
    : T extends `${infer _Before}Select ${infer After}`
    ? After
    : T extends `${infer _Before}SELECT\n${infer After}`
    ? After
    : T extends `${infer _Before}select\n${infer After}`
    ? After
    : T extends `${infer _Before}Select\n${infer After}`
    ? After
    : never;

type AfterLastSelect<T extends string> = [
  SplitAfterSelect<T>
] extends [never]
  ? never
  : SplitAfterSelect<T> extends infer Rest extends string
  ? [SplitAfterSelect<Rest>] extends [never]
    ? Rest
    : AfterLastSelect<Rest>
  : never;

type ProjectionBeforeFrom<T extends string> =
  T extends `${infer Projection} FROM ${string}`
    ? TrimSQL<Projection>
    : T extends `${infer Projection} from ${string}`
    ? TrimSQL<Projection>
    : T extends `${infer Projection} From ${string}`
    ? TrimSQL<Projection>
    : T extends `${infer Projection}\nFROM ${string}`
    ? TrimSQL<Projection>
    : T extends `${infer Projection}\nfrom ${string}`
    ? TrimSQL<Projection>
    : T extends `${infer Projection}\nFrom ${string}`
    ? TrimSQL<Projection>
    : never;

type SQLProjection<T extends string> = AfterLastSelect<T> extends infer Rest
  extends string
  ? ProjectionBeforeFrom<Rest>
  : never;

type PopSQLStack<TStack extends readonly unknown[]> =
  TStack extends readonly [unknown, ...infer Rest] ? Rest : [];

type PushSQLColumn<
  TColumns extends readonly string[],
  TColumn extends string
> = TrimSQL<TColumn> extends ''
  ? TColumns
  : [...TColumns, TrimSQL<TColumn>];

/**
 * Splits a SELECT projection only at top-level commas. Commas inside functions,
 * arrays, tuples, template expressions, and quoted strings remain intact.
 */
type SplitSQLColumns<
  TInput extends string,
  TCurrent extends string = '',
  TColumns extends readonly string[] = [],
  TStack extends readonly unknown[] = [],
  TQuote extends SQLQuote | '' = ''
> = TInput extends `${infer Character}${infer Rest}`
  ? TQuote extends SQLQuote
    ? Character extends TQuote
      ? SplitSQLColumns<
          Rest,
          `${TCurrent}${Character}`,
          TColumns,
          TStack,
          ''
        >
      : SplitSQLColumns<
          Rest,
          `${TCurrent}${Character}`,
          TColumns,
          TStack,
          TQuote
        >
    : Character extends SQLQuote
    ? SplitSQLColumns<
        Rest,
        `${TCurrent}${Character}`,
        TColumns,
        TStack,
        Character
      >
    : Character extends '(' | '[' | '{'
    ? SplitSQLColumns<
        Rest,
        `${TCurrent}${Character}`,
        TColumns,
        [unknown, ...TStack],
        ''
      >
    : Character extends ')' | ']' | '}'
    ? SplitSQLColumns<
        Rest,
        `${TCurrent}${Character}`,
        TColumns,
        PopSQLStack<TStack>,
        ''
      >
    : Character extends ','
    ? TStack extends readonly []
      ? SplitSQLColumns<Rest, '', PushSQLColumn<TColumns, TCurrent>, [], ''>
      : SplitSQLColumns<
          Rest,
          `${TCurrent}${Character}`,
          TColumns,
          TStack,
          ''
        >
    : SplitSQLColumns<
        Rest,
        `${TCurrent}${Character}`,
        TColumns,
        TStack,
        ''
      >
  : PushSQLColumn<TColumns, TCurrent>;

/**
 * Finds a top-level alias while ignoring AS tokens inside functions such as
 * CAST(value AS UInt64).
 */
type SplitSQLAlias<
  TInput extends string,
  TCurrent extends string = '',
  TStack extends readonly unknown[] = [],
  TQuote extends SQLQuote | '' = ''
> = TQuote extends ''
  ? TStack extends readonly []
    ? TInput extends ` AS ${infer Alias}`
      ? [TrimSQL<TCurrent>, TrimSQL<Alias>]
      : TInput extends ` as ${infer Alias}`
      ? [TrimSQL<TCurrent>, TrimSQL<Alias>]
      : TInput extends ` As ${infer Alias}`
      ? [TrimSQL<TCurrent>, TrimSQL<Alias>]
      : TInput extends ` aS ${infer Alias}`
      ? [TrimSQL<TCurrent>, TrimSQL<Alias>]
      : SplitSQLAliasCharacter<TInput, TCurrent, TStack, TQuote>
    : SplitSQLAliasCharacter<TInput, TCurrent, TStack, TQuote>
  : SplitSQLAliasCharacter<TInput, TCurrent, TStack, TQuote>;

type SplitSQLAliasCharacter<
  TInput extends string,
  TCurrent extends string,
  TStack extends readonly unknown[],
  TQuote extends SQLQuote | ''
> = TInput extends `${infer Character}${infer Rest}`
  ? TQuote extends SQLQuote
    ? Character extends TQuote
      ? SplitSQLAlias<Rest, `${TCurrent}${Character}`, TStack, ''>
      : SplitSQLAlias<Rest, `${TCurrent}${Character}`, TStack, TQuote>
    : Character extends SQLQuote
    ? SplitSQLAlias<Rest, `${TCurrent}${Character}`, TStack, Character>
    : Character extends '(' | '[' | '{'
    ? SplitSQLAlias<
        Rest,
        `${TCurrent}${Character}`,
        [unknown, ...TStack],
        ''
      >
    : Character extends ')' | ']' | '}'
    ? SplitSQLAlias<
        Rest,
        `${TCurrent}${Character}`,
        PopSQLStack<TStack>,
        ''
      >
    : SplitSQLAlias<Rest, `${TCurrent}${Character}`, TStack, ''>
  : [TrimSQL<TCurrent>, never];

type UnquoteSQLIdentifier<T extends string> =
  T extends `"${infer Identifier}"`
    ? Identifier
    : T extends `'${infer Identifier}'`
    ? Identifier
    : T extends `\`${infer Identifier}\``
    ? Identifier
    : T;

type LastSQLIdentifier<T extends string> =
  T extends `${infer _Prefix}.${infer Rest}` ? LastSQLIdentifier<Rest> : T;

type SQLColumnExpression<TColumn extends string> =
  SplitSQLAlias<TrimSQL<TColumn>>[0];

type SQLColumnName<TColumn extends string> =
  SplitSQLAlias<TrimSQL<TColumn>> extends [
    infer Expression extends string,
    infer Alias
  ]
    ? [Alias] extends [never]
      ? UnquoteSQLIdentifier<LastSQLIdentifier<TrimSQL<Expression>>>
      : Alias extends string
      ? UnquoteSQLIdentifier<TrimSQL<Alias>>
      : never
    : never;

type InferSQLTypeName<TType extends string> =
  Lowercase<TrimSQL<TType>> extends `nullable(${infer Inner})`
    ? InferSQLTypeName<Inner> | null
    : Lowercase<TrimSQL<TType>> extends `array(${infer Inner})`
    ? InferSQLTypeName<Inner>[]
    : Lowercase<TrimSQL<TType>> extends
        | 'string'
        | `fixedstring(${string})`
        | 'uuid'
        | 'ipv4'
        | 'ipv6'
        | `enum${string}`
        | 'date'
        | 'datetime'
        | `datetime64${string}`
    ? string
    : Lowercase<TrimSQL<TType>> extends
        | `int${string}`
        | `uint${string}`
        | `float${string}`
        | `decimal${string}`
    ? number
    : Lowercase<TrimSQL<TType>> extends 'bool' | 'boolean'
    ? boolean
    : unknown;

type InferSQLExpressionType<
  TExpression extends string,
  TSource extends Record<string, unknown>
> = Lowercase<TrimSQL<TExpression>> extends `cast(${string} as ${infer Type})`
  ? InferSQLTypeName<Type>
  : Lowercase<TrimSQL<TExpression>> extends
      | `count(${string})`
      | `countdistinct(${string})`
      | `uniq${string}(${string})`
      | `sum(${string})`
      | `sumif(${string})`
      | `avg(${string})`
      | `avgif(${string})`
      | `quantile${string}(${string})`
  ? number
  : Lowercase<TrimSQL<TExpression>> extends
      | `toint${string}(${string})`
      | `touint${string}(${string})`
      | `tofloat${string}(${string})`
      | `todecimal${string}(${string})`
      | `tounixtimestamp${string}(${string})`
      | `datediff(${string})`
      | `length(${string})`
  ? number
  : Lowercase<TrimSQL<TExpression>> extends
      | `tostring(${string})`
      | `lower(${string})`
      | `upper(${string})`
      | `concat(${string})`
      | `formatdatetime(${string})`
      | `touuid(${string})`
      | `todate(${string})`
      | `todatetime${string}(${string})`
      | `tostartof${string}(${string})`
  ? string
  : Lowercase<TrimSQL<TExpression>> extends
      | `tobool(${string})`
      | `isnull(${string})`
      | `isnotnull(${string})`
  ? boolean
  : TrimSQL<TExpression> extends `'${string}'` | `"${string}"`
  ? string
  : Lowercase<TrimSQL<TExpression>> extends 'true' | 'false'
  ? boolean
  : Lowercase<TrimSQL<TExpression>> extends 'null'
  ? null
  : TrimSQL<TExpression> extends `${number}`
  ? number
  : UnquoteSQLIdentifier<
      LastSQLIdentifier<TrimSQL<TExpression>>
    > extends infer SourceKey
  ? SourceKey extends keyof TSource
    ? TSource[SourceKey]
    : unknown
  : unknown;

type SQLColumnRecord<
  TColumn extends string,
  TSource extends Record<string, unknown>
> = TrimSQL<TColumn> extends '*'
  ? TSource
  : SQLColumnName<TColumn> extends infer Name extends string
  ? Name extends ''
    ? Record<string, unknown>
    : {
        [Key in Name]: InferSQLExpressionType<
          SQLColumnExpression<TColumn>,
          TSource
        >;
      }
  : Record<string, unknown>;

type Simplify<T> = { [Key in keyof T]: T[Key] };

type InferSQLColumns<
  TColumns extends readonly string[],
  TSource extends Record<string, unknown>,
  TResult = {}
> = TColumns extends readonly [
  infer Column extends string,
  ...infer Rest extends string[]
]
  ? InferSQLColumns<Rest, TSource, TResult & SQLColumnRecord<Column, TSource>>
  : Simplify<TResult>;

type InferSQLProjection<
  TProjection,
  TSource extends Record<string, unknown>
> = [TProjection] extends [never]
  ? Record<string, unknown>
  : TProjection extends string
  ? SplitSQLColumns<TProjection> extends infer Columns extends string[]
    ? Columns extends []
      ? Record<string, unknown>
      : InferSQLColumns<Columns, TSource>
    : Record<string, unknown>
  : Record<string, unknown>;

/**
 * Conservatively infers a row type from the final SELECT projection in a raw
 * SQL statement.
 *
 * It understands top-level aliases, nested function arguments, source-schema
 * columns, common ClickHouse aggregates, casts, literals, and date conversion
 * functions. Anything it cannot prove is `unknown`, never a guessed scalar.
 *
 * This is static inference only. Pass a Zod `data` schema to `Tinybird.pipe`
 * when runtime response validation is required.
 *
 * @template TSQL The literal SQL string.
 * @template TSource The source row used to resolve direct column references.
 */
export type InferSQLReturnType<
  TSQL extends string,
  TSource extends Record<string, unknown> = Record<string, unknown>
> = string extends TSQL
  ? Record<string, unknown>
  : InferSQLProjection<SQLProjection<TSQL>, TSource>;
