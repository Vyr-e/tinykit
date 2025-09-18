import { z } from 'zod';
import type { ColumnDefinition, DataSourceConfig, SchemaDefinition } from './types';

/**
 * Defines a String column for a Tinybird DataSource.
 * @param name The name of the column.
 * @param options Configuration options for the column.
 * @returns A column definition.
 *
 * @example
 * ```
 * const mySchema = defineSchema({
 *   myColumn: string('my_column'),
 * });
 * ```
 */
export const string = (
  name: string,
  options?: { jsonPath?: string; nullable?: boolean; comment?: string }
): ColumnDefinition<string> => ({
  name,
  type: 'String',
  schema: z.string(),
  ...options,
});

/**
 * Defines an Int32 column for a Tinybird DataSource.
 * @param name The name of the column.
 * @param options Configuration options for the column.
 * @returns A column definition.
 *
 * @example
 * ```
 * const mySchema = defineSchema({
 *   myColumn: int32('my_column'),
 * });
 * ```
 */
export const int32 = (
  name: string,
  options?: { jsonPath?: string; nullable?: boolean; comment?: string }
): ColumnDefinition<number> => ({
  name,
  type: 'Int32',
  schema: z.number().int(),
  ...options,
});

/**
 * Defines an Int64 column for a Tinybird DataSource.
 * @param name The name of the column.
 * @param options Configuration options for the column.
 * @returns A column definition.
 *
 * @example
 * ```
 * const mySchema = defineSchema({
 *   myColumn: int64('my_column'),
 * });
 * ```
 */
export const int64 = (
  name: string,
  options?: { jsonPath?: string; nullable?: boolean; comment?: string }
): ColumnDefinition<number> => ({
  name,
  type: 'Int64',
  schema: z.number().int(),
  ...options,
});

/**
 * Defines a Float64 column for a Tinybird DataSource.
 * @param name The name of the column.
 * @param options Configuration options for the column.
 * @returns A column definition.
 *
 * @example
 * ```
 * const mySchema = defineSchema({
 *   myColumn: float64('my_column'),
 * });
 * ```
 */
export const float64 = (
  name: string,
  options?: { jsonPath?: string; nullable?: boolean; comment?: string }
): ColumnDefinition<number> => ({
  name,
  type: 'Float64',
  schema: z.number(),
  ...options,
});

/**
 * Defines a Boolean column for a Tinybird DataSource.
 * @param name The name of the column.
 * @param options Configuration options for the column.
 * @returns A column definition.
 *
 * @example
 * ```
 * const mySchema = defineSchema({
 *   myColumn: boolean('my_column'),
 * });
 * ```
 */
export const boolean = (
  name: string,
  options?: { jsonPath?: string; nullable?: boolean; comment?: string }
): ColumnDefinition<boolean> => ({
  name,
  type: 'Boolean',
  schema: z.boolean(),
  ...options,
});

/**
 * Defines a DateTime64 column for a Tinybird DataSource.
 * @param name The name of the column.
 * @param options Configuration options for the column.
 * @returns A column definition.
 *
 * @example
 * ```
 * const mySchema = defineSchema({
 *   myColumn: dateTime('my_column'),
 *   myPreciseColumn: dateTime('my_precise_column', { precision: 9 }),
 * });
 * ```
 */
export const dateTime = (
  name: string,
  options?: { 
    jsonPath?: string; 
    nullable?: boolean; 
    comment?: string;
    precision?: number; // 0-9, defaults to 3 (milliseconds)
  }
): ColumnDefinition<string | Date | number> => ({
  name,
  type: `DateTime64(${options?.precision ?? 3})`,
  schema: z.union([z.string(), z.date(), z.number()]),
  jsonPath: options?.jsonPath,
  nullable: options?.nullable,
  comment: options?.comment,
});

/**
 * Defines a Date column for a Tinybird DataSource.
 * @param name The name of the column.
 * @param options Configuration options for the column.
 * @returns A column definition.
 *
 * @example
 * ```
 * const mySchema = defineSchema({
 *   myColumn: date('my_column'),
 * });
 * ```
 */
export const date = (
  name: string,
  options?: { jsonPath?: string; nullable?: boolean; comment?: string }
): ColumnDefinition<string | Date> => ({
  name,
  type: 'Date',
  schema: z.union([z.string(), z.date()]),
  ...options,
});

/**
 * Defines a UUID column for a Tinybird DataSource.
 * @param name The name of the column.
 * @param options Configuration options for the column.
 * @returns A column definition.
 *
 * @example
 * ```
 * const mySchema = defineSchema({
 *   myColumn: uuid('my_column'),
 * });
 * ```
 */
export const uuid = (
  name: string,
  options?: { jsonPath?: string; nullable?: boolean; comment?: string }
): ColumnDefinition<string> => ({
  name,
  type: 'UUID',
  schema: z.string().uuid(),
  ...options,
});

/**
 * Defines an Array column for a Tinybird DataSource.
 * @param name The name of the column.
 * @param schema The Zod schema for the elements of the array.
 * @param options Configuration options for the column.
 * @returns A column definition.
 *
 * @example
 * ```
 * const mySchema = defineSchema({
 *   myColumn: array('my_column', z.string()),
 * });
 * ```
 */
export const array = <T extends z.ZodTypeAny>(
  name: string,
  schema: T,
  options?: { jsonPath?: string; nullable?: boolean; comment?: string; innerType?: string }
): ColumnDefinition<z.infer<T>[]> => ({
  name,
  type: `Array(${options?.innerType || 'String'})`,
  schema: z.array(schema),
  ...options,
});

/**
 * Defines a Map column for a Tinybird DataSource.
 * @param name The name of the column.
 * @param keySchema The Zod schema for the keys of the map.
 * @param valueSchema The Zod schema for the values of the map.
 * @param options Configuration options for the column.
 * @returns A column definition.
 *
 * @example
 * ```
 * const mySchema = defineSchema({
 *   myColumn: map('my_column', z.string(), z.number()),
 * });
 * ```
 */
export const map = <K extends z.ZodTypeAny, V extends z.ZodTypeAny>(
  name: string,
  keySchema: K,
  valueSchema: V,
  options?: { jsonPath?: string; nullable?: boolean; comment?: string; keyType?: string; valueType?: string }
): ColumnDefinition<Map<z.infer<K>, z.infer<V>>> => ({
  name,
  type: `Map(${options?.keyType || 'String'}, ${options?.valueType || 'String'})`,
  schema: z.map(keySchema, valueSchema),
  ...options,
});

/**
 * Defines a Tuple column for a Tinybird DataSource.
 * @param name The name of the column.
 * @param schema The Zod schema for the tuple.
 * @param options Configuration options for the column.
 * @returns A column definition.
 *
 * @example
 * ```
 * const mySchema = defineSchema({
 *   myColumn: tuple('my_column', z.tuple([z.string(), z.number()])),
 * });
 * ```
 */
export const tuple = <T extends z.ZodTuple<any>>(
  name: string,
  schema: T,
  options?: { jsonPath?: string; nullable?: boolean; comment?: string; types?: string[] }
): ColumnDefinition<z.infer<T>> => ({
  name,
  type: `Tuple(${options?.types?.join(', ') || 'String, String'})`,
  schema,
  ...options,
});

/**
 * Defines a Nested column for a Tinybird DataSource.
 * @param name The name of the column.
 * @param schema The Zod schema for the nested object.
 * @param options Configuration options for the column.
 * @returns A column definition.
 *
 * @example
 * ```
 * const mySchema = defineSchema({
 *   myColumn: nested('my_column', z.object({ a: z.string(), b: z.number() })),
 * });
 * ```
 */
export const nested = <T extends z.ZodObject<any>>(
  name: string,
  schema: T,
  options?: { jsonPath?: string; nullable?: boolean; comment?: string; fields?: string }
): ColumnDefinition<z.infer<T>> => ({
  name,
  type: `Nested(${options?.fields || 'field String'})`,
  schema,
  ...options,
});

/**
 * Defines a LowCardinality column for a Tinybird DataSource.
 * @param name The name of the column.
 * @param schema The Zod schema for the column's data.
 * @param options Configuration options for the column.
 * @returns A column definition.
 *
 * @example
 * ```
 * const mySchema = defineSchema({
 *   myColumn: lowCardinality('my_column', z.string()),
 * });
 * ```
 */
export const lowCardinality = <T extends z.ZodTypeAny>(
  name: string,
  schema: T,
  options?: { jsonPath?: string; nullable?: boolean; comment?: string; innerType?: string }
): ColumnDefinition<z.infer<T>> => ({
  name,
  type: `LowCardinality(${options?.innerType || 'String'})`,
  schema,
  ...options,
});

/**
 * Defines a Nullable column for a Tinybird DataSource.
 * @param name The name of the column.
 * @param baseSchema The base Zod schema for the column's data.
 * @param options Configuration options for the column.
 * @returns A column definition.
 *
 * @example
 * ```
 * const mySchema = defineSchema({
 *   myColumn: nullable('my_column', z.string()),
 * });
 * ```
 */
export const nullable = <T extends z.ZodTypeAny>(
  name: string,
  baseSchema: T,
  options?: { jsonPath?: string; comment?: string; innerType?: string }
): ColumnDefinition<z.infer<T> | null> => ({
  name,
  type: `Nullable(${options?.innerType || 'String'})`,
  schema: baseSchema.nullable ? baseSchema.nullable() : baseSchema.optional(),
  nullable: true,
  ...options,
});

/**
 * Defines a JSON column for a Tinybird DataSource.
 * @param name The name of the column.
 * @param options Configuration options for the column.
 * @returns A column definition.
 *
 * @example
 * ```
 * const mySchema = defineSchema({
 *   myColumn: json('my_column'),
 * });
 * ```
 */
export const json = (
  name: string,
  options?: { jsonPath?: string; nullable?: boolean; comment?: string }
): ColumnDefinition<any> => ({
  name,
  type: 'JSON',
  schema: z.any(),
  ...options,
});

/**
 * Defines an IPv4 column for a Tinybird DataSource.
 * @param name The name of the column.
 * @param options Configuration options for the column.
 * @returns A column definition.
 *
 * @example
 * ```
 * const mySchema = defineSchema({
 *   myColumn: ipv4('my_column'),
 * });
 * ```
 */
export const ipv4 = (
  name: string,
  options?: { jsonPath?: string; nullable?: boolean; comment?: string }
): ColumnDefinition<string> => ({
  name,
  type: 'IPv4',
  schema: z.string().ip({ version: 'v4' }),
  ...options,
});

/**
 * Defines an IPv6 column for a Tinybird DataSource.
 * @param name The name of the column.
 * @param options Configuration options for the column.
 * @returns A column definition.
 *
 * @example
 * ```
 * const mySchema = defineSchema({
 *   myColumn: ipv6('my_column'),
 * });
 * ```
 */
export const ipv6 = (
  name: string,
  options?: { jsonPath?: string; nullable?: boolean; comment?: string }
): ColumnDefinition<string> => ({
  name,
  type: 'IPv6',
  schema: z.string().ip({ version: 'v6' }),
  ...options,
});

/**
 * A helper function to define a Tinybird DataSource configuration with type inference.
 * @param config The DataSource configuration.
 * @returns The DataSource configuration.
 *
 * @example
 * ```
 * const myDataSource = defineDataSource({
 *   name: 'my_datasource',
 *   schema: mySchema,
 *   engine: 'MergeTree',
 *   sortingKey: ['id'],
 * });
 * ```
 */
export function defineDataSource<TSchema extends SchemaDefinition>(
  config: DataSourceConfig<TSchema>
): DataSourceConfig<TSchema> {
  return config;
}

/**
 * A helper function to define a Tinybird schema with type inference.
 * @param schema The schema definition.
 * @returns The schema definition.
 *
 * @example
 * ```
 * const mySchema = defineSchema({
 *   id: string('id'),
 *   name: string('name'),
 * });
 * ```
 */
export function defineSchema<T extends Record<string, ColumnDefinition>>(schema: T): T {
  return schema;
}

/**
 * Generates a .datasource file content from a DataSource configuration.
 * @param dataSource The DataSource configuration.
 * @returns A string representing the content of a .datasource file.
 *
 * @example
 * ```
 * const sql = generateCreateTableSQL(myDataSource);
 * console.log(sql);
 * ```
 */
export function generateCreateTableSQL<TSchema extends SchemaDefinition>(
  dataSource: DataSourceConfig<TSchema>
): string {
  const columns = Object.entries(dataSource.schema)
    .map(([_, col]) => {
      let columnDef = `    ` + '`' + `${col.name}` + '`' + ` ${col.type}`;
      if (col.jsonPath) {
        columnDef += ` ` + '`' + `json:${col.jsonPath}` + '`';
      }
      if (col.comment) {
        columnDef = `${columnDef},
    # ${col.comment}`;
      }
      return columnDef;
    })
    .join(',\n');

  let sql = `SCHEMA >\n${columns}\n\n`;

  if (dataSource.version) {
    sql = `VERSION ${dataSource.version}\n\n${sql}`;
  }

  sql += `ENGINE "${dataSource.engine}"
`;

  if (dataSource.sortingKey && dataSource.sortingKey.length > 0) {
    sql += `ENGINE_SORTING_KEY "${dataSource.sortingKey.join(',')}"
`;
  }

  if (dataSource.partitionBy) {
    sql += `ENGINE_PARTITION_KEY "${dataSource.partitionBy}"
`;
  }

  if (dataSource.ttl) {
    sql += `ENGINE_TTL "${dataSource.ttl}"
`;
  }

  return sql;
}
