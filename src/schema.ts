import { z } from 'zod';
import type { SqlType, ColumnDefinition, SchemaDefinition, DataSourceConfig } from './types';

export const column = <T>(
  type: SqlType,
  schema: z.ZodSchema<T>,
  options?: {
    jsonPath?: string;
    nullable?: boolean;
    comment?: string;
  }
) => (name: string): ColumnDefinition<T> => ({
  name,
  type,
  schema,
  ...options,
});

export const string = (options?: { jsonPath?: string; nullable?: boolean; comment?: string }) =>
  column('String', z.string(), options);

export const int32 = (options?: { jsonPath?: string; nullable?: boolean; comment?: string }) =>
  column('Int32', z.number().int(), options);

export const int64 = (options?: { jsonPath?: string; nullable?: boolean; comment?: string }) =>
  column('Int64', z.number().int(), options);

export const float64 = (options?: { jsonPath?: string; nullable?: boolean; comment?: string }) =>
  column('Float64', z.number(), options);

export const boolean = (options?: { jsonPath?: string; nullable?: boolean; comment?: string }) =>
  column('Boolean', z.boolean(), options);

export const dateTime = (options?: { jsonPath?: string; nullable?: boolean; comment?: string }) =>
  column('DateTime64', z.union([z.string(), z.date(), z.number()]), options);

export const date = (options?: { jsonPath?: string; nullable?: boolean; comment?: string }) =>
  column('Date', z.union([z.string(), z.date()]), options);

export function defineDataSource<TSchema extends SchemaDefinition>(
  config: DataSourceConfig<TSchema>
): DataSourceConfig<TSchema> {
  return config;
}

export function defineSchema<T extends Record<string, ColumnDefinition>>(schema: T): T {
  return schema;
}

export function generateCreateTableSQL<TSchema extends SchemaDefinition>(
  dataSource: DataSourceConfig<TSchema>
): string {
  const columns = Object.entries(dataSource.schema)
    .map(([_, col]) => {
      let columnDef = `    \`${col.name}\` ${col.type}`;
      if (col.jsonPath) {
        columnDef += ` \`json:${col.jsonPath}\``;
      }
      if (col.comment) {
        columnDef = `${columnDef},\n    # ${col.comment}`;
      }
      return columnDef;
    })
    .join(',\n');

  let sql = `SCHEMA >\n${columns}\n\n`;

  if (dataSource.version) {
    sql = `VERSION ${dataSource.version}\n\n${sql}`;
  }

  sql += `ENGINE "${dataSource.engine}"\n`;

  if (dataSource.sortingKey && dataSource.sortingKey.length > 0) {
    sql += `ENGINE_SORTING_KEY "${dataSource.sortingKey.join(',')}"\n`;
  }

  if (dataSource.partitionBy) {
    sql += `ENGINE_PARTITION_KEY "${dataSource.partitionBy}"\n`;
  }

  if (dataSource.ttl) {
    sql += `ENGINE_TTL "${dataSource.ttl}"\n`;
  }

  return sql;
}