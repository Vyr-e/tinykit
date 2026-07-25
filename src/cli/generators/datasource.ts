import type { DataSourceConfig, SchemaDefinition } from '../../types.js';

export function generateDatasourceFile<T extends SchemaDefinition>(
  dataSource: DataSourceConfig<T>
): string {
  const lines: string[] = [];

  // Add version if specified
  if (dataSource.version) {
    lines.push(`VERSION ${dataSource.version}`);
    lines.push('');
  }

  // Generate schema section
  lines.push('SCHEMA >');

  // Add columns
  const columns = Object.values(dataSource.schema);
  columns.forEach((columnDef, idx) => {
    let columnLine = `    \`${columnDef.name}\` ${columnDef.type}`;
    // Always add JSONPath for ingestion compatibility. Array columns need the
    // [:] operator - Tinybird rejects a bare path on an Array type.
    const isArray = /^Array\(/.test(columnDef.type);
    const jsonPath =
      columnDef.jsonPath || `$.${columnDef.name}${isArray ? '[:]' : ''}`;
    columnLine += ` \`json:${jsonPath}\``;
    const isLast = idx === columns.length - 1;
    lines.push(isLast ? columnLine : columnLine + ',');
    if (columnDef.comment) {
      lines.push(`    # ${columnDef.comment}`);
    }
  });

  lines.push('');

  // Add engine configuration
  lines.push(`ENGINE "${dataSource.engine}"`);

  // Add sorting key if specified
  if (dataSource.sortingKey && dataSource.sortingKey.length > 0) {
    lines.push(`ENGINE_SORTING_KEY "${dataSource.sortingKey.join(',')}"`);
  }

  // Add partition key if specified
  if (dataSource.partitionBy) {
    lines.push(`ENGINE_PARTITION_KEY "${dataSource.partitionBy}"`);
  }

  // Add TTL if specified
  if (dataSource.ttl) {
    lines.push(`ENGINE_TTL "${dataSource.ttl}"`);
  }

  return lines.join('\n') + '\n';
}

export function extractDatasourceName<T extends SchemaDefinition>(
  dataSource: DataSourceConfig<T>
): string {
  return dataSource.name;
}
