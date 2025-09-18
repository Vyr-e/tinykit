import type {
  PipeConfig,
  QueryParameters,
  InferParametersType,
} from '../../types';

export function generatePipeFile<TParams extends QueryParameters>(
  pipe: PipeConfig<TParams>
): string {
  const lines: string[] = [];

  // Add version if specified
  if (pipe.version) {
    lines.push(`VERSION ${pipe.version}`);
    lines.push('');
  }

  // Add token if needed (for authenticated pipes)
  // lines.push('TOKEN "pipes_read"');
  // lines.push('');

  // Generate the pipe content
  lines.push('NODE endpoint');
  lines.push('SQL >');
  lines.push('    %');

  // Generate SQL with Tinybird template syntax
  const sql = pipe.isRaw ? pipe.sql({}) : generateTinybirdTemplateSQL(pipe);

  // Format SQL with proper indentation
  const sqlLines = sql.split('\n');
  sqlLines.forEach((line) => {
    if (line.trim()) {
      lines.push(`    ${line}`);
    } else {
      lines.push('');
    }
  });

  return lines.join('\n') + '\n';
}

export function extractPipeName<TParams extends QueryParameters>(
  pipe: PipeConfig<TParams>
): string {
  return pipe.name;
}

// Generate sample parameters for SQL generation
function generateSampleParams<T extends QueryParameters>(
  parameters: T
): InferParametersType<T> {
  const sampleParams: Record<string, unknown> = {};

  Object.entries(parameters).forEach(([key, param]) => {
    // Generate sample values based on parameter type
    switch (param.type) {
      case 'String':
        sampleParams[key] = param.default || 'sample_string';
        break;
      case 'Int64':
        sampleParams[key] = param.default || 123456789;
        break;
      case 'Float64':
        sampleParams[key] = param.default || 123.456;
        break;
      case 'DateTime':
        sampleParams[key] = param.default || '2024-01-01 00:00:00';
        break;
      case 'Date':
        sampleParams[key] = param.default || '2024-01-01';
        break;
      case 'Boolean':
        sampleParams[key] = param.default || true;
        break;
      default:
        sampleParams[key] = param.default || 'sample_value';
    }

    // Handle optional parameters
    if (!param.required && !param.default) {
      // For optional parameters without defaults, we'll include them in sample
      // The actual SQL generation will handle the conditional logic
    }
  });

  return sampleParams as InferParametersType<T>;
}

// Generate SQL with Tinybird template syntax
function generateTinybirdTemplateSQL<T extends QueryParameters>(
  pipe: PipeConfig<T>
): string {
  // Create template parameters that map to Tinybird syntax
  const templateParams = createTemplateParams(pipe.parameters);

  // Generate SQL using template parameters instead of real values
  const sql = pipe.sql(templateParams as InferParametersType<T>);

  return sql;
}

// Create template parameter object that generates Tinybird syntax
function createTemplateParams<T extends QueryParameters>(
  parameters: T
): Record<string, string> {
  const templateParams: Record<string, string> = {};

  Object.entries(parameters).forEach(([key, param]) => {
    // Map parameter types to Tinybird template syntax
    const template = mapParameterToTemplate(key, param);
    templateParams[key] = template;
  });

  return templateParams;
}

// Map individual parameter to Tinybird template syntax
function mapParameterToTemplate(name: string, param: any): string {
  const isRequired = param.required === true;
  const hasDefault = param.default !== undefined;

  // Get the base type name for Tinybird templates
  const tinybirdType = getTinybirdTemplateType(param.type);

  // Build template based on requirements and defaults
  if (isRequired) {
    return `{{ ${tinybirdType}(${name}, required=True) }}`;
  } else if (hasDefault) {
    const defaultValue = formatDefaultValue(param.default, param.type);
    return `{{ ${tinybirdType}(${name}, ${defaultValue}) }}`;
  } else {
    return `{{ ${tinybirdType}(${name}) }}`;
  }
}

// Map SQL types to Tinybird template types
function getTinybirdTemplateType(sqlType: string): string {
  // Handle basic types
  switch (sqlType) {
    case 'String':
      return 'String';
    case 'Int8':
    case 'Int16':
    case 'Int32':
    case 'Int64':
      return 'Int64'; // Tinybird typically uses Int64 for integers
    case 'UInt8':
    case 'UInt16':
    case 'UInt32':
    case 'UInt64':
      return 'UInt64'; // Tinybird UInt64 for unsigned integers
    case 'Float32':
    case 'Float64':
      return 'Float64';
    case 'DateTime':
    case 'DateTime64':
      return 'DateTime';
    case 'Date':
      return 'Date';
    case 'Boolean':
      return 'Boolean';
    case 'UUID':
      return 'String'; // UUIDs are strings in templates
    case 'JSON':
      return 'String'; // JSON fields are strings in templates
    case 'IPv4':
    case 'IPv6':
      return 'String'; // IP addresses are strings in templates
    default:
      // Handle complex types (Array, Map, Tuple, etc.)
      if (sqlType.startsWith('Array(')) {
        return 'String'; // Arrays are typically passed as strings
      }
      if (sqlType.startsWith('Map(')) {
        return 'String'; // Maps are typically passed as strings
      }
      if (sqlType.startsWith('Tuple(')) {
        return 'String'; // Tuples are typically passed as strings
      }
      if (sqlType.startsWith('Nested(')) {
        return 'String'; // Nested objects are typically passed as strings
      }
      if (sqlType.startsWith('LowCardinality(')) {
        return 'String'; // LowCardinality is typically string-based
      }
      if (sqlType.startsWith('Nullable(')) {
        // Extract inner type and recurse
        const innerType = sqlType.slice(9, -1); // Remove "Nullable(" and ")"
        return getTinybirdTemplateType(innerType);
      }

      // Fallback to String for unknown types
      return 'String';
  }
}

// Format default values for different types
function formatDefaultValue(defaultValue: any, sqlType: string): string {
  if (defaultValue === null || defaultValue === undefined) {
    return 'null';
  }

  switch (sqlType) {
    case 'String':
    case 'UUID':
    case 'IPv4':
    case 'IPv6':
    case 'JSON':
      return `'${String(defaultValue)}'`;
    case 'DateTime':
    case 'DateTime64':
    case 'Date':
      if (defaultValue instanceof Date) {
        return `'${defaultValue.toISOString()}'`;
      }
      return `'${String(defaultValue)}'`;
    case 'Boolean':
      return defaultValue ? 'true' : 'false';
    default:
      // Numbers and other types
      return String(defaultValue);
  }
}

// Helper to extract dependencies from pipes
export function extractPipeDependencies<TParams extends QueryParameters>(
  pipe: PipeConfig<TParams>
): string[] {
  const dependencies: string[] = [];

  // Generate sample SQL to analyze for table references
  const sampleParams = generateSampleParams(pipe.parameters);
  const sql = pipe.sql(sampleParams);

  // Simple regex to find table references in FROM and JOIN clauses
  // This is a basic implementation - could be enhanced with proper SQL parsing
  const tableMatches = sql.match(/FROM\s+(\w+)/gi) || [];
  const joinMatches = sql.match(/JOIN\s+(\w+)/gi) || [];

  [...tableMatches, ...joinMatches].forEach((match) => {
    const tableName = match.split(/\s+/)[1];
    if (tableName && !dependencies.includes(tableName)) {
      dependencies.push(tableName);
    }
  });

  return dependencies;
}
