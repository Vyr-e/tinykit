import { z } from 'zod';
import type { SchemaDefinition, InferSchemaType } from './types';

export type IngestConfig<T extends SchemaDefinition> = {
  datasource: string;
  schema: T;
  format?: 'NDJSON' | 'CSV' | 'Parquet';
  options?: {
    wait?: boolean;
    batchSize?: number;
  };
};

export type IngestResult = {
  successful_rows: number;
  quarantined_rows: number;
  bytes_ingested?: number;
  ingestion_time?: number;
  errors?: IngestError[];
};

export type IngestError = {
  row_number?: number;
  error_code: string;
  error_message: string;
  field?: string;
  value?: any;
};

export type CSVIngestOptions = {
  delimiter?: string;
  quote?: string;
  escape?: string;
  skip_header?: boolean;
  encoding?: 'utf8' | 'latin1' | 'ascii';
  null_values?: string[];
};

export type ParquetIngestOptions = {
  compression?: 'snappy' | 'gzip' | 'lz4';
  row_group_size?: number;
};

// Helper to create Zod object schema from our schema definition
type CreateZodSchema<T extends SchemaDefinition> = z.ZodObject<{
  [K in keyof T]: T[K]['schema'];
}>;

/**
 * Defines an ingestion configuration for a Tinybird DataSource.
 *
 * @param config The ingestion configuration.
 * @returns An object with the ingestion configuration and helper functions.
 *
 * @example
 * ```
 * import { defineIngest, string } from '@tinybird/sdk';
 *
 * const myIngest = defineIngest({
 *   datasource: 'my_datasource',
 *   schema: {
 *     id: string('id'),
 *     name: string('name'),
 *   },
 * });
 * ```
 */
export function defineIngest<T extends SchemaDefinition>(config: IngestConfig<T>) {
  // Create Zod schema from our schema definition with proper type inference
  const zodSchema = (() => {
    const schemaObj = {} as { [K in keyof T]: T[K]['schema'] };

    for (const [key, columnDef] of Object.entries(config.schema)) {
      schemaObj[key as keyof T] = columnDef.schema;
    }

    return z.object(schemaObj) as CreateZodSchema<T>;
  })();

  return {
    datasource: config.datasource,
    schema: config.schema,
    options: config.options || {},
    zodSchema,

    // Validate a single event with proper typing
    validateEvent(event: unknown): InferSchemaType<T> {
      return zodSchema.parse(event) as InferSchemaType<T>;
    },

    // Validate multiple events with proper typing and collect errors
    validateEvents(events: unknown[]): { 
      validEvents: InferSchemaType<T>[], 
      errors: IngestError[] 
    } {
      const validEvents: InferSchemaType<T>[] = [];
      const errors: IngestError[] = [];

      events.forEach((event, index) => {
        try {
          const validEvent = zodSchema.parse(event) as InferSchemaType<T>;
          validEvents.push(validEvent);
        } catch (error) {
          if (error instanceof z.ZodError) {
            error.issues.forEach(issue => {
              errors.push({
                row_number: index + 1,
                error_code: 'VALIDATION_ERROR',
                error_message: issue.message,
                field: issue.path.join('.'),
                value: (issue as any).received
              });
            });
          } else {
            errors.push({
              row_number: index + 1,
              error_code: 'UNKNOWN_ERROR',
              error_message: error instanceof Error ? error.message : String(error)
            });
          }
        }
      });

      return { validEvents, errors };
    },

    // Parse CSV data with error handling
    parseCSV(csvData: string, options: CSVIngestOptions = {}): { 
      validEvents: InferSchemaType<T>[], 
      errors: IngestError[] 
    } {
      const {
        delimiter = ',',
        quote = '"',
        skip_header = true,
        null_values = ['', 'NULL', 'null', 'N/A']
      } = options;

      try {
        const lines = csvData.trim().split('\n');
        let dataLines = lines;
        let headers: string[] = [];

        if (skip_header && lines.length > 0) {
          headers = parseCSVLine(lines[0] || '', delimiter, quote);
          dataLines = lines.slice(1);
        } else {
          // Use schema keys as headers if no header row
          headers = Object.keys(this.schema);
        }

        const rawEvents = dataLines.map((line, index) => {
          const values = parseCSVLine(line, delimiter, quote);
          const event: Record<string, any> = {};
          
          headers.forEach((header, i) => {
            const value = values[i];
            if (value && null_values.includes(value)) {
              event[header] = null;
            } else {
              event[header] = value;
            }
          });

          return event;
        });

        return this.validateEvents(rawEvents);
      } catch (error) {
        return {
          validEvents: [],
          errors: [{
            error_code: 'CSV_PARSE_ERROR',
            error_message: error instanceof Error ? error.message : String(error)
          }]
        };
      }
    },


    // Transform data for ingestion with error collection
    transform<R>(
      events: unknown[], 
      transformFn: (event: InferSchemaType<T>, index: number) => R
    ): { transformedEvents: R[], errors: IngestError[] } {
      const { validEvents, errors } = this.validateEvents(events);
      const transformedEvents: R[] = [];
      const transformErrors: IngestError[] = [...errors];

      validEvents.forEach((event, index) => {
        try {
          const transformed = transformFn(event, index);
          transformedEvents.push(transformed);
        } catch (error) {
          transformErrors.push({
            row_number: index + 1,
            error_code: 'TRANSFORM_ERROR',
            error_message: error instanceof Error ? error.message : String(error)
          });
        }
      });

      return { transformedEvents, errors: transformErrors };
    }
  };
}

export type IngestDefinition<T extends SchemaDefinition> = ReturnType<typeof defineIngest<T>>;

/**
 * Creates a streaming ingestion configuration.
 *
 * @example
 * ```
 * const myStreamingIngest = streamingIngest({
 *   datasource: 'my_datasource',
 *   schema: mySchema,
 * });
 * ```
 */
export const streamingIngest = <T extends SchemaDefinition>(config: {
  datasource: string;
  schema: T;
}) => defineIngest({
  ...config,
  options: { wait: false },
});

/**
 * Creates a synchronous ingestion configuration.
 *
 * @example
 * ```
 * const mySyncIngest = syncIngest({
 *   datasource: 'my_datasource',
 *   schema: mySchema,
 * });
 * ```
 */
export const syncIngest = <T extends SchemaDefinition>(config: {
  datasource: string;
  schema: T;
}) => defineIngest({
  ...config,
  options: { wait: true },
});

/**
 * Creates a batch ingestion configuration.
 *
 * @example
 * ```
 * const myBatchIngest = batchIngest({
 *   datasource: 'my_datasource',
 *   schema: mySchema,
 *   batchSize: 100,
 * });
 * ```
 */
export const batchIngest = <T extends SchemaDefinition>(config: {
  datasource: string;
  schema: T;
  batchSize: number;
}) => defineIngest({
  ...config,
  options: { batchSize: config.batchSize },
});

/**
 * Creates a report of the ingestion result.
 *
 * @example
 * ```
 * const result = myIngest.validateEvents(events);
 * const report = createIngestionReport(result);
 * console.log(report.summary);
 * ```
 */
export function createIngestionReport(result: { validEvents: any[], errors: IngestError[] }): {
  summary: string;
  hasErrors: boolean;
  errorsByType: Record<string, number>;
  detailedErrors: IngestError[];
} {
  const errorsByType = result.errors.reduce((acc, error) => {
    acc[error.error_code] = (acc[error.error_code] || 0) + 1;
    return acc;
  }, {} as Record<string, number>);

  const hasErrors = result.errors.length > 0;
  const totalRows = result.validEvents.length + result.errors.length;
  const summary = `Processed ${totalRows} rows: ${result.validEvents.length} valid, ${result.errors.length} errors`;

  return {
    summary,
    hasErrors,
    errorsByType,
    detailedErrors: result.errors
  };
}

/**
 * Handles ingestion errors by logging them and optionally throwing an error.
 *
 * @example
 * ```
 * const result = myIngest.validateEvents(events);
 * if (result.errors.length > 0) {
 *   handleIngestionErrors(result.errors, { throwOnErrors: true });
 * }
 * ```
 */
export function handleIngestionErrors(
  errors: IngestError[], 
  options: {
    logErrors?: boolean;
    throwOnErrors?: boolean;
    maxErrorsToLog?: number;
  } = {}
): void {
  const { logErrors = true, throwOnErrors = false, maxErrorsToLog = 10 } = options;

  if (errors.length === 0) return;

  if (logErrors) {
    console.warn(`Found ${errors.length} ingestion errors:`);
    const errorsToLog = errors.slice(0, maxErrorsToLog);
    
    errorsToLog.forEach(error => {
      const rowInfo = error.row_number ? ` (row ${error.row_number})` : '';
      const fieldInfo = error.field ? ` in field '${error.field}'` : '';
      console.warn(`  ${error.error_code}${rowInfo}${fieldInfo}: ${error.error_message}`);
      
      if (error.value !== undefined) {
        console.warn(`    Received value: ${JSON.stringify(error.value)}`);
      }
    });
    
    if (errors.length > maxErrorsToLog) {
      console.warn(`  ... and ${errors.length - maxErrorsToLog} more errors`);
    }
  }

  if (throwOnErrors) {
    throw new Error(`Ingestion failed with ${errors.length} errors`);
  }
}

/**
 * Creates a robust ingestion configuration with error handling.
 *
 * @example
 * ```
 * const myRobustIngest = robustIngest({
 *   datasource: 'my_datasource',
 *   schema: mySchema,
 *   onError: (errors) => console.error(errors),
 * });
 * ```
 */
export const robustIngest = <T extends SchemaDefinition>(config: {
  datasource: string;
  schema: T;
  onError?: (errors: IngestError[]) => void;
  continueOnError?: boolean;
}) => {
  const ingestDef = defineIngest({
    datasource: config.datasource,
    schema: config.schema
  });

  return {
    ...ingestDef,
    
    async ingestWithErrorHandling(
      events: unknown[]
    ): Promise<{ 
      validEvents: InferSchemaType<T>[], 
      report: ReturnType<typeof createIngestionReport> 
    }> {
      const result = ingestDef.validateEvents(events);
      const report = createIngestionReport(result);
      
      if (config.onError && result.errors.length > 0) {
        config.onError(result.errors);
      }
      
      if (!config.continueOnError && result.errors.length > 0) {
        throw new Error(`Ingestion validation failed: ${report.summary}`);
      }
      
      return { validEvents: result.validEvents, report };
    }
  };
};

/**
 * @internal
 */
function parseCSVLine(line: string, delimiter: string, quote: string): string[] {
  const result: string[] = [];
  let current = '';
  let inQuotes = false;
  
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    const nextChar = line[i + 1];
    
    if (char === quote) {
      if (inQuotes && nextChar === quote) {
        current += quote;
        i++; // Skip next quote
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === delimiter && !inQuotes) {
      result.push(current);
      current = '';
    } else {
      current += char;
    }
  }
  
  result.push(current);
  return result;
}
