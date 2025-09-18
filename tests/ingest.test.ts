import { expect, test, describe } from 'bun:test';
import { z } from 'zod';
import {
  defineIngest,
  defineSchema,
  string,
  nullable,
  streamingIngest,
  syncIngest,
  batchIngest,
  robustIngest,
  createIngestionReport,
  handleIngestionErrors,
  type IngestError,
} from '../src';

describe('Enhanced Ingestion System', () => {
  const testSchema = defineSchema({
    id: string('id'),
    name: nullable('name', z.string(), { innerType: 'String' }),
    age: nullable('age', z.string(), { innerType: 'String' }), // Allow nulls for CSV parsing tests
    email: string('email'),
  });

  const sampleEvents = [
    { id: '1', name: 'John', age: '30', email: 'john@example.com' },
    { id: '2', name: 'Jane', age: '25', email: 'jane@example.com' },
    { id: '3', name: 'Bob', age: '35', email: 'bob@example.com' },
  ];

  const invalidEvents = [
    { id: '1', name: 'John', age: 30, email: 'john@example.com' }, // age should be string
    { id: '2', name: 'Jane', age: '25' }, // missing email
    { id: '', name: 'Bob', age: '35', email: 'not-an-email' }, // empty id
  ];

  describe('Basic Ingestion', () => {
    test('should create ingestion definition', () => {
      const ingestDef = defineIngest({
        datasource: 'test_table',
        schema: testSchema,
      });

      expect(ingestDef.datasource).toBe('test_table');
      expect(ingestDef.schema).toBe(testSchema);
      expect(typeof ingestDef.validateEvent).toBe('function');
      expect(typeof ingestDef.validateEvents).toBe('function');
    });

    test('should validate single event successfully', () => {
      const ingestDef = defineIngest({
        datasource: 'test_table',
        schema: testSchema,
      });

      const validEvent = ingestDef.validateEvent(sampleEvents[0]);
      expect(validEvent.id).toBe('1');
      expect(validEvent.name).toBe('John');
      expect(validEvent.age).toBe('30');
    });

    test('should validate multiple events and separate valid/invalid', () => {
      const ingestDef = defineIngest({
        datasource: 'test_table',
        schema: testSchema,
      });

      const result = ingestDef.validateEvents(sampleEvents);
      expect(result.validEvents).toHaveLength(3);
      expect(result.errors).toHaveLength(0);
    });

    test('should collect validation errors', () => {
      const ingestDef = defineIngest({
        datasource: 'test_table',
        schema: testSchema,
      });

      const result = ingestDef.validateEvents(invalidEvents);
      expect(result.validEvents.length).toBeLessThan(invalidEvents.length);
      expect(result.errors.length).toBeGreaterThan(0);
      
      // Check error structure
      const error = result.errors[0];
      expect(error).toBeDefined();
      if (error) {
        expect(error.error_code).toBe('VALIDATION_ERROR');
        expect(error.error_message).toBeDefined();
        expect(error.row_number).toBeGreaterThan(0);
      }
    });
  });

  describe('CSV Parsing', () => {
    test('should parse simple CSV with headers', () => {
      const ingestDef = defineIngest({
        datasource: 'test_table',
        schema: testSchema,
      });

      const csvData = `id,name,age,email
1,John,30,john@example.com
2,Jane,25,jane@example.com`;

      const result = ingestDef.parseCSV(csvData);
      expect(result.validEvents).toHaveLength(2);
      expect(result.errors).toHaveLength(0);
      const firstEvent = result.validEvents[0];
      expect(firstEvent).toBeDefined();
      if (firstEvent) {
        expect(firstEvent.id).toBe('1');
        expect(firstEvent.name).toBe('John');
      }
    });

    test('should handle quoted CSV fields', () => {
      const ingestDef = defineIngest({
        datasource: 'test_table',
        schema: testSchema,
      });

      const csvData = `id,name,age,email
1,"John, Jr.",30,"john@example.com"
2,"Jane ""The Great""",25,"jane@example.com"`;

      const result = ingestDef.parseCSV(csvData);
      expect(result.validEvents).toHaveLength(2);
      const firstEvent = result.validEvents[0];
      const secondEvent = result.validEvents[1];
      expect(firstEvent).toBeDefined();
      expect(secondEvent).toBeDefined();
      if (firstEvent && secondEvent) {
        expect(firstEvent.name).toBe('John, Jr.');
        expect(secondEvent.name).toBe('Jane "The Great"');
      }
    });

    test('should handle custom delimiters and quotes', () => {
      const ingestDef = defineIngest({
        datasource: 'test_table',
        schema: testSchema,
      });

      const csvData = `id|name|age|email
1|'John'|30|'john@example.com'
2|'Jane'|25|'jane@example.com'`;

      const result = ingestDef.parseCSV(csvData, {
        delimiter: '|',
        quote: "'",
      });
      expect(result.validEvents).toHaveLength(2);
      const firstEvent = result.validEvents[0];
      expect(firstEvent).toBeDefined();
      if (firstEvent) {
        expect(firstEvent.name).toBe('John');
      }
    });

    test('should handle null values in CSV', () => {
      const ingestDef = defineIngest({
        datasource: 'test_table',
        schema: testSchema,
      });

      const csvData = `id,name,age,email
1,John,30,john@example.com
2,Jane,NULL,jane@example.com
3,Bob,35,bob@example.com`;

      const result = ingestDef.parseCSV(csvData, {
        null_values: ['', 'NULL', 'null'],
      });
      
      expect(result.validEvents).toHaveLength(3); // All rows should be valid with nullable schema
      expect(result.errors).toHaveLength(0); // No validation errors
      const events = result.validEvents;
      expect(events[0]).toBeDefined();
      expect(events[1]).toBeDefined();
      expect(events[2]).toBeDefined();
      if (events[0] && events[1] && events[2]) {
        expect(events[0].name).toBe('John');
        expect(events[1].name).toBe('Jane');
        expect(events[2].name).toBe('Bob');
      }
      
      // Verify null handling worked correctly
      const recordWithNullAge = result.validEvents.find(e => e.age === null);
      expect(recordWithNullAge).toBeDefined();
      if (recordWithNullAge) {
        expect(recordWithNullAge.age).toBeNull();
        expect(recordWithNullAge.name).toBe('Jane');
      }
    });

    test('should handle CSV without headers', () => {
      const ingestDef = defineIngest({
        datasource: 'test_table',
        schema: testSchema,
      });

      const csvData = `1,John,30,john@example.com
2,Jane,25,jane@example.com`;

      const result = ingestDef.parseCSV(csvData, { skip_header: false });
      expect(result.validEvents).toHaveLength(2);
      const firstEvent = result.validEvents[0];
      expect(firstEvent).toBeDefined();
      if (firstEvent) {
        expect(firstEvent.id).toBe('1');
      }
    });
  });

  describe('Error Handling and Reporting', () => {
    test('should create detailed ingestion report', () => {
      const mockResult = {
        validEvents: sampleEvents,
        errors: [
          {
            row_number: 1,
            error_code: 'VALIDATION_ERROR',
            error_message: 'Invalid age field',
            field: 'age',
            value: '30'
          } as IngestError,
          {
            row_number: 2,
            error_code: 'VALIDATION_ERROR',
            error_message: 'Missing email field',
            field: 'email'
          } as IngestError,
        ],
      };

      const report = createIngestionReport(mockResult);
      expect(report.summary).toContain('5 rows');
      expect(report.summary).toContain('3 valid');
      expect(report.summary).toContain('2 errors');
      expect(report.hasErrors).toBe(true);
      expect(report.errorsByType['VALIDATION_ERROR']).toBe(2);
      expect(report.detailedErrors).toHaveLength(2);
    });

    test('should handle ingestion errors with logging disabled', () => {
      const errors: IngestError[] = [
        {
          row_number: 1,
          error_code: 'VALIDATION_ERROR',
          error_message: 'Test error',
          field: 'test_field',
        },
      ];

      // Should not throw when logErrors is false and throwOnErrors is false
      expect(() => {
        handleIngestionErrors(errors, {
          logErrors: false,
          throwOnErrors: false,
        });
      }).not.toThrow();
    });

    test('should throw when throwOnErrors is true', () => {
      const errors: IngestError[] = [
        {
          row_number: 1,
          error_code: 'VALIDATION_ERROR',
          error_message: 'Test error',
        },
      ];

      expect(() => {
        handleIngestionErrors(errors, {
          throwOnErrors: true,
        });
      }).toThrow('Ingestion failed with 1 errors');
    });

    test('should handle empty errors array', () => {
      expect(() => {
        handleIngestionErrors([], {
          logErrors: true,
          throwOnErrors: true,
        });
      }).not.toThrow();
    });
  });

  describe('Ingestion Patterns', () => {
    test('should create streaming ingestion', () => {
      const ingestDef = streamingIngest({
        datasource: 'test_table',
        schema: testSchema,
      });

      expect(ingestDef.options.wait).toBe(false);
    });

    test('should create sync ingestion', () => {
      const ingestDef = syncIngest({
        datasource: 'test_table',
        schema: testSchema,
      });

      expect(ingestDef.options.wait).toBe(true);
    });

    test('should create batch ingestion', () => {
      const ingestDef = batchIngest({
        datasource: 'test_table',
        schema: testSchema,
        batchSize: 1000,
      });

      expect(ingestDef.options.batchSize).toBe(1000);
    });

    test('should create robust ingestion with error handling', async () => {
      const robustIngestDef = robustIngest({
        datasource: 'test_table',
        schema: testSchema,
        continueOnError: true,
        onError: (errors) => {
          expect(errors).toBeDefined();
        },
      });

      const result = await robustIngestDef.ingestWithErrorHandling(invalidEvents);
      expect(result.validEvents).toBeDefined();
      expect(result.report).toBeDefined();
      expect(result.report.hasErrors).toBe(true);
    });

    test('should throw on validation errors when continueOnError is false', async () => {
      const robustIngestDef = robustIngest({
        datasource: 'test_table',
        schema: testSchema,
        continueOnError: false,
      });

      await expect(
        robustIngestDef.ingestWithErrorHandling(invalidEvents)
      ).rejects.toThrow('Ingestion validation failed');
    });
  });

  describe('Data Transformation', () => {
    test('should transform data with error collection', () => {
      const ingestDef = defineIngest({
        datasource: 'test_table',
        schema: testSchema,
      });

      const result = ingestDef.transform(sampleEvents, (event, index) => ({
        ...event,
        fullName: `${event.name} (${event.id})`,
        index,
      }));

      expect(result.transformedEvents).toHaveLength(3);
      const firstTransformed = result.transformedEvents[0];
      expect(firstTransformed).toBeDefined();
      if (firstTransformed) {
        expect(firstTransformed.fullName).toBe('John (1)');
        expect(firstTransformed.index).toBe(0);
      }
      expect(result.errors).toHaveLength(0);
    });

    test('should collect transformation errors', () => {
      const ingestDef = defineIngest({
        datasource: 'test_table',
        schema: testSchema,
      });

      const result = ingestDef.transform(sampleEvents, (event) => {
        if (event.name === 'Jane') {
          throw new Error('Transformation failed for Jane');
        }
        return { ...event, processed: true };
      });

      expect(result.transformedEvents).toHaveLength(2); // John and Bob
      expect(result.errors).toHaveLength(1);
      const firstError = result.errors[0];
      expect(firstError).toBeDefined();
      if (firstError) {
        expect(firstError.error_code).toBe('TRANSFORM_ERROR');
        expect(firstError.error_message).toContain('Jane');
      }
    });
  });
});