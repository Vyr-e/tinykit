import { expect, test, describe } from 'bun:test';
import { z } from 'zod';
import {
  defineSchema,
  defineDataSource,
  generateCreateTableSQL,
  tuple,
  nested,
  lowCardinality,
  nullable,
  json,
  ipv4,
  ipv6,
  array,
  map,
  string,
} from '../src';

describe('Advanced Data Types', () => {
  describe('Tuple Type', () => {
    test('should create tuple column definition', () => {
      const coordinates = tuple(
        'coordinates',
        z.tuple([z.number(), z.number()]),
        { types: ['Float64', 'Float64'] }
      );

      expect(coordinates.name).toBe('coordinates');
      expect(coordinates.type).toBe('Tuple(Float64, Float64)');
      expect(coordinates.schema).toBeInstanceOf(z.ZodTuple);
    });

    test('should use default tuple type when not specified', () => {
      const defaultTuple = tuple('default_tuple', z.tuple([z.string(), z.string()]));
      expect(defaultTuple.type).toBe('Tuple(String, String)');
    });

    test('should validate tuple data correctly', () => {
      const coordinates = tuple(
        'coordinates',
        z.tuple([z.number(), z.number()]),
        { types: ['Float64', 'Float64'] }
      );

      const validData = [10.5, 20.3];
      const invalidData = [10.5, '20.3'];

      expect(() => coordinates.schema.parse(validData)).not.toThrow();
      expect(() => coordinates.schema.parse(invalidData)).toThrow();
    });
  });

  describe('Nested Type', () => {
    test('should create nested column definition', () => {
      const userInfo = nested(
        'user_info',
        z.object({
          name: z.string(),
          age: z.number(),
        }),
        { fields: 'name String, age Int32' }
      );

      expect(userInfo.name).toBe('user_info');
      expect(userInfo.type).toBe('Nested(name String, age Int32)');
      expect(userInfo.schema).toBeInstanceOf(z.ZodObject);
    });

    test('should use default nested type when not specified', () => {
      const defaultNested = nested('default_nested', z.object({ field: z.string() }));
      expect(defaultNested.type).toBe('Nested(field String)');
    });

    test('should validate nested data correctly', () => {
      const userInfo = nested(
        'user_info',
        z.object({
          name: z.string(),
          age: z.number(),
        })
      );

      const validData = { name: 'John', age: 30 };
      const invalidData = { name: 'John', age: '30' };

      expect(() => userInfo.schema.parse(validData)).not.toThrow();
      expect(() => userInfo.schema.parse(invalidData)).toThrow();
    });
  });

  describe('LowCardinality Type', () => {
    test('should create low cardinality column definition', () => {
      const status = lowCardinality(
        'status',
        z.enum(['active', 'inactive']),
        { innerType: 'String' }
      );

      expect(status.name).toBe('status');
      expect(status.type).toBe('LowCardinality(String)');
      expect(status.schema).toBeInstanceOf(z.ZodEnum);
    });

    test('should validate low cardinality data correctly', () => {
      const status = lowCardinality('status', z.enum(['active', 'inactive']));
      
      expect(() => status.schema.parse('active')).not.toThrow();
      expect(() => status.schema.parse('inactive')).not.toThrow();
      expect(() => status.schema.parse('unknown')).toThrow();
    });
  });

  describe('Nullable Type', () => {
    test('should create nullable column definition', () => {
      const optionalName = nullable(
        'optional_name',
        z.string(),
        { innerType: 'String' }
      );

      expect(optionalName.name).toBe('optional_name');
      expect(optionalName.type).toBe('Nullable(String)');
      expect(optionalName.nullable).toBe(true);
    });

    test('should validate nullable data correctly', () => {
      const optionalName = nullable('optional_name', z.string());
      
      expect(() => optionalName.schema.parse('John')).not.toThrow();
      expect(() => optionalName.schema.parse(null)).not.toThrow();
      expect(() => optionalName.schema.parse(123)).toThrow();
    });
  });

  describe('JSON Type', () => {
    test('should create JSON column definition', () => {
      const metadata = json('metadata');

      expect(metadata.name).toBe('metadata');
      expect(metadata.type).toBe('JSON');
      expect(metadata.schema).toBeInstanceOf(z.ZodAny);
    });

    test('should accept any JSON data', () => {
      const metadata = json('metadata');
      
      expect(() => metadata.schema.parse({ key: 'value' })).not.toThrow();
      expect(() => metadata.schema.parse(['a', 'b', 'c'])).not.toThrow();
      expect(() => metadata.schema.parse('string')).not.toThrow();
      expect(() => metadata.schema.parse(123)).not.toThrow();
      expect(() => metadata.schema.parse(null)).not.toThrow();
    });
  });

  describe('IP Address Types', () => {
    test('should create IPv4 column definition', () => {
      const clientIp = ipv4('client_ip');

      expect(clientIp.name).toBe('client_ip');
      expect(clientIp.type).toBe('IPv4');
      expect(clientIp.schema).toBeInstanceOf(z.ZodString);
    });

    test('should create IPv6 column definition', () => {
      const clientIpv6 = ipv6('client_ipv6');

      expect(clientIpv6.name).toBe('client_ipv6');
      expect(clientIpv6.type).toBe('IPv6');
      expect(clientIpv6.schema).toBeInstanceOf(z.ZodString);
    });

    test('should validate IPv4 addresses', () => {
      const clientIp = ipv4('client_ip');
      
      expect(() => clientIp.schema.parse('192.168.1.1')).not.toThrow();
      expect(() => clientIp.schema.parse('255.255.255.255')).not.toThrow();
      expect(() => clientIp.schema.parse('0.0.0.0')).not.toThrow();
      expect(() => clientIp.schema.parse('invalid-ip')).toThrow();
      expect(() => clientIp.schema.parse('256.256.256.256')).toThrow();
      expect(() => clientIp.schema.parse('::1')).toThrow(); // IPv6 should fail IPv4 validation
    });

    test('should validate IPv6 addresses', () => {
      const clientIpv6 = ipv6('client_ipv6');
      
      expect(() => clientIpv6.schema.parse('::1')).not.toThrow();
      expect(() => clientIpv6.schema.parse('2001:0db8:85a3:0000:0000:8a2e:0370:7334')).not.toThrow();
      expect(() => clientIpv6.schema.parse('2001:db8::8a2e:370:7334')).not.toThrow();
      expect(() => clientIpv6.schema.parse('invalid-ipv6')).toThrow();
      expect(() => clientIpv6.schema.parse('192.168.1.1')).toThrow(); // IPv4 should fail IPv6 validation
    });
  });

  describe('Enhanced Array and Map Types', () => {
    test('should create array with specific inner type', () => {
      const tags = array('tags', z.string(), { innerType: 'String' });
      const scores = array('scores', z.number(), { innerType: 'Float64' });

      expect(tags.type).toBe('Array(String)');
      expect(scores.type).toBe('Array(Float64)');
    });

    test('should create map with specific key and value types', () => {
      const stringToInt = map(
        'counters',
        z.string(),
        z.number(),
        { keyType: 'String', valueType: 'Int64' }
      );

      expect(stringToInt.type).toBe('Map(String, Int64)');
    });
  });

  describe('SQL Generation with Advanced Types', () => {
    test('should generate correct SQL for all advanced types', () => {
      const schema = defineSchema({
        id: string('id'),
        coordinates: tuple('coordinates', z.tuple([z.number(), z.number()]), {
          types: ['Float64', 'Float64']
        }),
        user_info: nested('user_info', z.object({
          name: z.string(),
          age: z.number(),
        }), { fields: 'name String, age Int32' }),
        status: lowCardinality('status', z.enum(['active', 'inactive']), {
          innerType: 'String'
        }),
        optional_field: nullable('optional_field', z.string(), {
          innerType: 'String'
        }),
        metadata: json('metadata'),
        client_ip: ipv4('client_ip'),
        client_ipv6: ipv6('client_ipv6'),
        tags: array('tags', z.string(), { innerType: 'String' }),
        counters: map('counters', z.string(), z.number(), {
          keyType: 'String',
          valueType: 'Int64'
        }),
      });

      const dataSource = defineDataSource({
        name: 'advanced_types_table',
        schema,
        engine: 'MergeTree',
      });

      const sql = generateCreateTableSQL(dataSource);

      expect(sql).toContain('`id` String');
      expect(sql).toContain('`coordinates` Tuple(Float64, Float64)');
      expect(sql).toContain('`user_info` Nested(name String, age Int32)');
      expect(sql).toContain('`status` LowCardinality(String)');
      expect(sql).toContain('`optional_field` Nullable(String)');
      expect(sql).toContain('`metadata` JSON');
      expect(sql).toContain('`client_ip` IPv4');
      expect(sql).toContain('`client_ipv6` IPv6');
      expect(sql).toContain('`tags` Array(String)');
      expect(sql).toContain('`counters` Map(String, Int64)');
    });

    test('should handle comments and JSON paths with advanced types', () => {
      const schema = defineSchema({
        coordinates: tuple(
          'coordinates',
          z.tuple([z.number(), z.number()]),
          {
            types: ['Float64', 'Float64'],
            jsonPath: '$.location',
            comment: 'Geographic coordinates (lat, lng)'
          }
        ),
        metadata: json('metadata', {
          jsonPath: '$.meta',
          comment: 'Additional metadata as JSON'
        }),
      });

      const dataSource = defineDataSource({
        name: 'geo_table',
        schema,
        engine: 'MergeTree',
      });

      const sql = generateCreateTableSQL(dataSource);

      expect(sql).toContain('`json:$.location`');
      expect(sql).toContain('`json:$.meta`');
      expect(sql).toContain('# Geographic coordinates (lat, lng)');
      expect(sql).toContain('# Additional metadata as JSON');
    });
  });

  describe('Type Combinations and Edge Cases', () => {
    test('should handle nullable arrays', () => {
      const nullableArray = nullable(
        'optional_tags',
        z.array(z.string()).nullable(),
        { innerType: 'Array(String)' }
      );

      expect(nullableArray.type).toBe('Nullable(Array(String))');
      expect(() => nullableArray.schema.parse(['a', 'b'])).not.toThrow();
      expect(() => nullableArray.schema.parse(null)).not.toThrow();
    });

    test('should handle low cardinality nullable strings', () => {
      const optionalStatus = nullable(
        'optional_status',
        z.enum(['active', 'inactive']).nullable(),
        { innerType: 'LowCardinality(String)' }
      );

      expect(optionalStatus.type).toBe('Nullable(LowCardinality(String))');
      expect(() => optionalStatus.schema.parse('active')).not.toThrow();
      expect(() => optionalStatus.schema.parse(null)).not.toThrow();
    });

    test('should handle complex nested structures', () => {
      const complexNested = nested(
        'user_sessions',
        z.object({
          user_id: z.string(),
          sessions: z.array(z.object({
            id: z.string(),
            duration: z.number(),
            pages: z.array(z.string())
          }))
        }),
        {
          fields: 'user_id String, sessions Array(Nested(id String, duration Int64, pages Array(String)))'
        }
      );

      expect(complexNested.type).toBe('Nested(user_id String, sessions Array(Nested(id String, duration Int64, pages Array(String))))');
    });
  });
});