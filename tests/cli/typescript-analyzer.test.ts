import { expect, test, describe, beforeAll, afterAll } from 'bun:test';
import { writeFileSync, mkdirSync, rmSync } from 'fs';
import { join } from 'path';
import { analyzeTypeScriptFile, validateNaming } from '../../src/cli/utils/typescript-analyzer';

const testDir = join(__dirname, 'test-fixtures');

beforeAll(() => {
  // Create test directory
  mkdirSync(testDir, { recursive: true });
});

afterAll(() => {
  // Clean up test directory
  rmSync(testDir, { recursive: true, force: true });
});

describe('TypeScript Analyzer', () => {
  test('should discover Tinybird client with datasources and pipes', async () => {
    const testFile = join(testDir, 'client-with-resources.ts');
    
    const content = `
import { Tinybird } from '../../../src/client';
import { defineDataSource, defineSchema, string, int64 } from '../../../src/schema';
import { definePipe, stringParam, int64Param } from '../../../src/pipe';

const eventsSchema = defineSchema({
  id: string('id'),
  tenantId: string('tenantId'),
  time: int64('time'),
  event: string('event'),
});

const eventsDataSource = defineDataSource({
  name: 'events__v1',
  schema: eventsSchema,
  engine: 'MergeTree',
  sortingKey: ['tenantId', 'time']
});

const getUserActivityPipe = definePipe({
  name: 'get_user_activity__v1',
  schema: eventsSchema,
  parameters: {
    tenantId: stringParam('tenantId', { required: true }),
    limit: int64Param('limit', { default: 100 })
  }
}).endpoint((q, params) =>
  q.select('id', 'event')
   .where(\`tenantId = \${params.tenantId}\`)
   .limit(params.limit)
);

export const tb = new Tinybird({
  token: process.env.TINYBIRD_TOKEN!,
  datasources: {
    userEvents: eventsDataSource
  },
  pipes: {
    getUserActivity: getUserActivityPipe
  }
});
`;

    writeFileSync(testFile, content);
    
    const result = await analyzeTypeScriptFile(testFile);
    
    expect(result.errors).toHaveLength(0);
    expect(result.tinybirdClients).toHaveLength(1);
    
    const client = result.tinybirdClients[0];
    expect(client).toBeDefined();
    if (client) {
      expect(client.exportName).toBe('tb');
      expect(Object.keys(client.datasources)).toHaveLength(1);
      expect(Object.keys(client.pipes)).toHaveLength(1);
    }
    
    expect(result.datasources).toHaveLength(1);
    const datasource = result.datasources[0];
    expect(datasource).toBeDefined();
    if (datasource) {
      expect(datasource.name).toBe('events__v1');
      expect(datasource.exportName).toBe('tb.datasources.userEvents');
    }
    
    expect(result.pipes).toHaveLength(1);
    const pipe = result.pipes[0];
    expect(pipe).toBeDefined();
    if (pipe) {
      expect(pipe.name).toBe('get_user_activity__v1');
      expect(pipe.exportName).toBe('tb.pipes.getUserActivity');
    }
  });

  test('should discover standalone datasources and pipes', async () => {
    const testFile = join(testDir, 'standalone-resources.ts');
    
    const content = `
import { defineDataSource, definePipe, defineSchema, string, int64, stringParam } from '../../../src/index';

const schema = defineSchema({
  id: string('id'),
  name: string('name')
});

export const myDataSource = defineDataSource({
  name: 'my_table__v1',
  schema,
  engine: 'MergeTree'
});

export const myPipe = definePipe({
  name: 'my_pipe__v1',
  schema,
  parameters: {
    id: stringParam('id', { required: true })
  }
}).endpoint((q, params) =>
  q.select('*').where(\`id = \${params.id}\`)
);
`;

    writeFileSync(testFile, content);
    
    const result = await analyzeTypeScriptFile(testFile);
    
    expect(result.errors).toHaveLength(0);
    expect(result.tinybirdClients).toHaveLength(0);
    expect(result.datasources).toHaveLength(1);
    expect(result.pipes).toHaveLength(1);
    
    const datasource = result.datasources[0];
    const pipe = result.pipes[0];
    
    expect(datasource).toBeDefined();
    expect(pipe).toBeDefined();
    
    if (datasource) {
      expect(datasource.name).toBe('my_table__v1');
      expect(datasource.exportName).toBe('myDataSource');
    }
    
    if (pipe) {
      expect(pipe.name).toBe('my_pipe__v1');
      expect(pipe.exportName).toBe('myPipe');
    }
  });

  test('should handle files with no TinyKit exports', async () => {
    const testFile = join(testDir, 'no-tinykit.ts');
    
    const content = `
export const someVariable = 'hello';
export function someFunction() {
  return 42;
}
`;

    writeFileSync(testFile, content);
    
    const result = await analyzeTypeScriptFile(testFile);
    
    expect(result.errors).toHaveLength(0);
    expect(result.tinybirdClients).toHaveLength(0);
    expect(result.datasources).toHaveLength(0);
    expect(result.pipes).toHaveLength(0);
  });

  test('should handle import errors gracefully', async () => {
    const testFile = join(testDir, 'import-error.ts');
    
    const content = `
import { nonExistentImport } from 'non-existent-package';
export const something = nonExistentImport();
`;

    writeFileSync(testFile, content);
    
    const result = await analyzeTypeScriptFile(testFile);
    
    expect(result.errors.length).toBeGreaterThan(0);
    expect(result.tinybirdClients).toHaveLength(0);
    expect(result.datasources).toHaveLength(0);
    expect(result.pipes).toHaveLength(0);
  });

  test('should validate naming conflicts', () => {
    const analysis = {
      datasources: [
        { name: 'events__v1', exportName: 'ds1', config: {} as any, sourceFile: 'file1.ts' },
        { name: 'events__v1', exportName: 'ds2', config: {} as any, sourceFile: 'file2.ts' }
      ],
      pipes: [
        { name: 'get_events__v1', exportName: 'pipe1', config: {} as any, sourceFile: 'file1.ts' },
        { name: 'get_events__v1', exportName: 'pipe2', config: {} as any, sourceFile: 'file2.ts' }
      ],
      tinybirdClients: [],
      errors: []
    };
    
    const warnings = validateNaming(analysis);
    
    expect(warnings).toHaveLength(2);
    expect(warnings[0]).toBeDefined();
    expect(warnings[1]).toBeDefined();
    if (warnings[0] && warnings[1]) {
      expect(warnings[0]).toContain('Duplicate datasource names found: events__v1');
      expect(warnings[1]).toContain('Duplicate pipe names found: get_events__v1');
    }
  });

  test('should handle complex Tinybird client configurations', async () => {
    const testFile = join(testDir, 'complex-client.ts');
    
    const content = `
import { Tinybird, defineDataSource, definePipe, defineSchema, string, int64, stringParam, int64Param, enumParam } from '../../../src/index';

const schema1 = defineSchema({
  id: string('id'),
  type: string('type')
});

const schema2 = defineSchema({
  userId: string('userId'),
  timestamp: int64('timestamp')
});

export const primaryClient = new Tinybird({
  token: 'token1',
  datasources: {
    events: defineDataSource({
      name: 'events__v2',
      schema: schema1,
      engine: 'MergeTree'
    }),
    users: defineDataSource({
      name: 'users__v1',
      schema: schema2,
      engine: 'ReplacingMergeTree'
    })
  },
  pipes: {
    getEvents: definePipe({
      name: 'get_events__v2',
      schema: schema1,
      parameters: {
        type: stringParam('type', { required: true }),
        limit: int64Param('limit', { default: 50 }),
        status: enumParam('status', ['active', 'inactive'] as const, { default: 'active' })
      }
    }).endpoint((q, params) => q.select('*')),
    
    getUsers: definePipe({
      name: 'get_users__v1',
      schema: schema2,
      parameters: {
        since: int64Param('since', { required: true })
      }
    }).endpoint((q, params) => q.select('*'))
  }
});
`;

    writeFileSync(testFile, content);
    
    const result = await analyzeTypeScriptFile(testFile);
    
    expect(result.errors).toHaveLength(0);
    expect(result.tinybirdClients).toHaveLength(1);
    
    const client = result.tinybirdClients[0];
    expect(client).toBeDefined();
    if (client) {
      expect(client.exportName).toBe('primaryClient');
      expect(Object.keys(client.datasources)).toHaveLength(2);
      expect(Object.keys(client.pipes)).toHaveLength(2);
    }
    
    expect(result.datasources).toHaveLength(2);
    expect(result.pipes).toHaveLength(2);
    
    const eventsPipe = result.pipes.find(p => p.name === 'get_events__v2');
    expect(eventsPipe).toBeDefined();
    if (eventsPipe) {
      expect(eventsPipe.config.parameters).toHaveProperty('type');
      expect(eventsPipe.config.parameters).toHaveProperty('limit');
      expect(eventsPipe.config.parameters).toHaveProperty('status');
    }
  });

  test('should access client configuration via symbols (not public API)', async () => {
    const testFile = join(testDir, 'symbol-access-test.ts');
    
    const content = `
import { Tinybird } from '../../../src/client';
import { defineDataSource, defineSchema, string, int64 } from '../../../src/schema';
import { definePipe, stringParam } from '../../../src/pipe';

const testSchema = defineSchema({
  id: string('id'),
  name: string('name'),
});

const testDataSource = defineDataSource({
  name: 'test_table__v1',
  schema: testSchema,
  engine: 'MergeTree'
});

const testPipe = definePipe({
  name: 'test_pipe__v1',
  schema: testSchema,
  parameters: {
    id: stringParam('id', { required: true })
  }
}).endpoint((q, params) => q.select('*'));

export const testClient = new Tinybird({
  token: 'test-token',
  datasources: {
    testTable: testDataSource
  },
  pipes: {
    testPipe: testPipe
  }
});
`;

    writeFileSync(testFile, content);
    
    const result = await analyzeTypeScriptFile(testFile);
    
    expect(result.errors).toHaveLength(0);
    expect(result.tinybirdClients).toHaveLength(1);
    
    const client = result.tinybirdClients[0];
    expect(client).toBeDefined();
    if (client) {
      expect(client.exportName).toBe('testClient');
      
      // Verify analyzer can access configuration via symbols
      expect(Object.keys(client.datasources)).toEqual(['testTable']);
      expect(Object.keys(client.pipes)).toEqual(['testPipe']);
    }
    
    expect(result.datasources).toHaveLength(1);
    const datasource = result.datasources[0];
    expect(datasource).toBeDefined();
    if (datasource) {
      expect(datasource.name).toBe('test_table__v1');
      expect(datasource.exportName).toBe('testClient.datasources.testTable');
    }
    
    expect(result.pipes).toHaveLength(1);
    const pipe = result.pipes[0];
    expect(pipe).toBeDefined();
    if (pipe) {
      expect(pipe.name).toBe('test_pipe__v1');
      expect(pipe.exportName).toBe('testClient.pipes.testPipe');
    }
  });

  test('should ensure datasources and pipes are not publicly accessible', async () => {
    const testFile = join(testDir, 'public-api-privacy-test.ts');
    
    const content = `
import { Tinybird } from '../../../src/client';
import { defineDataSource, defineSchema, string } from '../../../src/schema';

const schema = defineSchema({
  id: string('id'),
  name: string('name'),
});

const dataSource = defineDataSource({
  name: 'private_table__v1',
  schema,
  engine: 'MergeTree'
});

export const client = new Tinybird({
  token: 'test-token',
  datasources: {
    privateTable: dataSource
  }
});

// Test that public API doesn't expose internal config
export const publicApiTest = {
  // Private properties are detectable via 'in' operator but not accessible
  hasDatasourcesProperty: 'datasources' in client!,
  hasPipesProperty: 'pipes' in client!,
  hasConfigProperty: 'config' in client!,
  
  // Test actual access - these should be undefined for private properties
  datasourcesValue: (client! as any).datasources,
  pipesValue: (client! as any).pipes,
  configValue: (client! as any).config,
  
  // These should be accessible (public API)
  hasTokenProperty: 'token' in client!,
  hasNoopProperty: 'noop' in client!,
  hasFromMethod: typeof client!.from === 'function',
  hasPipeMethod: typeof client!.pipe === 'function',
  hasIngestMethod: typeof client!.ingest === 'function',
  
  // Public properties should have actual values
  tokenValue: client!.token,
  noopValue: client!.noop,
  
  // Object.keys should not reveal internal symbols or private properties
  objectKeys: Object.keys(client!),
  
  // getOwnPropertyNames should not reveal symbol properties
  ownPropertyNames: Object.getOwnPropertyNames(client!),
  symbolPropertyNames: Object.getOwnPropertyNames(client!).filter(name => 
    name.includes('datasource') || name.includes('pipe') || name.includes('config')
  )
};
`;

    writeFileSync(testFile, content);
    
    const result = await analyzeTypeScriptFile(testFile);
    
    expect(result.errors).toHaveLength(0);
    
    // Import the module to test the actual runtime behavior
    const fileUrl = new URL('file://' + testFile.replace(/\\/g, '/'));
    const module = await import(fileUrl.href);
    
    const publicApiTest = module.publicApiTest;
    
    // Private properties are detectable but not accessible
    expect(publicApiTest.hasDatasourcesProperty).toBe(true); // 'in' operator detects private properties
    expect(publicApiTest.hasPipesProperty).toBe(true);
    expect(publicApiTest.hasConfigProperty).toBe(false); // this one doesn't exist
    
    // But the actual values should be undefined (not accessible)
    expect(publicApiTest.datasourcesValue).toBeUndefined();
    expect(publicApiTest.pipesValue).toBeUndefined();
    expect(publicApiTest.configValue).toBeUndefined();
    
    // Verify public API is accessible
    expect(publicApiTest.hasTokenProperty).toBe(true);
    expect(publicApiTest.hasNoopProperty).toBe(true);
    expect(publicApiTest.hasFromMethod).toBe(true);
    expect(publicApiTest.hasPipeMethod).toBe(true);
    expect(publicApiTest.hasIngestMethod).toBe(true);
    
    // And public values should be accessible
    expect(publicApiTest.tokenValue).toBe('test-token');
    expect(publicApiTest.noopValue).toBe(false);
    
    // Verify Object.keys doesn't reveal private properties or symbols
    expect(publicApiTest.objectKeys).not.toContain('datasources');
    expect(publicApiTest.objectKeys).not.toContain('pipes');
    expect(publicApiTest.objectKeys).not.toContain('config');
    
    // Verify getOwnPropertyNames doesn't reveal symbol properties
    expect(publicApiTest.symbolPropertyNames).toHaveLength(0);
    
    // But analyzer should still work
    expect(result.tinybirdClients).toHaveLength(1);
    expect(result.datasources).toHaveLength(1);
    const datasource = result.datasources[0];
    expect(datasource).toBeDefined();
    if (datasource) {
      expect(datasource.name).toBe('private_table__v1');
    }
  });

  test('should handle clients with no datasources or pipes', async () => {
    const testFile = join(testDir, 'empty-client.ts');
    
    const content = `
import { Tinybird } from '../../../src/client';

export const emptyClient = new Tinybird({
  token: 'test-token'
});

export const noopClient = new Tinybird({
  noop: true
});
`;

    writeFileSync(testFile, content);
    
    const result = await analyzeTypeScriptFile(testFile);
    
    expect(result.errors).toHaveLength(0);
    expect(result.tinybirdClients).toHaveLength(2);
    
    const emptyClient = result.tinybirdClients.find(c => c.exportName === 'emptyClient');
    expect(emptyClient).toBeDefined();
    if (emptyClient) {
      expect(Object.keys(emptyClient.datasources)).toHaveLength(0);
      expect(Object.keys(emptyClient.pipes)).toHaveLength(0);
    }
    
    const noopClient = result.tinybirdClients.find(c => c.exportName === 'noopClient');
    expect(noopClient).toBeDefined();
    if (noopClient) {
      expect(Object.keys(noopClient.datasources)).toHaveLength(0);
      expect(Object.keys(noopClient.pipes)).toHaveLength(0);
    }
    
    expect(result.datasources).toHaveLength(0);
    expect(result.pipes).toHaveLength(0);
  });

  test('should handle mixed client and standalone exports', async () => {
    const testFile = join(testDir, 'mixed-exports.ts');
    
    const content = `
import { Tinybird } from '../../../src/client';
import { defineDataSource, definePipe, defineSchema, string, stringParam } from '../../../src/index';

const schema = defineSchema({
  id: string('id'),
  name: string('name')
});

// Standalone exports
export const standaloneDataSource = defineDataSource({
  name: 'standalone_table__v1',
  schema,
  engine: 'MergeTree'
});

export const standalonePipe = definePipe({
  name: 'standalone_pipe__v1',
  schema,
  parameters: {
    id: stringParam('id', { required: true })
  }
}).endpoint((q, params) => q.select('*'));

// Client with configuration
export const mixedClient = new Tinybird({
  token: 'test-token',
  datasources: {
    clientTable: defineDataSource({
      name: 'client_table__v1',
      schema,
      engine: 'MergeTree'
    })
  },
  pipes: {
    clientPipe: definePipe({
      name: 'client_pipe__v1',
      schema,
      parameters: {
        name: stringParam('name', { required: true })
      }
    }).endpoint((q, params) => q.select('*'))
  }
});
`;

    writeFileSync(testFile, content);
    
    const result = await analyzeTypeScriptFile(testFile);
    
    expect(result.errors).toHaveLength(0);
    expect(result.tinybirdClients).toHaveLength(1);
    expect(result.datasources).toHaveLength(2); // 1 standalone + 1 from client
    expect(result.pipes).toHaveLength(2); // 1 standalone + 1 from client
    
    // Check standalone exports
    const standaloneDs = result.datasources.find(ds => ds.exportName === 'standaloneDataSource');
    expect(standaloneDs).toBeDefined();
    expect(standaloneDs!.name).toBe('standalone_table__v1');
    
    const standalonePipe = result.pipes.find(p => p.exportName === 'standalonePipe');
    expect(standalonePipe).toBeDefined();
    expect(standalonePipe!.name).toBe('standalone_pipe__v1');
    
    // Check client exports
    const clientDs = result.datasources.find(ds => ds.exportName === 'mixedClient.datasources.clientTable');
    expect(clientDs).toBeDefined();
    expect(clientDs!.name).toBe('client_table__v1');
    
    const clientPipe = result.pipes.find(p => p.exportName === 'mixedClient.pipes.clientPipe');
    expect(clientPipe).toBeDefined();
    expect(clientPipe!.name).toBe('client_pipe__v1');
  });
});