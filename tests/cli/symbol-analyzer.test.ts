import { expect, test, describe, beforeAll, afterAll } from 'bun:test';
import { writeFileSync, mkdirSync, rmSync } from 'fs';
import { join } from 'path';
import { analyzeTypeScriptFile } from '../../src/cli/utils/typescript-analyzer';
import {
  isTinybirdClientSymbol,
  datasourcesSymbol,
  pipesSymbol,
} from '../../src/client';

const testDir = join(__dirname, 'symbol-test-fixtures');

beforeAll(() => {
  // Create test directory
  mkdirSync(testDir, { recursive: true });
});

afterAll(() => {
  // Clean up test directory
  rmSync(testDir, { recursive: true, force: true });
});

describe('Symbol-based Analyzer', () => {
  test('should verify symbols are properly exported and accessible', () => {
    // Verify the symbols exist and are unique
    expect(typeof isTinybirdClientSymbol).toBe('symbol');
    expect(typeof datasourcesSymbol).toBe('symbol');
    expect(typeof pipesSymbol).toBe('symbol');

    // Verify symbols are unique
    expect(isTinybirdClientSymbol).not.toBe(datasourcesSymbol);
    expect(isTinybirdClientSymbol).not.toBe(pipesSymbol);
    expect(datasourcesSymbol).not.toBe(pipesSymbol);

    // Verify symbols use expected keys for global symbol registry
    expect(Symbol.for('isTinybirdClient')).toBe(isTinybirdClientSymbol);
    expect(Symbol.for('tinybirdDatasources')).toBe(datasourcesSymbol);
    expect(Symbol.for('tinybirdPipes')).toBe(pipesSymbol);
  });

  test('should handle objects with symbol properties but invalid configs', async () => {
    const testFile = join(testDir, 'invalid-symbol-objects.ts');

    const content = `
import { isTinybirdClientSymbol, datasourcesSymbol, pipesSymbol } from '../../../src/client';

// Object with client symbol but no actual client functionality
export const fakeClient = {
  [Symbol.for('isTinybirdClient')]: true,
  [Symbol.for('tinybirdDatasources')]: {
    invalidDs: { name: 'test', invalidProperty: true }
  },
  [Symbol.for('tinybirdPipes')]: {
    invalidPipe: { name: 'test', invalidProperty: true }
  }
};

// Object with symbols but wrong types
export const wrongTypeClient = {
  [Symbol.for('isTinybirdClient')]: true,
  [Symbol.for('tinybirdDatasources')]: "not an object",
  [Symbol.for('tinybirdPipes')]: 42
};
`;

    writeFileSync(testFile, content);

    const result = await analyzeTypeScriptFile(testFile);

    // Should discover fake clients but filter out invalid configs
    expect(result.errors).toHaveLength(0);
    expect(result.tinybirdClients).toHaveLength(2);

    // Neither should produce valid datasources or pipes
    expect(result.datasources).toHaveLength(0);
    expect(result.pipes).toHaveLength(0);

    // But clients should still be detected
    const fakeClient = result.tinybirdClients.find(
      (c) => c.exportName === 'fakeClient'
    );
    const wrongTypeClient = result.tinybirdClients.find(
      (c) => c.exportName === 'wrongTypeClient'
    );

    expect(fakeClient).toBeDefined();
    expect(wrongTypeClient).toBeDefined();

    expect(Object.keys(fakeClient!.datasources)).toHaveLength(0);
    expect(Object.keys(fakeClient!.pipes)).toHaveLength(0);
    expect(Object.keys(wrongTypeClient!.datasources)).toHaveLength(0);
    expect(Object.keys(wrongTypeClient!.pipes)).toHaveLength(0);
  });

  test('should handle symbol collision scenarios', async () => {
    const testFile = join(testDir, 'symbol-collision.ts');

    const content = `
import { Tinybird } from '../../../src/client';
import { defineDataSource, defineSchema, string } from '../../../src/schema';

const schema = defineSchema({
  id: string('id'),
  name: string('name')
});

const dataSource = defineDataSource({
  name: 'collision_test__v1',
  schema,
  engine: 'MergeTree'
});

// Legitimate client
export const realClient = new Tinybird({
  token: 'test-token',
  datasources: {
    realTable: dataSource
  }
});

// Object that manually sets the same symbols (should still work)
export const manualClient = {
  [Symbol.for('isTinybirdClient')]: true,
  [Symbol.for('tinybirdDatasources')]: {
    manualTable: dataSource
  },
  [Symbol.for('tinybirdPipes')]: {}
};

// Object with client symbol but conflicting property names
export const conflictClient = {
  [Symbol.for('isTinybirdClient')]: true,
  [Symbol.for('tinybirdDatasources')]: {
    conflictTable: dataSource
  },
  // Regular property (should not interfere)
  datasources: "this should not interfere with symbol access",
  pipes: { fake: "pipe" }
};
`;

    writeFileSync(testFile, content);

    const result = await analyzeTypeScriptFile(testFile);

    expect(result.errors).toHaveLength(0);
    expect(result.tinybirdClients).toHaveLength(3);
    expect(result.datasources).toHaveLength(3);

    // All three clients should be properly analyzed
    const realClient = result.tinybirdClients.find(
      (c) => c.exportName === 'realClient'
    );
    const manualClient = result.tinybirdClients.find(
      (c) => c.exportName === 'manualClient'
    );
    const conflictClient = result.tinybirdClients.find(
      (c) => c.exportName === 'conflictClient'
    );

    expect(realClient).toBeDefined();
    expect(manualClient).toBeDefined();
    expect(conflictClient).toBeDefined();

    expect(Object.keys(realClient!.datasources)).toEqual(['realTable']);
    expect(Object.keys(manualClient!.datasources)).toEqual(['manualTable']);
    expect(Object.keys(conflictClient!.datasources)).toEqual(['conflictTable']);

    // Verify proper datasource extraction
    const realDs = result.datasources.find(
      (ds) => ds.exportName === 'realClient.datasources.realTable'
    );
    const manualDs = result.datasources.find(
      (ds) => ds.exportName === 'manualClient.datasources.manualTable'
    );
    const conflictDs = result.datasources.find(
      (ds) => ds.exportName === 'conflictClient.datasources.conflictTable'
    );

    expect(realDs).toBeDefined();
    expect(manualDs).toBeDefined();
    expect(conflictDs).toBeDefined();

    expect(realDs!.name).toBe('collision_test__v1');
    expect(manualDs!.name).toBe('collision_test__v1');
    expect(conflictDs!.name).toBe('collision_test__v1');
  });

  test('should verify symbol access is truly private from normal JS operations', async () => {
    const testFile = join(testDir, 'privacy-verification.ts');

    const content = `
import { Tinybird } from '../../../src/client';
import { defineDataSource, defineSchema, string } from '../../../src/schema';

const schema = defineSchema({
  id: string('id'),
  secret: string('secret')
});

const secretDataSource = defineDataSource({
  name: 'secret_table__v1',
  schema,
  engine: 'MergeTree'
});

export const client = new Tinybird({
  token: 'secret-token',
  datasources: {
    secretTable: secretDataSource
  }
});

// Comprehensive privacy testing
export const privacyTests = {
  // Normal property access
  directAccess: {
    datasources: (client as any).datasources,
    pipes: (client as any).pipes,
    config: (client as any).config
  },
  
  // Object inspection methods
  objectKeys: Object.keys(client),
  objectValues: Object.values(client),
  objectEntries: Object.entries(client),
  
  // Property enumeration
  ownPropertyNames: Object.getOwnPropertyNames(client),
  propertyDescriptors: Object.getOwnPropertyDescriptors(client),
  
  // JSON serialization (should not reveal symbols)
  jsonSerialization: JSON.stringify(client),
  
  // for...in loop results
  forInProperties: (() => {
    const props: string[] = [];
    for (const prop in client) {
      props.push(prop);
    }
    return props;
  })(),
  
  // Symbol inspection (symbols should exist but not be accessible without the symbol)
  ownPropertySymbols: Object.getOwnPropertySymbols(client),
  symbolsLength: Object.getOwnPropertySymbols(client).length,
  
  // Reflection attempts
  hasOwnProperty: {
    datasources: client.hasOwnProperty('datasources'),
    pipes: client.hasOwnProperty('pipes'),
    config: client.hasOwnProperty('config')
  },
  
  // Descriptor access attempts
  getPropertyDescriptor: {
    datasources: Object.getPropertyDescriptor(client, 'datasources'),
    pipes: Object.getPropertyDescriptor(client, 'pipes'),
    config: Object.getPropertyDescriptor(client, 'config')
  }
};
`;

    writeFileSync(testFile, content);

    const result = await analyzeTypeScriptFile(testFile);

    expect(result.errors).toHaveLength(0);
    expect(result.tinybirdClients).toHaveLength(1);
    expect(result.datasources).toHaveLength(1);

    // Import and test the privacy verification
    const fileUrl = new URL('file://' + testFile.replace(/\\/g, '/'));
    const module = await import(fileUrl.href);
    const privacyTests = module.privacyTests;

    // Direct access should be undefined
    expect(privacyTests.directAccess.datasources).toBeUndefined();
    expect(privacyTests.directAccess.pipes).toBeUndefined();
    expect(privacyTests.directAccess.config).toBeUndefined();

    // Object inspection should not reveal internal properties
    expect(privacyTests.objectKeys).not.toContain('datasources');
    expect(privacyTests.objectKeys).not.toContain('pipes');
    expect(privacyTests.objectKeys).not.toContain('config');

    expect(privacyTests.ownPropertyNames).not.toContain('datasources');
    expect(privacyTests.ownPropertyNames).not.toContain('pipes');
    expect(privacyTests.ownPropertyNames).not.toContain('config');

    // for...in should not reveal symbol properties
    expect(privacyTests.forInProperties).not.toContain('datasources');
    expect(privacyTests.forInProperties).not.toContain('pipes');
    expect(privacyTests.forInProperties).not.toContain('config');

    // JSON serialization should not include symbols
    const parsedJson = JSON.parse(privacyTests.jsonSerialization);
    expect(parsedJson.datasources).toBeUndefined();
    expect(parsedJson.pipes).toBeUndefined();
    expect(parsedJson.config).toBeUndefined();

    // Symbols should exist (our internal symbols)
    expect(privacyTests.symbolsLength).toBeGreaterThan(0);

    // hasOwnProperty should return false for non-existent properties
    expect(privacyTests.hasOwnProperty.datasources).toBe(false);
    expect(privacyTests.hasOwnProperty.pipes).toBe(false);
    expect(privacyTests.hasOwnProperty.config).toBe(false);

    // Property descriptors should be undefined for non-existent properties
    expect(privacyTests.getPropertyDescriptor.datasources).toBeUndefined();
    expect(privacyTests.getPropertyDescriptor.pipes).toBeUndefined();
    expect(privacyTests.getPropertyDescriptor.config).toBeUndefined();

    // But analyzer should still successfully extract the configuration
    expect(result.datasources[0]!.name).toBe('secret_table__v1');
    expect(result.datasources[0]!.exportName).toBe(
      'client.datasources.secretTable'
    );
  });

  test('should handle symbol property edge cases and malformed data', async () => {
    const testFile = join(testDir, 'symbol-edge-cases.ts');

    const content = `
// Test various edge cases with symbol properties
export const edgeCases = {
  // Client with symbols but null/undefined values
  nullDatasources: {
    [Symbol.for('isTinybirdClient')]: true,
    [Symbol.for('tinybirdDatasources')]: null,
    [Symbol.for('tinybirdPipes')]: undefined
  },
  
  // Client with empty symbol properties
  emptySymbolProps: {
    [Symbol.for('isTinybirdClient')]: true,
    [Symbol.for('tinybirdDatasources')]: {},
    [Symbol.for('tinybirdPipes')]: {}
  },
  
  // Client with circular references (should not break analyzer)
  circularRefs: (() => {
    const obj: any = {
      [Symbol.for('isTinybirdClient')]: true,
      [Symbol.for('tinybirdDatasources')]: {},
      [Symbol.for('tinybirdPipes')]: {}
    };
    obj[Symbol.for('tinybirdDatasources')].self = obj;
    return obj;
  })(),
  
  // Object with symbol but wrong client symbol value
  wrongClientSymbol: {
    [Symbol.for('isTinybirdClient')]: false,
    [Symbol.for('tinybirdDatasources')]: { fake: 'data' },
    [Symbol.for('tinybirdPipes')]: { fake: 'pipe' }
  },
  
  // Object with symbol but non-boolean client symbol
  nonBooleanSymbol: {
    [Symbol.for('isTinybirdClient')]: 'true',
    [Symbol.for('tinybirdDatasources')]: { fake: 'data' },
    [Symbol.for('tinybirdPipes')]: { fake: 'pipe' }
  }
};
`;

    writeFileSync(testFile, content);

    const result = await analyzeTypeScriptFile(testFile);

    expect(result.errors).toHaveLength(0);

    // Should only detect legitimate clients (with boolean true symbol)
    const clientExports = result.tinybirdClients.filter((c) =>
      c.exportName.includes('edgeCases.')
    );

    // Only nullDatasources, emptySymbolProps, and circularRefs should be detected as clients
    // wrongClientSymbol and nonBooleanSymbol should not be detected
    expect(clientExports).toHaveLength(3);

    const nullClient = result.tinybirdClients.find(
      (c) => c.exportName === 'edgeCases.nullDatasources'
    );
    const emptyClient = result.tinybirdClients.find(
      (c) => c.exportName === 'edgeCases.emptySymbolProps'
    );
    const circularClient = result.tinybirdClients.find(
      (c) => c.exportName === 'edgeCases.circularRefs'
    );

    expect(nullClient).toBeDefined();
    expect(emptyClient).toBeDefined();
    expect(circularClient).toBeDefined();

    // All should have empty datasources/pipes due to null/empty/invalid data
    expect(Object.keys(nullClient!.datasources)).toHaveLength(0);
    expect(Object.keys(nullClient!.pipes)).toHaveLength(0);

    expect(Object.keys(emptyClient!.datasources)).toHaveLength(0);
    expect(Object.keys(emptyClient!.pipes)).toHaveLength(0);

    expect(Object.keys(circularClient!.datasources)).toHaveLength(0);
    expect(Object.keys(circularClient!.pipes)).toHaveLength(0);

    // Should produce no datasources or pipes
    expect(result.datasources).toHaveLength(0);
    expect(result.pipes).toHaveLength(0);
  });
});
