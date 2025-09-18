import { expect, test, describe, beforeAll, afterAll } from 'bun:test';
import { writeFileSync, mkdirSync, rmSync, existsSync, readFileSync } from 'fs';
import { join } from 'path';
import { generateCommand } from '../../src/cli/commands/generate';

const testDir = join(__dirname, 'generate-test-fixtures');
const outputDir = join(testDir, 'tinybird');

beforeAll(() => {
  // Create test directory
  mkdirSync(testDir, { recursive: true });
});

afterAll(() => {
  // Clean up test directory
  rmSync(testDir, { recursive: true, force: true });
});

describe('Generate Command Integration', () => {
  test('should generate datasource and pipe files from Tinybird client', async () => {
    const sourceFile = join(testDir, 'client.ts');
    
    const content = `
import { Tinybird } from '../../src/client';
import { defineDataSource, defineSchema, string, int64 } from '../../src/schema';
import { definePipe, stringParam, int64Param } from '../../src/pipe';

const eventsSchema = defineSchema({
  id: string('id', { jsonPath: '$.id' }),
  tenantId: string('tenantId', { jsonPath: '$.tenantId' }),
  time: int64('time', { jsonPath: '$.time' }),
  event: string('event', { jsonPath: '$.event' }),
});

const eventsDataSource = defineDataSource({
  name: 'events__v1',
  version: 1,
  schema: eventsSchema,
  engine: 'MergeTree',
  sortingKey: ['tenantId', 'time', 'id'],
});

const getUserActivityPipe = definePipe({
  name: 'get_user_activity__v1',
  version: 1,
  schema: eventsSchema,
  parameters: {
    tenantId: stringParam('tenantId', { required: true }),
    start: int64Param('start', { required: true }),
    limit: int64Param('limit', { default: 100 })
  }
}).endpoint((q, params) =>
  \`SELECT 
    id, 
    event,
    count() AS count
  FROM events__v1 
  WHERE tenantId = \${params.tenantId} 
    AND time >= \${params.start}
  GROUP BY id, event
  ORDER BY count DESC
  LIMIT \${params.limit}\`
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

    writeFileSync(sourceFile, content);

    // Run generate command
    await generateCommand({
      file: sourceFile,
      dir: outputDir,
      watch: false,
      dryRun: false
    });

    // Check that directories were created
    expect(existsSync(join(outputDir, 'datasources'))).toBe(true);
    expect(existsSync(join(outputDir, 'pipes'))).toBe(true);

    // Check datasource file
    const datasourceFile = join(outputDir, 'datasources', 'events.datasource');
    expect(existsSync(datasourceFile)).toBe(true);
    
    const datasourceContent = readFileSync(datasourceFile, 'utf-8');
    expect(datasourceContent).toContain('VERSION 1');
    expect(datasourceContent).toContain('SCHEMA >');
    expect(datasourceContent).toContain('`id` String `json:$.id`');
    expect(datasourceContent).toContain('`tenantId` String `json:$.tenantId`');
    expect(datasourceContent).toContain('`time` Int64 `json:$.time`');
    expect(datasourceContent).toContain('`event` String `json:$.event`');
    expect(datasourceContent).toContain('ENGINE "MergeTree"');
    expect(datasourceContent).toContain('ENGINE_SORTING_KEY "tenantId,time,id"');

    // Check pipe file
    const pipeFile = join(outputDir, 'pipes', 'get_user_activity__v1.pipe');
    expect(existsSync(pipeFile)).toBe(true);
    
    const pipeContent = readFileSync(pipeFile, 'utf-8');
    expect(pipeContent).toContain('VERSION 1');
    expect(pipeContent).toContain('NODE endpoint');
    expect(pipeContent).toContain('SQL >');
    expect(pipeContent).toContain('{{ String(tenantId, required=True) }}');
    expect(pipeContent).toContain('{{ Int64(start, required=True) }}');
    expect(pipeContent).toContain('{{ Int64(limit, 100) }}');
    expect(pipeContent).toContain('SELECT');
    expect(pipeContent).toContain('FROM events__v1');
    expect(pipeContent).toContain('GROUP BY id, event');
  });

  test('should handle dry run mode', async () => {
    const sourceFile = join(testDir, 'dry-run-client.ts');
    
    const content = `
import { Tinybird, defineDataSource, defineSchema, string } from 'tinykit';

const schema = defineSchema({
  id: string('id')
});

export const tb = new Tinybird({
  token: 'test',
  datasources: {
    test: defineDataSource({
      name: 'test__v1',
      schema,
      engine: 'MergeTree'
    })
  }
});
`;

    writeFileSync(sourceFile, content);

    const dryRunOutputDir = join(testDir, 'dry-run-output');

    // Run generate command with dry run
    await generateCommand({
      file: sourceFile,
      dir: dryRunOutputDir,
      watch: false,
      dryRun: true
    });

    // Files should not be created in dry run mode
    expect(existsSync(dryRunOutputDir)).toBe(false);
  });

  test('should generate multiple datasources and pipes', async () => {
    const sourceFile = join(testDir, 'multi-resource-client.ts');
    
    const content = `
import { Tinybird } from '../../src/client';
import { defineDataSource, defineSchema, string, int64 } from '../../src/schema';
import { definePipe, stringParam, int64Param } from '../../src/pipe';

const eventsSchema = defineSchema({
  id: string('id'),
  event: string('event')
});

const usersSchema = defineSchema({
  userId: string('userId'),
  name: string('name')
});

export const tb = new Tinybird({
  token: 'test',
  datasources: {
    events: defineDataSource({
      name: 'events__v2',
      schema: eventsSchema,
      engine: 'MergeTree'
    }),
    users: defineDataSource({
      name: 'users__v1',
      schema: usersSchema,
      engine: 'ReplacingMergeTree'
    })
  },
  pipes: {
    getEvents: definePipe({
      name: 'get_events__v2',
      schema: eventsSchema,
      parameters: {
        eventType: stringParam('eventType', { required: true })
      }
    }).endpoint((q, params) => \`SELECT * FROM events__v2 WHERE event = \${params.eventType}\`),
    
    getUsers: definePipe({
      name: 'get_users__v1',
      schema: usersSchema,
      parameters: {
        limit: int64Param('limit', { default: 50 })
      }
    }).endpoint((q, params) => \`SELECT * FROM users__v1 LIMIT \${params.limit}\`)
  }
});
`;

    writeFileSync(sourceFile, content);

    const multiOutputDir = join(testDir, 'multi-output');

    await generateCommand({
      file: sourceFile,
      dir: multiOutputDir,
      watch: false,
      dryRun: false
    });

    // Check all files were generated
    expect(existsSync(join(multiOutputDir, 'datasources', 'events.datasource'))).toBe(true);
    expect(existsSync(join(multiOutputDir, 'datasources', 'users.datasource'))).toBe(true);
    expect(existsSync(join(multiOutputDir, 'pipes', 'get_events__v2.pipe'))).toBe(true);
    expect(existsSync(join(multiOutputDir, 'pipes', 'get_users__v1.pipe'))).toBe(true);

    // Verify content of one datasource
    const eventsDataSource = readFileSync(join(multiOutputDir, 'datasources', 'events.datasource'), 'utf-8');
    expect(eventsDataSource).toContain('ENGINE "MergeTree"');

    const usersDataSource = readFileSync(join(multiOutputDir, 'datasources', 'users.datasource'), 'utf-8');
    expect(usersDataSource).toContain('ENGINE "ReplacingMergeTree"');

    // Verify pipe content
    const eventsPipe = readFileSync(join(multiOutputDir, 'pipes', 'get_events__v2.pipe'), 'utf-8');
    expect(eventsPipe).toContain('{{ String(eventType, required=True) }}');

    const usersPipe = readFileSync(join(multiOutputDir, 'pipes', 'get_users__v1.pipe'), 'utf-8');
    expect(usersPipe).toContain('{{ Int64(limit, 50) }}');
  });

  test('should handle standalone datasources and pipes', async () => {
    const sourceFile = join(testDir, 'standalone-resources.ts');
    
    const content = `
import { defineDataSource, defineSchema, string } from '../../src/schema';
import { definePipe, stringParam } from '../../src/pipe';

const schema = defineSchema({
  id: string('id'),
  name: string('name')
});

export const myDataSource = defineDataSource({
  name: 'standalone_table__v1',
  schema,
  engine: 'MergeTree',
  version: 2
});

export const myPipe = definePipe({
  name: 'standalone_pipe__v1',
  schema,
  parameters: {
    searchTerm: stringParam('searchTerm', { required: true })
  }
}).endpoint((q, params) => \`SELECT * FROM standalone_table__v1 WHERE name LIKE '%\${params.searchTerm}%'\`);
`;

    writeFileSync(sourceFile, content);

    const standaloneOutputDir = join(testDir, 'standalone-output');

    await generateCommand({
      file: sourceFile,
      dir: standaloneOutputDir,
      watch: false,
      dryRun: false
    });

    // Check files were generated
    expect(existsSync(join(standaloneOutputDir, 'datasources', 'standalone_table.datasource'))).toBe(true);
    expect(existsSync(join(standaloneOutputDir, 'pipes', 'standalone_pipe__v1.pipe'))).toBe(true);

    // Verify content
    const datasourceContent = readFileSync(join(standaloneOutputDir, 'datasources', 'standalone_table.datasource'), 'utf-8');
    expect(datasourceContent).toContain('VERSION 2');

    const pipeContent = readFileSync(join(standaloneOutputDir, 'pipes', 'standalone_pipe__v1.pipe'), 'utf-8');
    expect(pipeContent).toContain('{{ String(searchTerm, required=True) }}');
    expect(pipeContent).toContain('LIKE');
  });

  test('should handle files with no TinyKit resources', async () => {
    const sourceFile = join(testDir, 'empty-file.ts');
    
    const content = `
export const someVariable = 'hello world';
export function someFunction() {
  return 42;
}
`;

    writeFileSync(sourceFile, content);

    const emptyOutputDir = join(testDir, 'empty-output');

    await generateCommand({
      file: sourceFile,
      dir: emptyOutputDir,
      watch: false,
      dryRun: false
    });

    // Output directories should be created but empty
    expect(existsSync(join(emptyOutputDir, 'datasources'))).toBe(true);
    expect(existsSync(join(emptyOutputDir, 'pipes'))).toBe(true);
    
    // But no files should be generated
    const datasourceFiles = require('fs').readdirSync(join(emptyOutputDir, 'datasources'));
    const pipeFiles = require('fs').readdirSync(join(emptyOutputDir, 'pipes'));
    
    expect(datasourceFiles).toHaveLength(0);
    expect(pipeFiles).toHaveLength(0);
  });
});