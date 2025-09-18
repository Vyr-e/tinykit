import * as prompts from 'prompts';
import { readdirSync, statSync, writeFileSync, mkdirSync, existsSync } from 'fs';
import { join, extname } from 'path';
import { log } from '../utils/terminal';
import { generateDatasourceFile, extractDatasourceName } from '../generators/datasource';
import { generatePipeFile, extractPipeName } from '../generators/pipe';
import { analyzeTypeScriptFiles, validateNaming } from '../utils/typescript-analyzer';
import type { FileGenerationResult } from '../types';

interface GenerateOptions {
  file?: string;
  dir: string;
  watch: boolean;
  dryRun: boolean;
}

export async function generateCommand(options: GenerateOptions): Promise<void> {
  log.info('🔧 Starting TinyKit generation...\n');

  let sourceFiles: string[] = [];

  // If no file specified, prompt user to find TypeScript files
  if (!options.file) {
    const tsFiles = findTypeScriptFiles('./src');

    if (tsFiles.length === 0) {
      log.warning('No TypeScript files found in ./src directory');

      const { createExample } = await prompts({
        type: 'confirm',
        name: 'createExample',
        message: 'Would you like to create an example file?',
        initial: true,
      });

      if (createExample) {
        await createExampleFile();
        sourceFiles = ['./src/example.ts'];
      } else {
        log.info('Generation cancelled');
        return;
      }
    } else if (tsFiles.length === 1) {
      sourceFiles = tsFiles;
      log.info(`Found TypeScript file: ${tsFiles[0]}`);
    } else {
      // Multiple files found - let user choose
      const { selectedFiles } = await prompts({
        type: 'multiselect',
        name: 'selectedFiles',
        message: 'Select TypeScript files to process:',
        choices: tsFiles.map(file => ({
          title: file.replace('./src/', ''),
          value: file,
          selected: true,
        })),
        hint: '- Space to select. Return to submit',
      });

      if (!selectedFiles || selectedFiles.length === 0) {
        log.warning('No files selected');
        return;
      }

      sourceFiles = selectedFiles;
    }
  } else {
    sourceFiles = [options.file];
  }

  // Validate source files exist
  for (const file of sourceFiles) {
    if (!existsSync(file)) {
      log.error(`File not found: ${file}`);
      return;
    }
  }

  // Create output directory if it doesn't exist
  if (!options.dryRun) {
    ensureDirectoryStructure(options.dir);
  }

  try {
    log.info(`Processing ${sourceFiles.length} file(s)...`);

    // Analyze all TypeScript files to discover datasources and pipes
    const analysis = await analyzeTypeScriptFiles(sourceFiles);

    // Check for naming conflicts and other issues
    const warnings = validateNaming(analysis);
    if (warnings.length > 0) {
      log.warning('Warning(s) found:');
      warnings.forEach(warning => log.plain(`  • ${warning}`));
      log.plain('');
    }

    if (analysis.errors.length > 0) {
      log.error('Error(s) occurred during analysis:');
      analysis.errors.forEach(error => log.plain(`  • ${error}`));
      log.plain('');
    }

    const results: FileGenerationResult[] = [];

    // Generate datasource files
    for (const discovered of analysis.datasources) {
      log.info(`Generating datasource: ${discovered.name}`);
      const content = generateDatasourceFile(discovered.config);
      const fileName = `${extractDatasourceName(discovered.config)}.datasource`;
      const filePath = join(options.dir, 'datasources', fileName);

      if (!options.dryRun) {
        writeFileSync(filePath, content);
      }

      results.push({
        path: filePath,
        content,
        type: 'datasource',
        name: discovered.name,
      });
    }

    // Generate pipe files
    for (const discovered of analysis.pipes) {
      log.info(`Generating pipe: ${discovered.name}`);
      const content = generatePipeFile(discovered.config);
      const fileName = `${extractPipeName(discovered.config)}.pipe`;
      const filePath = join(options.dir, 'pipes', fileName);

      if (!options.dryRun) {
        writeFileSync(filePath, content);
      }

      results.push({
        path: filePath,
        content,
        type: 'pipe',
        name: discovered.name,
      });
    }

    // Summary
    const datasources = results.filter(r => r.type === 'datasource');
    const pipes = results.filter(r => r.type === 'pipe');

    log.plain('');
    log.success('Generation completed!');
    
    if (analysis.tinybirdClients.length > 0) {
      log.info(`Found ${analysis.tinybirdClients.length} Tinybird client(s)`);
      analysis.tinybirdClients.forEach(client => {
        const dsCount = Object.keys(client.datasources).length;
        const pipeCount = Object.keys(client.pipes).length;
        log.dim(`  ${client.exportName}: ${dsCount} datasource(s), ${pipeCount} pipe(s)`);
      });
    }
    
    log.info(`Generated ${datasources.length} datasource(s) and ${pipes.length} pipe(s)`);

    if (options.dryRun) {
      log.dim('\n(Dry run - no files were written)');
    } else {
      log.dim('\nGenerated files:');
      results.forEach(result => {
        log.dim(`  ${result.type}: ${result.path}`);
      });
    }

    if (options.watch) {
      log.info('\n👀 Watching for changes... (Press Ctrl+C to stop)');
      // TODO: Implement file watching
    }

  } catch (error) {
    log.error(`Generation failed: ${error instanceof Error ? error.message : String(error)}`);
    process.exit(1);
  }
}

function findTypeScriptFiles(dir: string): string[] {
  const files: string[] = [];

  if (!existsSync(dir)) {
    return files;
  }

  const entries = readdirSync(dir);

  for (const entry of entries) {
    const fullPath = join(dir, entry);
    const stat = statSync(fullPath);

    if (stat.isDirectory() && entry !== 'node_modules' && entry !== 'dist') {
      files.push(...findTypeScriptFiles(fullPath));
    } else if (stat.isFile() && extname(entry) === '.ts' && !entry.endsWith('.d.ts')) {
      files.push(fullPath);
    }
  }

  return files;
}

function ensureDirectoryStructure(baseDir: string): void {
  const dirs = [
    baseDir,
    join(baseDir, 'datasources'),
    join(baseDir, 'pipes'),
  ];

  dirs.forEach(dir => {
    mkdirSync(dir, { recursive: true });
  });
}

async function createExampleFile(): Promise<void> {
  mkdirSync('./src', { recursive: true });

  const exampleContent = `import { z } from 'zod';
import {
  defineSchema,
  defineDataSource,
  definePipe,
  defineParameters,
  query,
  string,
  int64,
  stringParam,
  int64Param,
  count,
  timeGranularity,
  fromUnixTimestamp64Milli,
  param,
} from 'tinykit';

// Example events schema
export const eventsSchema = defineSchema({
  id: string({ jsonPath: '$.id' })('id'),
  timestamp: int64({ jsonPath: '$.timestamp' })('timestamp'),
  event_type: string({ jsonPath: '$.event_type' })('event_type'),
  user_id: string({ jsonPath: '$.user_id' })('user_id'),
});

// Example data source
export const eventsDataSource = defineDataSource({
  name: 'events__v1',
  version: 1,
  schema: eventsSchema,
  engine: 'MergeTree',
  sortingKey: ['timestamp', 'id'],
});

// Example pipe
export const getEventCountsPipe = definePipe({
  name: 'get_event_counts__v1',
  version: 1,
  schema: eventsSchema,
  parameters: defineParameters({
    start_time: int64Param({ required: true })('start_time'),
    end_time: int64Param()('end_time'),
  }),
  outputSchema: z.object({
    event_type: z.string(),
    count: z.number(),
  }),
}).endpoint((q, params) =>
  query(eventsSchema)
    .select('event_type')
    .selectRaw(\`\${count()} AS count\`)
    .from('events__v1')
    .where(\`timestamp >= \${param('start_time', 'Int64', true)}\`)
    .and(params.end_time ? \`timestamp <= \${param('end_time', 'Int64')}\` : '1=1')
    .groupBy('event_type')
    .orderBy('count', 'DESC')
);
`;

  writeFileSync('./src/example.ts', exampleContent);
  log.success('Created example file: ./src/example.ts');
}

