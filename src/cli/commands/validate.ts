import prompts from 'prompts';
import { readdirSync, readFileSync, existsSync } from 'fs';
import { join } from 'path';
import { log } from '../utils/terminal';

interface ValidateOptions {
  dir: string;
}

interface ValidationResult {
  file: string;
  type: 'datasource' | 'pipe';
  valid: boolean;
  errors: string[];
  warnings: string[];
}

export async function validateCommand(options: ValidateOptions): Promise<void> {
  log.info('🔍 Validating Tinybird files...\n');

  if (!existsSync(options.dir)) {
    log.error(`Directory not found: ${options.dir}`);

    const { create } = await prompts({
      type: 'confirm',
      name: 'create',
      message: `Create directory ${options.dir}?`,
      initial: true,
    });

    if (!create) {
      return;
    }

    log.info('Run `tinykit generate` to create Tinybird files');
    return;
  }

  const datasourcesDir = join(options.dir, 'datasources');
  const pipesDir = join(options.dir, 'pipes');

  const results: ValidationResult[] = [];

  // Validate datasources
  if (existsSync(datasourcesDir)) {
    const datasourceFiles = readdirSync(datasourcesDir)
      .filter(file => file.endsWith('.datasource'));

    log.info(`Found ${datasourceFiles.length} datasource file(s)`);

    for (const file of datasourceFiles) {
      const filePath = join(datasourcesDir, file);
      const result = await validateDatasource(filePath);
      results.push(result);
    }
  }

  // Validate pipes
  if (existsSync(pipesDir)) {
    const pipeFiles = readdirSync(pipesDir)
      .filter(file => file.endsWith('.pipe'));

    log.info(`Found ${pipeFiles.length} pipe file(s)`);

    for (const file of pipeFiles) {
      const filePath = join(pipesDir, file);
      const result = await validatePipe(filePath);
      results.push(result);
    }
  }

  // Display results
  displayValidationResults(results);

  // Ask user what to do with invalid files
  const invalidFiles = results.filter(r => !r.valid);
  if (invalidFiles.length > 0) {
    const { action } = await prompts({
      type: 'select',
      name: 'action',
      message: 'What would you like to do with invalid files?',
      choices: [
        { title: 'Show detailed errors', value: 'details' },
        { title: 'Regenerate invalid files', value: 'regenerate' },
        { title: 'Continue anyway', value: 'continue' },
      ],
      initial: 0,
    });

    if (action === 'details') {
      showDetailedErrors(invalidFiles);
    } else if (action === 'regenerate') {
      log.info('Run `tinykit generate` to regenerate files');
    }
  }
}

async function validateDatasource(filePath: string): Promise<ValidationResult> {
  const errors: string[] = [];
  const warnings: string[] = [];
  const content = readFileSync(filePath, 'utf-8');

  // Basic datasource validation
  if (!content.includes('SCHEMA >')) {
    errors.push('Missing SCHEMA section');
  }

  if (!content.includes('ENGINE ')) {
    errors.push('Missing ENGINE specification');
  }

  // Check for required patterns
  if (!content.match(/`\w+`\s+\w+/)) {
    errors.push('No valid column definitions found');
  }

  // Check for version
  if (!content.includes('VERSION ')) {
    warnings.push('No version specified - consider adding VERSION');
  }

  // Check for sorting key on MergeTree engines
  if (content.includes('ENGINE "MergeTree"') && !content.includes('ENGINE_SORTING_KEY')) {
    warnings.push('MergeTree engine without sorting key may impact performance');
  }

  return {
    file: filePath,
    type: 'datasource',
    valid: errors.length === 0,
    errors,
    warnings,
  };
}

async function validatePipe(filePath: string): Promise<ValidationResult> {
  const errors: string[] = [];
  const warnings: string[] = [];
  const content = readFileSync(filePath, 'utf-8');

  // Basic pipe validation
  if (!content.includes('NODE ')) {
    errors.push('Missing NODE specification');
  }

  if (!content.includes('SQL >')) {
    errors.push('Missing SQL section');
  }

  // Check for SQL content
  if (!content.includes('SELECT')) {
    errors.push('No SELECT statement found in SQL');
  }

  if (!content.includes('FROM')) {
    errors.push('No FROM clause found in SQL');
  }

  // Check for version
  if (!content.includes('VERSION ')) {
    warnings.push('No version specified - consider adding VERSION');
  }

  // Check for proper parameter syntax
  const paramMatches = content.match(/{{.*?}}/g);
  if (paramMatches) {
    paramMatches.forEach(param => {
      if (!param.match(/{{ (String|Int64|Float64|DateTime|Boolean)\(/)) {
        warnings.push(`Parameter ${param} may have incorrect syntax`);
      }
    });
  }

  return {
    file: filePath,
    type: 'pipe',
    valid: errors.length === 0,
    errors,
    warnings,
  };
}

function displayValidationResults(results: ValidationResult[]): void {
  const valid = results.filter(r => r.valid);
  const invalid = results.filter(r => !r.valid);
  const withWarnings = results.filter(r => r.warnings.length > 0);

  log.plain('');
  log.success(`${valid.length} file(s) are valid`);

  if (invalid.length > 0) {
    log.error(`${invalid.length} file(s) have errors`);
  }

  if (withWarnings.length > 0) {
    log.warning(`${withWarnings.length} file(s) have warnings`);
  }

  // Summary by type
  const datasources = results.filter(r => r.type === 'datasource');
  const pipes = results.filter(r => r.type === 'pipe');

  if (datasources.length > 0) {
    const validDatasources = datasources.filter(r => r.valid).length;
    log.dim(`  Datasources: ${validDatasources}/${datasources.length} valid`);
  }

  if (pipes.length > 0) {
    const validPipes = pipes.filter(r => r.valid).length;
    log.dim(`  Pipes: ${validPipes}/${pipes.length} valid`);
  }

  // Show file-by-file status
  log.plain('');
  results.forEach(result => {
    const icon = result.valid ? '✅' : '❌';
    const warnings = result.warnings.length > 0 ? ` (${result.warnings.length} warnings)` : '';
    log.plain(`${icon} ${result.file}${warnings}`);
  });
}

function showDetailedErrors(invalidFiles: ValidationResult[]): void {
  log.plain('');
  log.bold('Detailed validation errors:');

  invalidFiles.forEach(result => {
    log.plain('');
    log.error(`${result.file}:`);

    result.errors.forEach(error => {
      log.plain(`  • ${error}`);
    });

    if (result.warnings.length > 0) {
      log.warning('Warnings:');
      result.warnings.forEach(warning => {
        log.plain(`  • ${warning}`);
      });
    }
  });
}