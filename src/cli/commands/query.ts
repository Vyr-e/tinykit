import { execSync, spawn } from 'child_process';
import { log } from '../utils/terminal';
import { readFileSync, existsSync, writeFileSync } from 'fs';
import { join, resolve } from 'path';
import { tmpdir } from 'os';

export interface QueryOptions {
  sql?: string;
  file?: string;
  pipe?: string;
  format: 'json' | 'csv' | 'human';
  limit?: number;
  stats: boolean;
  token?: string;
  config?: string;
}

export async function queryCommand(options: QueryOptions, args: string[]) {
  try {
    // Check if Tinybird CLI is installed
    await ensureTinybirdCLI();

    // Prepare environment and arguments for tb command
    const env = await prepareEnvironment(options);
    const tbArgs = buildTbArgs(options, args);

    log.dim(`Running: tb ${tbArgs.join(' ')}`);

    // Execute tb command
    await executeTbCommand(tbArgs, env);

  } catch (error) {
    log.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  }
}

async function ensureTinybirdCLI(): Promise<void> {
  try {
    execSync('tb --version', { stdio: 'ignore' });
  } catch {
    log.error('Tinybird CLI not found. Please install it first:');
    log.plain('  curl https://tinybird.co | sh');
    log.plain('');
    log.plain('Then authenticate with:');
    log.plain('  tb login');
    process.exit(1);
  }
}

async function prepareEnvironment(options: QueryOptions): Promise<NodeJS.ProcessEnv> {
  const env = { ...process.env };

  // Set token if provided
  const token = getToken(options);
  if (token) {
    env.TB_TOKEN = token;
  }

  return env;
}

function buildTbArgs(options: QueryOptions, args: string[]): string[] {
  const tbArgs: string[] = [];

  if (options.sql) {
    // Direct SQL execution
    tbArgs.push('sql');
    tbArgs.push('-q', options.sql);
  } else if (options.file) {
    // SQL from file
    if (!existsSync(options.file)) {
      throw new Error(`File not found: ${options.file}`);
    }
    tbArgs.push('sql');
    tbArgs.push('-f', resolve(options.file));
  } else if (options.pipe) {
    // Pipe execution with parameters
    tbArgs.push('pipe');
    tbArgs.push('data', options.pipe);
    
    // Add any additional args as pipe parameters
    for (const arg of args) {
      if (arg.includes('=')) {
        const [key, value] = arg.split('=', 2);
        tbArgs.push('--param', `${key}=${value}`);
      }
    }
  } else if (args.length > 0) {
    // SQL from command line arguments
    const query = args.join(' ');
    const tempFile = join(tmpdir(), `tinykit-query-${Date.now()}.sql`);
    writeFileSync(tempFile, query, 'utf-8');
    
    tbArgs.push('sql');
    tbArgs.push('-f', tempFile);
  } else {
    throw new Error('No query provided. Use --sql, --file, --pipe, or provide query as arguments.');
  }

  // Add format option
  tbArgs.push('--format', options.format);

  // Add limit if specified
  if (options.limit) {
    tbArgs.push('--limit', options.limit.toString());
  }

  // Add stats option (Tinybird CLI uses --stats/--no-stats)
  if (options.stats) {
    tbArgs.push('--stats');
  } else {
    tbArgs.push('--no-stats');
  }

  return tbArgs;
}

async function executeTbCommand(args: string[], env: NodeJS.ProcessEnv): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = spawn('tb', args, {
      stdio: 'inherit',
      env,
      shell: process.platform === 'win32'
    });

    child.on('close', (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`Tinybird CLI exited with code ${code}`));
      }
    });

    child.on('error', (error) => {
      if (error.message.includes('ENOENT')) {
        reject(new Error('Tinybird CLI not found. Please install tinybird-cli first.'));
      } else {
        reject(error);
      }
    });
  });
}

function getToken(options: QueryOptions): string | undefined {
  // 1. Command line option
  if (options.token) {
    return options.token;
  }

  // 2. Environment variable
  if (process.env.TINYBIRD_TOKEN) {
    return process.env.TINYBIRD_TOKEN;
  }

  // 3. Config file
  if (options.config) {
    return readTokenFromConfig(options.config);
  }

  // 4. Default config locations
  const defaultConfigs = [
    '.tinykitrc',
    '.tinybird',
    join(process.env.HOME || process.env.USERPROFILE || '', '.tinykitrc')
  ];

  for (const configPath of defaultConfigs) {
    if (existsSync(configPath)) {
      const token = readTokenFromConfig(configPath);
      if (token) return token;
    }
  }

  return undefined;
}

function readTokenFromConfig(configPath: string): string | undefined {
  try {
    const content = readFileSync(configPath, 'utf-8');
    
    // Try JSON format
    try {
      const json = JSON.parse(content);
      return json.token || json.TINYBIRD_TOKEN;
    } catch {
      // Try simple key=value format
      const lines = content.split('\n');
      for (const line of lines) {
        const trimmed = line.trim();
        if (trimmed.startsWith('token=') || trimmed.startsWith('TINYBIRD_TOKEN=')) {
          return trimmed.split('=')[1]?.trim();
        }
      }
    }
  } catch {
    // Ignore config read errors
  }
  
  return undefined;
}