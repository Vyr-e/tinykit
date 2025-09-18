import { pathToFileURL } from 'url';
import type { DataSourceConfig, PipeConfig } from '../../types';
import { log } from './terminal';
import { isTinybirdClientSymbol, datasourcesSymbol, pipesSymbol } from '../../client';

export interface DiscoveredDataSource {
  name: string;
  exportName: string;
  config: DataSourceConfig<any>;
  sourceFile: string;
}

export interface DiscoveredPipe {
  name: string;
  exportName: string;
  config: PipeConfig<any>;
  sourceFile: string;
}

export interface DiscoveredTinybirdClient {
  exportName: string;
  datasources: Record<string, DataSourceConfig<any>>;
  pipes: Record<string, PipeConfig<any>>;
  sourceFile: string;
}

export interface AnalysisResult {
  datasources: DiscoveredDataSource[];
  pipes: DiscoveredPipe[];
  tinybirdClients: DiscoveredTinybirdClient[];
  errors: string[];
}

/**
 * Analyze a TypeScript file to discover defineDataSource and definePipe exports
 */
export async function analyzeTypeScriptFile(filePath: string): Promise<AnalysisResult> {
  const result: AnalysisResult = {
    datasources: [],
    pipes: [],
    tinybirdClients: [],
    errors: []
  };

  try {
    log.dim(`    Importing ${filePath}...`);
    
    // Convert file path to file URL for dynamic import
    const fileUrl = pathToFileURL(filePath).href;
    
    // Dynamic import the TypeScript file
    const module = await import(fileUrl);
    
    // Analyze all exports in the module
    for (const [exportName, exportValue] of Object.entries(module)) {
      if (!exportValue || typeof exportValue !== 'object') {
        continue;
      }

      // Check if this export looks like a Tinybird client instance
      if (isTinybirdClient(exportValue)) {
        log.dim(`    Found Tinybird client: ${exportName}`);
        
        const datasources: Record<string, DataSourceConfig<any>> = {};
        const pipes: Record<string, PipeConfig<any>> = {};
        
        // Extract datasources from client config via symbol
        if (exportValue[datasourcesSymbol]) {
          for (const [dsKey, dsValue] of Object.entries(exportValue[datasourcesSymbol])) {
            if (isDataSourceConfig(dsValue)) {
              datasources[dsKey] = dsValue;
              result.datasources.push({
                name: dsValue.name,
                exportName: `${exportName}.datasources.${dsKey}`,
                config: dsValue,
                sourceFile: filePath
              });
            }
          }
        }
        
        // Extract pipes from client config via symbol
        if (exportValue[pipesSymbol]) {
          for (const [pipeKey, pipeValue] of Object.entries(exportValue[pipesSymbol])) {
            if (isPipeConfig(pipeValue)) {
              pipes[pipeKey] = pipeValue;
              result.pipes.push({
                name: pipeValue.name,
                exportName: `${exportName}.pipes.${pipeKey}`,
                config: pipeValue,
                sourceFile: filePath
              });
            }
          }
        }
        
        result.tinybirdClients.push({
          exportName,
          datasources,
          pipes,
          sourceFile: filePath
        });
      }
      
      // Also check for standalone datasource/pipe exports (backward compatibility)
      else if (isDataSourceConfig(exportValue)) {
        log.dim(`    Found standalone datasource: ${exportName}`);
        result.datasources.push({
          name: exportValue.name,
          exportName,
          config: exportValue,
          sourceFile: filePath
        });
      }
      
      else if (isPipeConfig(exportValue)) {
        log.dim(`    Found standalone pipe: ${exportName}`);
        result.pipes.push({
          name: exportValue.name,
          exportName,
          config: exportValue,
          sourceFile: filePath
        });
      }
    }

    const totalDatasources = result.datasources.length;
    const totalPipes = result.pipes.length;
    const totalClients = result.tinybirdClients.length;
    
    log.dim(`    Found ${totalClients} client(s), ${totalDatasources} datasource(s), ${totalPipes} pipe(s)`);

  } catch (error) {
    const errorMsg = `Failed to analyze ${filePath}: ${error instanceof Error ? error.message : String(error)}`;
    result.errors.push(errorMsg);
    log.warning(`    ${errorMsg}`);
  }

  return result;
}

/**
 * Type guard to check if an object is a DataSourceConfig
 */
function isDataSourceConfig(obj: any): obj is DataSourceConfig<any> {
  return obj &&
    typeof obj === 'object' &&
    typeof obj.name === 'string' &&
    obj.schema &&
    typeof obj.schema === 'object' &&
    typeof obj.engine === 'string' &&
    ['MergeTree', 'ReplacingMergeTree', 'SummingMergeTree', 'AggregatingMergeTree'].includes(obj.engine);
}

/**
 * Type guard to check if an object is a PipeConfig
 */
function isPipeConfig(obj: any): obj is PipeConfig<any> {
  return obj &&
    typeof obj === 'object' &&
    typeof obj.name === 'string' &&
    obj.parameters &&
    typeof obj.parameters === 'object' &&
    typeof obj.sql === 'function';
}

/**
 * Interface representing a Tinybird client instance with accessible properties via symbols
 */
interface TinybirdClientInstance {
  [isTinybirdClientSymbol]: true;
  [datasourcesSymbol]?: Record<string, DataSourceConfig<any>>;
  [pipesSymbol]?: Record<string, any>;
}

/**
 * Type guard to check if an object is a Tinybird client instance
 */
function isTinybirdClient(obj: any): obj is TinybirdClientInstance {
  return obj && typeof obj === 'object' && obj[isTinybirdClientSymbol] === true;
}

/**
 * Batch analyze multiple TypeScript files
 */
export async function analyzeTypeScriptFiles(filePaths: string[]): Promise<AnalysisResult> {
  const combinedResult: AnalysisResult = {
    datasources: [],
    pipes: [],
    tinybirdClients: [],
    errors: []
  };

  for (const filePath of filePaths) {
    const fileResult = await analyzeTypeScriptFile(filePath);
    
    combinedResult.datasources.push(...fileResult.datasources);
    combinedResult.pipes.push(...fileResult.pipes);
    combinedResult.tinybirdClients.push(...fileResult.tinybirdClients);
    combinedResult.errors.push(...fileResult.errors);
  }

  return combinedResult;
}

/**
 * Helper to check if a discovered datasource has any naming conflicts
 */
export function validateNaming(analysis: AnalysisResult): string[] {
  const warnings: string[] = [];
  
  // Check for datasource name conflicts
  const dsNames = analysis.datasources.map(ds => ds.name);
  const duplicateDsNames = dsNames.filter((name, index) => dsNames.indexOf(name) !== index);
  
  if (duplicateDsNames.length > 0) {
    warnings.push(`Duplicate datasource names found: ${Array.from(new Set(duplicateDsNames)).join(', ')}`);
  }
  
  // Check for pipe name conflicts
  const pipeNames = analysis.pipes.map(pipe => pipe.name);
  const duplicatePipeNames = pipeNames.filter((name, index) => pipeNames.indexOf(name) !== index);
  
  if (duplicatePipeNames.length > 0) {
    warnings.push(`Duplicate pipe names found: ${Array.from(new Set(duplicatePipeNames)).join(', ')}`);
  }
  
  return warnings;
}