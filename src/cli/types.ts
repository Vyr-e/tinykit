export interface GenerateOptions {
  file?: string;
  dir: string;
  watch?: boolean;
  dryRun?: boolean;
}

export interface InitOptions {
  dir: string;
}

export interface TinybirdConfig {
  datasources?: string[];
  pipes?: string[];
  dependencies?: string[];
}

export interface FileGenerationResult {
  path: string;
  content: string;
  type: 'datasource' | 'pipe';
  name: string;
}