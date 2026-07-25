import { expect, test, describe } from 'bun:test';

import { defineSchema, defineDataSource, string, int64, array } from '../src';
import { generateDatasourceFile } from '../src/cli/generators/datasource';

describe('array JSONPaths', () => {
  test('appends the [:] operator to array columns', () => {
    const schema = defineSchema({
      id: string('id'),
      tags: array('tags', string('tags').schema, { innerType: 'String' }),
    });

    const file = generateDatasourceFile(
      defineDataSource({
        name: 'items__v1',
        schema,
        engine: 'MergeTree',
        sortingKey: ['id'],
      })
    );

    expect(file).toContain('`tags` Array(String) `json:$.tags[:]`');
  });

  test('leaves scalar columns alone', () => {
    const schema = defineSchema({
      id: string('id'),
      count: int64('count'),
    });

    const file = generateDatasourceFile(
      defineDataSource({
        name: 'scalars__v1',
        schema,
        engine: 'MergeTree',
        sortingKey: ['id'],
      })
    );

    expect(file).toContain('`id` String `json:$.id`');
    expect(file).toContain('`count` Int64 `json:$.count`');
  });

  test('an explicit jsonPath still wins', () => {
    const schema = defineSchema({
      id: string('id'),
      tags: array('tags', string('tags').schema, {
        innerType: 'String',
        jsonPath: '$.custom.path[:]',
      }),
    });

    const file = generateDatasourceFile(
      defineDataSource({
        name: 'custom__v1',
        schema,
        engine: 'MergeTree',
        sortingKey: ['id'],
      })
    );

    expect(file).toContain('`json:$.custom.path[:]`');
  });
});
