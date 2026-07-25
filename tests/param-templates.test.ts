import { expect, test, describe } from 'bun:test';

import {
  defineSchema,
  definePipe,
  defineParameters,
  stringParam,
  int64Param,
  query,
  param,
  string,
  int64,
  eq,
  gte,
  unsafeSQL,
} from '../src';

const schema = defineSchema({
  id: string('id', { jsonPath: '$.id' }),
  name: string('name', { jsonPath: '$.name' }),
  timestamp: int64('timestamp', { jsonPath: '$.timestamp' }),
});

describe('parameter templates (tpl)', () => {
  test('renders a default declared in defineParameters', () => {
    const pipe = definePipe({
      name: 'with_default__v1',
      schema,
      parameters: defineParameters({
        hours: int64Param('hours', { default: 24 }),
      }),
    }).endpoint((_q, _params, tpl) =>
      query(schema).selectRaw('*').from('t').where(`h >= ${tpl.hours}`)
    );

    expect(pipe.sql({})).toContain('h >= {{ Int64(hours, 24) }}');
  });

  test('renders required parameters with required=True', () => {
    const pipe = definePipe({
      name: 'required__v1',
      schema,
      parameters: defineParameters({
        userId: stringParam('userId', { required: true }),
      }),
    }).endpoint((_q, _params, tpl) =>
      query(schema).selectRaw('*').from('t').where(`user_id = ${tpl.userId}`)
    );

    expect(pipe.sql({})).toContain('user_id = {{ String(userId, required=True) }}');
  });

  test('quotes string defaults', () => {
    const pipe = definePipe({
      name: 'string_default__v1',
      schema,
      parameters: defineParameters({
        category: stringParam('category', { default: '' }),
      }),
    }).endpoint((_q, _params, tpl) =>
      query(schema).selectRaw('*').from('t').where(`c = ${tpl.category}`)
    );

    expect(pipe.sql({})).toContain("c = {{ String(category, '') }}");
  });

  test('omits the default when none is declared, and stays conditional', () => {
    const pipe = definePipe({
      name: 'no_default__v1',
      schema,
      parameters: defineParameters({
        days: int64Param('days'),
      }),
    }).endpoint((_q, _params, tpl) =>
      query(schema).selectRaw('*').from('t').where(`d >= ${tpl.days}`)
    );

    expect(pipe.sql({})).toContain(
      'd >= {% if defined(days) %}{{ Int64(days) }}{% end %}'
    );
  });

  test('the default is declared once and cannot drift', () => {
    const parameters = defineParameters({
      limit: int64Param('limit', { default: 10 }),
    });

    const pipe = definePipe({
      name: 'single_source__v1',
      schema,
      parameters,
    }).endpoint((_q, _params, tpl) =>
      query(schema).selectRaw('*').from('t').limit(tpl.limit)
    );

    expect(pipe.sql({})).toContain('LIMIT {{ Int64(limit, 10) }}');
    expect(parameters.limit.default).toBe(10);
  });
});

describe('limit and offset accept SQL expressions', () => {
  test('a template limit stays a parameter', () => {
    const pipe = definePipe({
      name: 'tpl_limit__v1',
      schema,
      parameters: defineParameters({
        limit: int64Param('limit', { default: 10 }),
      }),
    }).endpoint((_q, _params, tpl) =>
      query(schema).selectRaw('*').from('t').limit(tpl.limit)
    );

    const sql = pipe.sql({});
    expect(sql).toContain('LIMIT {{ Int64(limit, 10) }}');
    expect(sql).not.toContain('LIMIT 10\n');
  });

  test('resolved params still bake in, which is why tpl exists', () => {
    const pipe = definePipe({
      name: 'resolved_limit__v1',
      schema,
      parameters: defineParameters({
        limit: int64Param('limit', { default: 10 }),
      }),
    }).endpoint((_q, params) =>
      query(schema).selectRaw('*').from('t').limit(params.limit)
    );

    expect(pipe.sql({})).toContain('LIMIT 10');
  });

  test('a numeric limit is emitted literally', () => {
    expect(query(schema).selectRaw('*').from('t').limit(25).build()).toContain(
      'LIMIT 25'
    );
  });

  test('offset accepts a template', () => {
    const pipe = definePipe({
      name: 'tpl_offset__v1',
      schema,
      parameters: defineParameters({
        skip: int64Param('skip', { default: 0 }),
      }),
    }).endpoint((_q, _params, tpl) =>
      query(schema).selectRaw('*').from('t').limit(10).offset(tpl.skip)
    );

    expect(pipe.sql({})).toContain('OFFSET {{ Int64(skip, 0) }}');
  });
});

describe('standalone param()', () => {
  test('supports a default via options', () => {
    expect(String(param('hours', 'Int64', { default: 24 }))).toBe(
      '{{ Int64(hours, 24) }}'
    );
  });

  test('quotes string defaults', () => {
    expect(String(param('category', 'String', { default: 'all' }))).toBe(
      "{{ String(category, 'all') }}"
    );
  });

  test('keeps the boolean third argument working', () => {
    expect(String(param('userId', 'String', true))).toBe(
      '{{ String(userId, required=True) }}'
    );
    expect(String(param('userId', 'String', false))).toBe(
      '{{ String(userId) }}'
    );
  });

  test('required wins over a default, since the default is unreachable', () => {
    expect(
      String(param('userId', 'String', { required: true, default: 'x' }))
    ).toBe('{{ String(userId, required=True) }}');
  });

  test('interpolates into a template literal', () => {
    expect(`WHERE x = ${param('x', 'Int64', { default: 1 })}`).toBe(
      'WHERE x = {{ Int64(x, 1) }}'
    );
  });
});

describe('escapeValue treats unbranded values as data', () => {
  test('quotes a plain string', () => {
    expect(query(schema).selectRaw('*').from('t').where('name', eq('bob')).build()).toContain(
      "WHERE name = 'bob'"
    );
  });

  test('escapes embedded single quotes', () => {
    expect(
      query(schema).selectRaw('*').from('t').where('name', eq("O'Brien")).build()
    ).toContain("WHERE name = 'O''Brien'");
  });

  test('does not let parentheses turn a value into SQL', () => {
    const hostile = "x') OR 1=1 --";
    const sql = query(schema).selectRaw('*').from('t').where('name', eq(hostile)).build();

    expect(sql).toContain("WHERE name = 'x'') OR 1=1 --'");
    expect(sql).not.toContain("OR 1=1 --'\n");
  });

  test('does not let braces turn a value into SQL', () => {
    const hostile = '{{ String(admin) }}';
    const sql = query(schema).selectRaw('*').from('t').where('name', eq(hostile)).build();

    expect(sql).toContain("WHERE name = '{{ String(admin) }}'");
  });

  test('unsafeSQL is the explicit opt-in for a literal expression', () => {
    const sql = query(schema)
      .selectRaw('*')
      .from('t')
      .where('timestamp', gte(unsafeSQL('now() - INTERVAL 1 DAY')))
      .build();

    expect(sql).toContain('WHERE timestamp >= now() - INTERVAL 1 DAY');
  });

  test('a branded expression is still inlined', () => {
    const sql = query(schema)
      .selectRaw('*')
      .from('t')
      .where('timestamp', gte(param('since', 'DateTime', { required: true })))
      .build();

    expect(sql).toContain('WHERE timestamp >= {{ DateTime(since, required=True) }}');
  });
});
