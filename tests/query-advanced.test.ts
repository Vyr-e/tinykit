import { expect, test, describe } from 'bun:test';
import {
  query,
  defineSchema,
  string,
  int64,
  rowNumber,
  rank,
  lag,
  lead,
  firstValue,
  lastValue,
} from '../src';

describe('Advanced Query Builder Features', () => {
  const testSchema = defineSchema({
    id: string('id'),
    name: string('name'),
    age: int64('age'),
    department: string('department'),
    salary: int64('salary'),
    manager_id: string('manager_id'),
    employee_id: string('employee_id'),
  });

  describe('UNION operations', () => {
    test('should build UNION ALL query', () => {
      const q1 = query(testSchema).select('id', 'name').from('employees');
      const q2 = query(testSchema).select('id', 'name').from('contractors');
      const q = q1.unionAll(q2);
      
      expect(q.build()).toBe(
        'SELECT id, name\nFROM employees\nUNION ALL\nSELECT id, name\nFROM contractors'
      );
    });

    test('should build UNION DISTINCT query', () => {
      const q1 = query(testSchema).select('id', 'name').from('employees');
      const q2 = query(testSchema).select('id', 'name').from('contractors');
      const q = q1.unionDistinct(q2);
      
      expect(q.build()).toBe(
        'SELECT id, name\nFROM employees\nUNION\nSELECT id, name\nFROM contractors'
      );
    });

    test('should build INTERSECT query', () => {
      const q1 = query(testSchema).select('id', 'name').from('employees');
      const q2 = query(testSchema).select('id', 'name').from('managers');
      const q = q1.intersect(q2);
      
      expect(q.build()).toBe(
        'SELECT id, name\nFROM employees\nINTERSECT\nSELECT id, name\nFROM managers'
      );
    });

    test('should build EXCEPT query', () => {
      const q1 = query(testSchema).select('id', 'name').from('employees');
      const q2 = query(testSchema).select('id', 'name').from('contractors');
      const q = q1.except(q2);
      
      expect(q.build()).toBe(
        'SELECT id, name\nFROM employees\nEXCEPT\nSELECT id, name\nFROM contractors'
      );
    });

    test('should chain multiple UNION operations', () => {
      const q1 = query(testSchema).select('id', 'name').from('employees');
      const q2 = query(testSchema).select('id', 'name').from('contractors');
      const q3 = query(testSchema).select('id', 'name').from('freelancers');
      const q = q1.unionAll(q2).unionAll(q3);
      
      expect(q.build()).toBe(
        'SELECT id, name\nFROM employees\nUNION ALL\nSELECT id, name\nFROM contractors\nUNION ALL\nSELECT id, name\nFROM freelancers'
      );
    });
  });

  describe('Subqueries', () => {
    test('should build EXISTS subquery', () => {
      const subq = query(testSchema).select('id').from('orders').where('orders.employee_id = employees.id');
      const q = query(testSchema)
        .select('id', 'name')
        .from('employees')
        .where(query(testSchema).existsSubquery(subq));
      
      expect(q.build()).toContain('EXISTS (SELECT id\nFROM orders\nWHERE orders.employee_id = employees.id)');
    });

    test('should build NOT EXISTS subquery', () => {
      const subq = query(testSchema).select('id').from('orders').where('orders.employee_id = employees.id');
      const q = query(testSchema)
        .select('id', 'name')
        .from('employees')
        .where(query(testSchema).notExistsSubquery(subq));
      
      expect(q.build()).toContain('NOT EXISTS (SELECT id\nFROM orders\nWHERE orders.employee_id = employees.id)');
    });

    test('should build IN subquery', () => {
      const subq = query(testSchema).select('manager_id').from('departments');
      const q = query(testSchema)
        .select('id', 'name')
        .from('employees')
        .where(query(testSchema).inSubquery('id', subq));
      
      expect(q.build()).toContain('id IN (SELECT manager_id\nFROM departments)');
    });

    test('should build NOT IN subquery', () => {
      const subq = query(testSchema).select('employee_id').from('terminated');
      const q = query(testSchema)
        .select('id', 'name')
        .from('employees')
        .where(query(testSchema).notInSubquery('id', subq));
      
      expect(q.build()).toContain('id NOT IN (SELECT employee_id\nFROM terminated)');
    });

    test('should build subquery in FROM clause', () => {
      const subq = query(testSchema).select('department', 'salary').from('employees').where('salary > 50000');
      const q = query(testSchema)
        .subquery('high_earners', subq)
        .select('department')
        .selectRaw('AVG(salary) as avg_salary')
        .groupBy('department');
      
      expect(q.build()).toContain('FROM (SELECT department, salary\nFROM employees\nWHERE salary > 50000) AS high_earners');
    });
  });

  describe('Window Functions', () => {
    test('should generate ROW_NUMBER window function', () => {
      const windowFunc = rowNumber('department', 'salary DESC');
      expect(windowFunc).toBe('ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC)');
    });

    test('should generate ROW_NUMBER without partition', () => {
      const windowFunc = rowNumber(undefined, 'salary DESC');
      expect(windowFunc).toBe('ROW_NUMBER() OVER (ORDER BY salary DESC)');
    });

    test('should generate RANK window function', () => {
      const windowFunc = rank('department', 'salary DESC');
      expect(windowFunc).toBe('RANK() OVER (PARTITION BY department ORDER BY salary DESC)');
    });

    test('should generate LAG window function', () => {
      const windowFunc = lag('salary', 2, '0', 'department', 'id');
      expect(windowFunc).toBe('LAG(salary, 2, 0) OVER (PARTITION BY department ORDER BY id)');
    });

    test('should generate LAG with defaults', () => {
      const windowFunc = lag('salary');
      expect(windowFunc).toBe('LAG(salary) OVER ()');
    });

    test('should generate LEAD window function', () => {
      const windowFunc = lead('salary', 2, '0', 'department', 'id');
      expect(windowFunc).toBe('LEAD(salary, 2, 0) OVER (PARTITION BY department ORDER BY id)');
    });

    test('should generate FIRST_VALUE window function', () => {
      const windowFunc = firstValue('salary', 'department', 'salary DESC');
      expect(windowFunc).toBe('FIRST_VALUE(salary) OVER (PARTITION BY department ORDER BY salary DESC)');
    });

    test('should generate LAST_VALUE window function', () => {
      const windowFunc = lastValue('salary', 'department', 'salary DESC');
      expect(windowFunc).toBe('LAST_VALUE(salary) OVER (PARTITION BY department ORDER BY salary DESC)');
    });

    test('should use window functions in queries', () => {
      const q = query(testSchema)
        .select('id', 'name', 'salary', 'department')
        .selectRaw(`${rowNumber('department', 'salary DESC')} as rank_in_dept`)
        .selectRaw(`${lag('salary', 2, '0', 'department', 'id')} as prev_salary`)
        .from('employees');
      
      const sql = q.build();
      expect(sql).toContain('ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank_in_dept');
      expect(sql).toContain('LAG(salary, 2, 0) OVER (PARTITION BY department ORDER BY id) as prev_salary');
    });
  });

  describe('Complex Query Combinations', () => {
    test('should build complex query with unions, subqueries, and window functions', () => {
      const currentEmployees = query(testSchema)
        .select('id', 'name', 'department', 'salary')
        .selectRaw(`${rowNumber('department', 'salary DESC')} as dept_rank`)
        .from('employees')
        .where('status = "active"');

      const formerEmployees = query(testSchema)
        .select('id', 'name', 'department', 'salary')
        .selectRaw('NULL as dept_rank')
        .from('employees_archive')
        .where('status = "terminated"');

      const highPerformers = query(testSchema)
        .select('employee_id')
        .from('performance_reviews')
        .where('score >= 4.5');

      const finalQuery = currentEmployees
        .unionAll(formerEmployees)
        .where(query(testSchema).inSubquery('id', highPerformers));

      const sql = finalQuery.build();
      expect(sql).toContain('ROW_NUMBER()');
      expect(sql).toContain('UNION ALL');
      expect(sql).toContain('IN (SELECT employee_id');
    });
  });
});