// D1-compatible adapter wrapping better-sqlite3
// Exposes the same API as Cloudflare D1 so all existing db.ts functions work unchanged
// D1: db.prepare(sql).bind(...params).first<T>() / .all<T>() / .run()
// better-sqlite3: db.prepare(sql).get(...params) / .all(...params) / .run(...params)
// Key difference: D1 uses numbered params (?1, ?2), better-sqlite3 only works with unnamed (?)

import Database from 'better-sqlite3';

interface D1Result<T = unknown> {
  results: T[];
  success: boolean;
  meta: Record<string, unknown>;
}

interface D1PreparedStatement {
  bind(...params: unknown[]): {
    first<T = unknown>(): Promise<T | null>;
    all<T = unknown>(): Promise<D1Result<T>>;
    run(): Promise<{ success: boolean; meta: Record<string, unknown> }>;
  };
  first<T = unknown>(): Promise<T | null>;
  all<T = unknown>(): Promise<D1Result<T>>;
  run(): Promise<{ success: boolean; meta: Record<string, unknown> }>;
}

export interface D1Database {
  prepare(sql: string): D1PreparedStatement;
}

// Convert D1 numbered params (?1, ?2) to unnamed (?) for better-sqlite3
function normalizeParams(sql: string): string {
  return sql.replace(/\?(\d+)/g, '?');
}

export function createD1Adapter(dbPath: string): D1Database {
  const db = new Database(dbPath, { readonly: true, fileMustExist: true });

  // Self-heal: force DELETE journal mode to prevent WAL issues on :ro mounts.
  // WAL mode DBs try to create a WAL file even for reads, causing 500 errors.
  try {
    db.pragma('journal_mode = DELETE');
  } catch {
    // Expected to fail on truly read-only filesystems — safe to ignore
  }

  return {
    prepare(sql: string): D1PreparedStatement {
      let stmt: ReturnType<InstanceType<typeof Database>['prepare']>;
      try {
        stmt = db.prepare(normalizeParams(sql));
      } catch (err: any) {
        if (err?.message?.includes('readonly database')) {
          throw new Error(
            `SQLite readonly error — DB may be in WAL mode. ` +
            `Fix: sqlite3 ${dbPath} "PRAGMA journal_mode=DELETE; VACUUM;" then re-upload. ` +
            `Original: ${err.message}`
          );
        }
        throw err;
      }

      function makeBindResult(params: unknown[]) {
        return {
          async first<T = unknown>(): Promise<T | null> {
            const row = stmt.get(...params);
            return (row as T) ?? null;
          },
          async all<T = unknown>(): Promise<D1Result<T>> {
            const rows = stmt.all(...params);
            return { results: rows as T[], success: true, meta: {} };
          },
          async run() {
            stmt.run(...params);
            return { success: true, meta: {} };
          },
        };
      }

      return {
        bind(...params: unknown[]) {
          return makeBindResult(params);
        },
        // Unbound versions (no params)
        async first<T = unknown>(): Promise<T | null> {
          const row = stmt.get();
          return (row as T) ?? null;
        },
        async all<T = unknown>(): Promise<D1Result<T>> {
          const rows = stmt.all();
          return { results: rows as T[], success: true, meta: {} };
        },
        async run() {
          stmt.run();
          return { success: true, meta: {} };
        },
      };
    },
  };
}
