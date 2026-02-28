/// <reference path="../.astro/types.d.ts" />
/// <reference types="astro/client" />

// D1-compatible adapter injected by middleware
import type { D1Database } from './lib/d1-adapter';

declare namespace App {
  interface Locals {
    runtime: {
      env: {
        DB: D1Database;
      };
    };
  }
}
