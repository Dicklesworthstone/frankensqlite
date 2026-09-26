// Orchestration-contract loader: source manifest/storage/ACK APIs are replaced
// by explicit SQLite-backed reference fixtures. This does NOT certify the
// production bootstrap receiver, source store, fanout, or FrankenSQLite engine.
import { readFile } from 'node:fs/promises';
const { default: ts } = await import(process.env.FSQLITE_TYPESCRIPT_MODULE ?? 'typescript');
const fixture = new URL('./bootstrap-transfer-fixture.mjs', import.meta.url).href;
export async function resolve(specifier, context, next) {
  if (context.parentURL?.endsWith('/changeset-bootstrap-transfer.ts') &&
      (specifier === './changeset-bootstrap' || specifier === './changeset-outbox-store')) {
    return { url: fixture, shortCircuit: true };
  }
  return next(specifier, context);
}
export async function load(url, context, next) {
  if (url.endsWith('/changeset-bootstrap-transfer.ts')) {
    const source = await readFile(new URL(url), 'utf8');
    return { format: 'module', shortCircuit: true, source: ts.transpileModule(source, {
      fileName: url, compilerOptions: { module: ts.ModuleKind.ESNext,
        target: ts.ScriptTarget.ES2022, verbatimModuleSyntax: true },
    }).outputText };
  }
  return next(url, context);
}
