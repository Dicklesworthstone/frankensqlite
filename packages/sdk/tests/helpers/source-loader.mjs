// Runtime-only TypeScript loader for the Node SQL-reference SDK tests. It does
// not typecheck, replace Vitest, or substitute a fake @frankensqlite/core.
// Run from the repository root after installing workspace dev dependencies:
// node --experimental-loader=./packages/sdk/tests/helpers/source-loader.mjs \
//   --test packages/sdk/tests/queue*.test.mjs
import { access, readFile } from 'node:fs/promises';
const { default: ts } = await import(process.env.FSQLITE_TYPESCRIPT_MODULE ?? 'typescript');
const worker = new URL('../../../worker/src/index.ts', import.meta.url);

export async function resolve(specifier, context, nextResolve) {
  if (specifier === '@frankensqlite/worker') return { url: worker.href, shortCircuit: true };
  if (specifier.startsWith('.') && context.parentURL) {
    const url = new URL(specifier, context.parentURL);
    if (!/\.[a-z]+$/i.test(url.pathname)) {
      url.pathname += '.ts';
      try {
        await access(url);
        return { url: url.href, shortCircuit: true };
      } catch { /* Let Node report the original resolution failure. */ }
    }
  }
  return nextResolve(specifier, context);
}

export async function load(url, context, nextLoad) {
  if (url.endsWith('.ts')) {
    const source = await readFile(new URL(url), 'utf8');
    const output = ts.transpileModule(source, { fileName: url, compilerOptions: {
      module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022,
      verbatimModuleSyntax: true,
    } });
    return { format: 'module', source: output.outputText, shortCircuit: true };
  }
  return nextLoad(url, context);
}
