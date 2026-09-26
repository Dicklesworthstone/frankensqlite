// Load the actual TypeScript sources. No SDK modules are substituted.
import { readFile } from 'node:fs/promises';
import { stripTypeScriptTypes } from 'node:module';
export async function resolve(specifier, context, nextResolve) {
  if (specifier.startsWith('.') && !/\.[cm]?[jt]s$/.test(specifier)) {
    return nextResolve(`${specifier}.ts`, context);
  }
  return nextResolve(specifier, context);
}
export async function load(url, context, nextLoad) {
  if (url.endsWith('.ts')) {
    return { format: 'module', shortCircuit: true,
      source: stripTypeScriptTypes(await readFile(new URL(url), 'utf8'), { mode: 'transform', sourceUrl: url }) };
  }
  return nextLoad(url, context);
}
