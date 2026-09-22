// Node 22.16+ resolves production TS without installing or building the SDK.
// Run with --experimental-transform-types; only extensionless relative TS is resolved.
export async function resolve(specifier, context, nextResolve) {
  if (specifier.startsWith('.') && !/\.[cm]?[jt]s$/.test(specifier)) {
    return nextResolve(`${specifier}.ts`, context);
  }
  return nextResolve(specifier, context);
}
