// Resolution only: execute production .ts modules, never substitute SDK backends.
export async function resolve(specifier, context, nextResolve) {
  if (specifier.startsWith('./') || specifier.startsWith('../')) {
    if (!/\.[cm]?[jt]s$/.test(specifier)) {
      return nextResolve(`${specifier}.ts`, context);
    }
  }
  return nextResolve(specifier, context);
}
