export function resolvePath(obj, path) {
  const parts = path.split('.');
  const method = parts.pop();
  let parent = obj;
  for (const part of parts) {
    if (parent && part in parent) {
      parent = parent[part];
    } else {
      throw new Error(`Invalid path: ${path}`);
    }
  }
  return { parent, method };
}
