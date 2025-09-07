import * as fs from 'fs';
import * as path from 'path';

export function clearOldEntitiesJs() {
  const directory = path.resolve('dist', 'src', 'core', 'database', 'entities');

  if (!fs.existsSync(directory)) return;

  const files = fs.readdirSync(directory);

  for (const file of files) {
    const fullPath = path.join(directory, file);

    try {
      const stat = fs.statSync(fullPath);

      // Ensure only regular files with .js extension are deleted
      if (stat.isFile() && file.endsWith('.js')) {
        fs.unlinkSync(fullPath);
      }
    } catch (err) {
      console.error(`❌ Error processing: ${fullPath}`, err);
    }
  }
}
