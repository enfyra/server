import { randomUUID } from 'crypto';
import {
  readFileSync,
  writeFileSync,
  mkdirSync,
  existsSync,
  rmSync,
  readdirSync,
} from 'fs';
import { join, resolve, dirname } from 'path';
import { BadRequestException } from '@nestjs/common';
import { build } from 'vite';
import vue from '@vitejs/plugin-vue';
// @ts-ignore
import { compile } from 'tailwindcss';

function extractCandidates(source: string): string[] {
  const candidates = new Set<string>();
  const regex = /["'`]([^"'`]*)["'`]/g;
  let match: RegExpExecArray | null;
  while ((match = regex.exec(source)) !== null) {
    for (const token of match[1].split(/\s+/)) {
      if (token && /^[a-zA-Z!@\-_]/.test(token) && token.length < 100) {
        candidates.add(token);
      }
    }
  }
  return Array.from(candidates);
}

async function generateTailwindCss(
  vueContent: string,
  projectRoot: string,
): Promise<string> {
  const candidates = extractCandidates(vueContent);
  if (candidates.length === 0) return '';

  const twBase = dirname(require.resolve('tailwindcss/package.json'));

  const themeConfigPath = resolve(projectRoot, '..', 'app', 'tailwind.theme.js');
  const configDirective = existsSync(themeConfigPath)
    ? `@config "${themeConfigPath}";\n`
    : '';

  const cssInput = [
    configDirective,
    '@import "tailwindcss/theme.css";',
    '@import "tailwindcss/utilities.css";',
    '@variant dark (&:where(.dark, .dark *));',
  ].join('\n');

  const loadStylesheet = async (id: string, base: string) => {
    let resolved: string;
    try {
      resolved = require.resolve(id, { paths: [base] });
    } catch {
      resolved = resolve(base, id);
    }
    return { content: readFileSync(resolved, 'utf-8'), base: dirname(resolved) };
  };

  const loadModule = async (id: string, base: string) => {
    const resolved = require.resolve(id, { paths: [base] });
    const mod = await import(resolved);
    return { module: mod.default || mod, base: dirname(resolved) };
  };

  const compiler = await compile(cssInput, { base: twBase, loadStylesheet, loadModule });
  const generated = compiler.build(candidates);

  return generated
    .replace(/:root,?\s*:host\s*\{[^}]*\}/g, '')
    .trim();
}

async function buildExtensionWithVite(
  vueContent: string,
  extensionId: string,
): Promise<string> {
  const buildId = `${extensionId}-${Date.now()}-${randomUUID()}`;
  const projectRoot = process.cwd();
  const tempDir = join(projectRoot, '.temp-extension-builds', buildId);
  const tempExtensionFile = join(tempDir, 'extension.vue');
  const tempEntryFile = join(tempDir, 'entry.js');

  try {
    if (!existsSync(tempDir)) {
      mkdirSync(tempDir, { recursive: true });
    }

    writeFileSync(tempExtensionFile, vueContent);
    writeFileSync(
      tempEntryFile,
      `
import ExtensionComponent from './extension.vue'
export default ExtensionComponent
`,
    );

    const parentDir = resolve(projectRoot, '..');

    const possibleVuePaths = [
      resolve(parentDir, 'node_modules/vue'),
      resolve(projectRoot, 'node_modules/vue'),
      resolve(projectRoot, 'dist/node_modules/vue'),
    ];

    const possibleCompilerPaths = [
      resolve(parentDir, 'node_modules/@vue/compiler-sfc'),
      resolve(projectRoot, 'node_modules/@vue/compiler-sfc'),
      resolve(projectRoot, 'dist/node_modules/@vue/compiler-sfc'),
    ];

    let vuePath: string | undefined;
    let vueCompilerPath: string | undefined;

    try {
      vuePath = require.resolve('vue');
      try {
        vueCompilerPath = require.resolve('vue/compiler-sfc');
      } catch {
        vueCompilerPath = require.resolve('@vue/compiler-sfc');
      }
    } catch (e) {
      vuePath = possibleVuePaths.find((p) => existsSync(p));
      vueCompilerPath = possibleCompilerPaths.find((p) => existsSync(p));
    }

    if (!vuePath || !existsSync(vuePath)) {
      throw new Error(
        `Vue not found. Make sure vue is installed. Searched: ${possibleVuePaths.join(', ')}`,
      );
    }
    if (!vueCompilerPath || !existsSync(vueCompilerPath)) {
      throw new Error(
        `Vue compiler-sfc not found. Make sure vue is installed. Searched: ${possibleCompilerPaths.join(', ')}`,
      );
    }

    await build({
      root: projectRoot,
      resolve: {
        alias: {
          vue: vuePath,
          'vue/compiler-sfc': vueCompilerPath,
          '@vue/compiler-sfc': vueCompilerPath,
        },
        dedupe: ['vue', '@vue/compiler-sfc'],
      },
      server: {
        fs: {
          allow: [
            projectRoot,
            parentDir,
            resolve(projectRoot, 'node_modules'),
            resolve(parentDir, 'node_modules'),
            resolve(projectRoot, 'dist'),
          ],
        },
      },
      build: {
        lib: {
          entry: tempEntryFile,
          name: extensionId,
          fileName: () => 'extension.js',
          formats: ['umd'],
        },
        outDir: join(tempDir, 'dist'),
        emptyOutDir: true,
        write: true,
        rollupOptions: {
          external: ['vue'],
          output: {
            globals: {
              vue: 'Vue',
            },
          },
        },
      },
      plugins: [vue()],
    });

    const compiledFile = join(tempDir, 'dist', 'extension.js');
    let compiledCode = readFileSync(compiledFile, 'utf-8');

    const cssParts: string[] = [];

    const distDir = join(tempDir, 'dist');
    const cssFiles = readdirSync(distDir).filter((f) => f.endsWith('.css'));
    for (const f of cssFiles) {
      const content = readFileSync(join(distDir, f), 'utf-8').trim();
      if (content) cssParts.push(content);
    }

    try {
      const twCss = await generateTailwindCss(vueContent, projectRoot);
      if (twCss) cssParts.push(twCss);
    } catch {
      // Tailwind CSS generation failed, continue without it
    }

    if (cssParts.length > 0) {
      const allCss = cssParts.join('\n');
      const cssInjection = `(function(){var id="ext-style-${extensionId}";var el=document.getElementById(id);if(el){el.textContent=${JSON.stringify(allCss)}}else{var s=document.createElement("style");s.id=id;s.textContent=${JSON.stringify(allCss)};document.head.appendChild(s)}})();\n`;
      compiledCode = cssInjection + compiledCode;
    }

    return compiledCode;
  } catch (error: any) {
    throw new BadRequestException(
      `Failed to build extension: ${error.message || 'Unknown error'}`,
    );
  } finally {
    try {
      if (existsSync(tempDir)) {
        rmSync(tempDir, { recursive: true, force: true });
      }
    } catch (cleanupError) {
      // ignore
    }
  }
}

export { buildExtensionWithVite };
