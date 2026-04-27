import { BadRequestException } from '../../../domain/exceptions';

export function isProbablyVueSFC(content: string): boolean {
  if (typeof content !== 'string') return false;
  const trimmed = content.trim();
  if (!trimmed) return false;

  const hasSfcTags = /<template[\s>]|<script[\s>]|<style[\s>]/i.test(trimmed);
  const hasClosing = /<\/template>|<\/script>|<\/style>/i.test(trimmed);

  return hasSfcTags && hasClosing;
}

export function assertValidVueSFC(content: string): void {
  const templateOpen = (content.match(/<template[^>]*>/g) || []).length;
  const templateClose = (content.match(/<\/template>/g) || []).length;
  const scriptOpen = (content.match(/<script[^>]*>/g) || []).length;
  const scriptClose = (content.match(/<\/script>/g) || []).length;
  const styleOpen = (content.match(/<style[^>]*>/g) || []).length;
  const styleClose = (content.match(/<\/style>/g) || []).length;

  if (
    templateOpen !== templateClose ||
    scriptOpen !== scriptClose ||
    styleOpen !== styleClose
  ) {
    throw new BadRequestException('Invalid Vue SFC: unbalanced tags');
  }

  if (templateOpen === 0 && scriptOpen === 0) {
    throw new BadRequestException(
      'Invalid Vue SFC: must have at least <template> or <script>',
    );
  }

  if (scriptOpen > 0) {
    const scriptContent = content.match(/<script[^>]*>([\s\S]*?)<\/script>/i);
    if (scriptContent && scriptContent[1]) {
      const script = scriptContent[1];
      if (script.includes('export default') && !script.includes('{')) {
        throw new BadRequestException(
          'Invalid Vue SFC: script must have proper export default syntax',
        );
      }
    }
  }
}

export function assertValidJsBundleSyntax(code: string): void {
  const brackets = { '(': 0, ')': 0, '{': 0, '}': 0, '[': 0, ']': 0 };

  for (const char of code) {
    if (char in brackets) {
      brackets[char as keyof typeof brackets]++;
    }
  }

  if (
    brackets['('] !== brackets[')'] ||
    brackets['{'] !== brackets['}'] ||
    brackets['['] !== brackets[']']
  ) {
    throw new BadRequestException('Invalid JS syntax: unbalanced brackets');
  }

  if (
    !code.includes('export') &&
    !code.includes('module.exports') &&
    !code.includes('window.')
  ) {
    throw new BadRequestException(
      'Invalid JS bundle: must have export statement or module.exports',
    );
  }

  if (code.includes('function(') && !code.includes(')')) {
    throw new BadRequestException(
      'Invalid JS syntax: incomplete function declaration',
    );
  }
}
