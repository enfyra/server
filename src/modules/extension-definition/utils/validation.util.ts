import { BadRequestException } from '../../../domain/exceptions';

const SFC_TAGS = ['template', 'script', 'style'] as const;
type SfcTag = (typeof SFC_TAGS)[number];

function isTagBoundary(char: string | undefined): boolean {
  return !char || /\s|>|\//.test(char);
}

function scanSfcTags(content: string): {
  counts: Record<SfcTag, { open: number; close: number }>;
  firstScriptContent: string | null;
} {
  const counts: Record<SfcTag, { open: number; close: number }> = {
    template: { open: 0, close: 0 },
    script: { open: 0, close: 0 },
    style: { open: 0, close: 0 },
  };
  let firstScriptContent: string | null = null;
  const lower = content.toLowerCase();
  let index = 0;

  while (index < lower.length) {
    const tagStart = lower.indexOf('<', index);
    if (tagStart === -1) break;

    const isClosing = lower[tagStart + 1] === '/';
    const nameStart = tagStart + (isClosing ? 2 : 1);
    const tagName = SFC_TAGS.find(
      (name) =>
        lower.startsWith(name, nameStart) &&
        isTagBoundary(lower[nameStart + name.length]),
    );

    if (!tagName) {
      index = tagStart + 1;
      continue;
    }

    if (isClosing) {
      counts[tagName].close += 1;
      index = tagStart + 1;
      continue;
    }

    counts[tagName].open += 1;
    const openEnd = lower.indexOf('>', nameStart + tagName.length);
    if (tagName === 'script' && firstScriptContent === null && openEnd !== -1) {
      const closeStart = lower.indexOf('</script', openEnd + 1);
      if (closeStart !== -1) {
        firstScriptContent = content.slice(openEnd + 1, closeStart);
      }
    }
    index = tagStart + 1;
  }

  return { counts, firstScriptContent };
}

export function isProbablyVueSFC(content: string): boolean {
  if (typeof content !== 'string') return false;
  const trimmed = content.trim();
  if (!trimmed) return false;

  const { counts } = scanSfcTags(trimmed);
  const hasSfcTags = SFC_TAGS.some((tag) => counts[tag].open > 0);
  const hasClosing = SFC_TAGS.some((tag) => counts[tag].close > 0);

  return hasSfcTags && hasClosing;
}

export function assertValidVueSFC(content: string): void {
  const { counts, firstScriptContent } = scanSfcTags(content);

  if (
    counts.template.open !== counts.template.close ||
    counts.script.open !== counts.script.close ||
    counts.style.open !== counts.style.close
  ) {
    throw new BadRequestException('Invalid Vue SFC: unbalanced tags');
  }

  if (counts.template.open === 0 && counts.script.open === 0) {
    throw new BadRequestException(
      'Invalid Vue SFC: must have at least <template> or <script>',
    );
  }

  if (counts.script.open > 0 && firstScriptContent) {
    if (
      firstScriptContent.includes('export default') &&
      !firstScriptContent.includes('{')
    ) {
      throw new BadRequestException(
        'Invalid Vue SFC: script must have proper export default syntax',
      );
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
