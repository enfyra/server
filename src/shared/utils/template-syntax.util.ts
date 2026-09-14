import ts from 'typescript';

const TEMPLATE_MAPPINGS: Record<string, string> = {
  '@CACHE': '$ctx.$cache',
  '@REPOS': '$ctx.$repos',
  '@HELPERS': '$ctx.$helpers',
  '@STORAGE': '$ctx.$storage',
  '@FETCH': '$ctx.$helpers.$fetch',
  '@LOGS': '$ctx.$logs',
  '@BODY': '$ctx.$body',
  '@ENV': '$ctx.$env',
  '@DATA': '$ctx.$data',
  '@PARAMS': '$ctx.$params',
  '@QUERY': '$ctx.$query',
  '@USER': '$ctx.$user',
  '@REQ': '$ctx.$req',
  '@RES': '$ctx.$res',
  '@SHARE': '$ctx.$share',
  '@API': '$ctx.$api',
  '@UPLOADED_FILE': '$ctx.$uploadedFile',
  '@PKGS': '$ctx.$pkgs',
  '@SOCKET': '$ctx.$socket',
  '@TRIGGER': '$ctx.$trigger',
  '@TRANSACTION': '$ctx.$transaction',
  '@FLOW': '$ctx.$flow',
  '@FLOW_PAYLOAD': '$ctx.$flow.$payload',
  '@FLOW_LAST': '$ctx.$flow.$last',
  '@FLOW_META': '$ctx.$flow.$meta',
  '@THROW400': "$ctx.$throw['400']",
  '@THROW401': "$ctx.$throw['401']",
  '@THROW403': "$ctx.$throw['403']",
  '@THROW404': "$ctx.$throw['404']",
  '@THROW409': "$ctx.$throw['409']",
  '@THROW422': "$ctx.$throw['422']",
  '@THROW429': "$ctx.$throw['429']",
  '@THROW500': "$ctx.$throw['500']",
  '@THROW503': "$ctx.$throw['503']",
  '@THROW': '$ctx.$throw',
  '@ERROR': '$ctx.$error',
  '@STATUS': '$ctx.$statusCode',
};

const CODE = 0;
const STRING_DOUBLE = 1;
const STRING_SINGLE = 2;
const TEMPLATE = 3;
const COMMENT_LINE = 4;
const COMMENT_BLOCK = 5;

interface SourceRange {
  start: number;
  end: number;
}

function findRegularExpressionRanges(code: string): SourceRange[] {
  const sourceFile = ts.createSourceFile(
    'enfyra-script.ts',
    code,
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.TS,
  );
  const ranges: SourceRange[] = [];

  const visit = (node: ts.Node) => {
    if (ts.isRegularExpressionLiteral(node)) {
      ranges.push({ start: node.getStart(sourceFile), end: node.end });
    }
    ts.forEachChild(node, visit);
  };

  visit(sourceFile);
  return ranges.sort((left, right) => left.start - right.start);
}

function isIdentifierStart(char: string | undefined): boolean {
  return !!char && /[A-Za-z_]/.test(char);
}

function isIdentifierChar(char: string | undefined): boolean {
  return !!char && /[A-Za-z0-9_]/.test(char);
}

export function transformTemplateSyntax(code: string): string {
  const len = code.length;
  let result = '';
  let pos = 0;
  let state = CODE;
  let templateExprDepth = 0;
  let braceDepth = 0;
  const regularExpressionRanges = findRegularExpressionRanges(code);
  let regularExpressionIndex = 0;

  while (pos < len) {
    while (
      regularExpressionIndex < regularExpressionRanges.length &&
      regularExpressionRanges[regularExpressionIndex].end <= pos
    ) {
      regularExpressionIndex++;
    }
    const regularExpression = regularExpressionRanges[regularExpressionIndex];
    if (
      state === CODE &&
      regularExpression &&
      regularExpression.start === pos
    ) {
      result += code.slice(regularExpression.start, regularExpression.end);
      pos = regularExpression.end;
      regularExpressionIndex++;
      continue;
    }

    const char = code[pos];
    const next = code[pos + 1];

    switch (state) {
      case CODE:
        if (char === '"') {
          state = STRING_DOUBLE;
          result += char;
          pos++;
        } else if (char === "'") {
          state = STRING_SINGLE;
          result += char;
          pos++;
        } else if (char === '`') {
          state = TEMPLATE;
          result += char;
          pos++;
        } else if (char === '/' && next === '/') {
          state = COMMENT_LINE;
          result += char + next;
          pos += 2;
        } else if (char === '/' && next === '*') {
          state = COMMENT_BLOCK;
          result += char + next;
          pos += 2;
        } else if (char === '@') {
          const start = pos;
          pos++;

          while (pos < len) {
            const c = code[pos];
            if ((c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c === '_') {
              pos++;
            } else {
              break;
            }
          }

          const identifier = code.substring(start, pos);
          const mapped = TEMPLATE_MAPPINGS[identifier];
          result += mapped || identifier;
        } else if (char === '#' || char === '%') {
          const start = pos;
          const registry = char === '#' ? '$ctx.$repos.' : '$ctx.$pkgs.';
          pos++;

          if (!isIdentifierStart(code[pos])) {
            result += char;
          } else {
            pos++;
            while (pos < len && isIdentifierChar(code[pos])) pos++;
            result += registry + code.substring(start + 1, pos);
          }
        } else if (char === '{') {
          if (templateExprDepth > 0) braceDepth++;
          result += char;
          pos++;
        } else if (char === '}' && templateExprDepth > 0) {
          if (braceDepth > 0) {
            braceDepth--;
            result += char;
            pos++;
          } else {
            templateExprDepth--;
            result += char;
            pos++;
            state = TEMPLATE;
          }
        } else {
          result += char;
          pos++;
        }
        break;

      case STRING_DOUBLE:
        result += char;
        if (char === '\\') {
          pos++;
          if (pos < len) {
            result += code[pos];
            pos++;
          }
        } else if (char === '"') {
          state = CODE;
          pos++;
        } else {
          pos++;
        }
        break;

      case STRING_SINGLE:
        result += char;
        if (char === '\\') {
          pos++;
          if (pos < len) {
            result += code[pos];
            pos++;
          }
        } else if (char === "'") {
          state = CODE;
          pos++;
        } else {
          pos++;
        }
        break;

      case TEMPLATE:
        result += char;
        if (char === '\\') {
          pos++;
          if (pos < len) {
            result += code[pos];
            pos++;
          }
        } else if (char === '`') {
          state = CODE;
          pos++;
        } else if (char === '$' && next === '{') {
          templateExprDepth++;
          braceDepth = 0;
          result += next;
          pos += 2;
          state = CODE;
        } else {
          pos++;
        }
        break;

      case COMMENT_LINE:
        result += char;
        pos++;
        if (char === '\n') state = CODE;
        break;

      case COMMENT_BLOCK:
        result += char;
        if (char === '*' && next === '/') {
          result += next;
          pos += 2;
          state = CODE;
        } else {
          pos++;
        }
        break;
    }
  }

  return result;
}
