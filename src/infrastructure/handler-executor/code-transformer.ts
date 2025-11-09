const MAPPINGS: Record<string, string> = {
  '@CACHE': '$ctx.$cache',
  '@REPOS': '$ctx.$repos',
  '@HELPERS': '$ctx.$helpers',
  '@LOGS': '$ctx.$logs',
  '@BODY': '$ctx.$body',
  '@DATA': '$ctx.$data',
  '@STATUS': '$ctx.$statusCode',
  '@PARAMS': '$ctx.$params',
  '@QUERY': '$ctx.$query',
  '@USER': '$ctx.$user',
  '@REQ': '$ctx.$req',
  '@RES': '$ctx.$res',
  '@SHARE': '$ctx.$share',
  '@API': '$ctx.$api',
  '@UPLOADED_FILE': '$ctx.$uploadedFile',
  '@PKGS': '$ctx.$pkgs',
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
};

const CODE = 0;
const STRING_DOUBLE = 1;
const STRING_SINGLE = 2;
const TEMPLATE = 3;
const COMMENT_LINE = 4;
const COMMENT_BLOCK = 5;

export function transformCode(code: string): string {
  const len = code.length;
  let result = '';
  let pos = 0;
  let state = CODE;

  while (pos < len) {
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
        } else if (char === '@' || char === '#' || char === '%') {
          const start = pos;
          const prefix = char;
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

          if (prefix === '@') {
            const mapped = MAPPINGS[identifier];
            result += mapped || identifier;
          } else if (prefix === '#') {
            result += '$ctx.$repos.' + identifier.substring(1);
          } else {
            result += '$ctx.$pkgs.' + identifier.substring(1);
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
        } else {
          pos++;
        }
        break;

      case COMMENT_LINE:
        result += char;
        if (char === '\n') {
          state = CODE;
        }
        pos++;
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
