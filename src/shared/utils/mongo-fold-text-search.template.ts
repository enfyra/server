import { foldForSqlSearch } from './unaccent-fold.util';

export function buildMongoCharFoldMapJson(): string {
  const mapObj: Record<string, string> = {};
  for (let cp = 0x80; cp <= 0x10ffff; cp++) {
    try {
      const ch = String.fromCodePoint(cp);
      const f = foldForSqlSearch(ch);
      if (f !== ch) {
        mapObj[ch] = f;
      }
    } catch {
      /* invalid scalar */
    }
  }
  return JSON.stringify(mapObj);
}

export function buildMongoFoldTextSearchJs(): string {
  const json = buildMongoCharFoldMapJson();
  return `function() {
  var M = ${json};
  function foldStr(s) {
    if (s == null) return '';
    s = String(s);
    var o = '';
    for (var i = 0; i < s.length; ) {
      var cp = s.codePointAt(i);
      var ch = String.fromCodePoint(cp);
      i += cp > 0xffff ? 2 : 1;
      var rep = M[ch];
      if (rep !== undefined) {
        o += rep;
        continue;
      }
      var tl = ch.toLowerCase();
      rep = M[tl];
      if (rep !== undefined) {
        o += rep;
        continue;
      }
      o += tl;
    }
    return o;
  }
  function checkOne(haystack, needle, mode) {
    var h = foldStr(haystack);
    var n = foldStr(needle);
    if (mode === 'contains') return h.indexOf(n) >= 0;
    if (mode === 'starts') return n.length === 0 || h.indexOf(n) === 0;
    if (mode === 'ends') return n.length === 0 || (h.length >= n.length && h.substring(h.length - n.length) === n);
    return false;
  }
  var al = arguments.length;
  if (al === 3) {
    return checkOne(arguments[0], arguments[1], arguments[2]);
  }
  if (al === 0 || al % 3 !== 0) {
    return false;
  }
  for (var j = 0; j < al; j += 3) {
    if (!checkOne(arguments[j], arguments[j + 1], arguments[j + 2])) {
      return false;
    }
  }
  return true;
}`;
}
