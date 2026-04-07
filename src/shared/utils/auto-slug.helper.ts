export function autoSlug(
  input: string,
  options: {
    separator?: string;
    lowercase?: boolean;
    trim?: boolean;
    maxLength?: number;
  } = {}
): string {
  const {
    separator = '-',
    lowercase = true,
    trim = true,
    maxLength = 100,
  } = options;
  if (!input || typeof input !== 'string') {
    return '';
  }
  let slug = input;
  slug = slug.replace(/đ/g, 'd').replace(/Đ/g, 'D');
  slug = slug.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  if (lowercase) {
    slug = slug.toLowerCase();
  }
  const escapedSep = separator.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  slug = slug
    .replace(/[^a-zA-Z0-9\s-]/g, '')
    .replace(/\s+/g, separator)
    .replace(new RegExp(`${escapedSep}+`, 'g'), separator);
  if (trim) {
    slug = slug.replace(new RegExp(`^${escapedSep}+|${escapedSep}+$`, 'g'), '');
  }
  if (maxLength && slug.length > maxLength) {
    slug = slug.substring(0, maxLength);
    slug = slug.replace(new RegExp(`${escapedSep}+$`, 'g'), '');
  }
  return slug;
}
export function generateUniqueSlug(
  input: string,
  existingSlugs: string[] = []
): string {
  const baseSlug = autoSlug(input);
  if (!existingSlugs.includes(baseSlug)) {
    return baseSlug;
  }
  let counter = 1;
  let uniqueSlug = `${baseSlug}-${counter}`;
  while (existingSlugs.includes(uniqueSlug)) {
    counter++;
    uniqueSlug = `${baseSlug}-${counter}`;
  }
  return uniqueSlug;
}