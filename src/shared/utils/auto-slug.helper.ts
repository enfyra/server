/**
 * AutoSlug Helper - Generate SEO-friendly slugs from any string
 * Supports multiple languages including Vietnamese, Chinese, Arabic, etc.
 */

/**
 * Generate a URL-friendly slug from any string
 * @param input - The input string to convert to slug
 * @param options - Configuration options
 * @returns A clean, URL-friendly slug
 */
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

  // Handle special Vietnamese Đ character first (must be before NFD normalization)
  slug = slug.replace(/đ/g, 'd').replace(/Đ/g, 'D');

  // Normalize and remove all diacritics (including Vietnamese)
  slug = slug.normalize('NFD').replace(/[\u0300-\u036f]/g, '');

  if (lowercase) {
    slug = slug.toLowerCase();
  }

  // Replace spaces and special characters with separator
  slug = slug
    .replace(/[^a-zA-Z0-9\s-]/g, '') // Remove special characters except spaces and hyphens
    .replace(/\s+/g, separator) // Replace spaces with separator
    .replace(new RegExp(`${separator}+`, 'g'), separator); // Replace multiple separators with single

  if (trim) {
    slug = slug.replace(new RegExp(`^${separator}+|${separator}+$`, 'g'), '');
  }

  // Limit length
  if (maxLength && slug.length > maxLength) {
    slug = slug.substring(0, maxLength);
    // Remove trailing separator if cut off in middle
    slug = slug.replace(new RegExp(`${separator}+$`, 'g'), '');
  }

  return slug;
}

/**
 * Generate unique slug by checking against existing slugs
 * @param input - The input string to convert
 * @param existingSlugs - Array of existing slugs to avoid conflicts
 * @returns A unique slug
 */
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
