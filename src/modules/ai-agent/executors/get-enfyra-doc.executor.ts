import {
  getEnfyraDocSection,
  getMultipleEnfyraDocSections,
  listEnfyraDocSectionIds,
} from '../utils/enfyra-doc-sections.helper';

export async function executeGetEnfyraDoc(args: {
  section?: string;
  sections?: string[];
}): Promise<{
  ok: boolean;
  availableSections?: string[];
  content?: Record<string, string>;
  message?: string;
}> {
  const sectionsArg = args?.sections;
  const single = args?.section;

  if (Array.isArray(sectionsArg) && sectionsArg.length > 0) {
    const { sections, unknown } = getMultipleEnfyraDocSections(sectionsArg);
    return {
      ok: true,
      content: sections,
      message:
        unknown.length > 0
          ? `Unknown section id(s): ${unknown.join(', ')}. Valid: ${listEnfyraDocSectionIds().join(', ')}`
          : undefined,
    };
  }

  if (single && typeof single === 'string' && single.trim()) {
    const r = getEnfyraDocSection(single.trim());
    if (!r.found) {
      return {
        ok: false,
        availableSections: listEnfyraDocSectionIds(),
        message: `Unknown section "${single}". Use one of availableSections or call with no args to list ids.`,
      };
    }
    return {
      ok: true,
      content: { [r.section!]: r.content! },
    };
  }

  return {
    ok: true,
    availableSections: listEnfyraDocSectionIds(),
    message: 'Pass section (string) or sections (string[]) to load Enfyra rules aligned with MCP. No token-heavy full doc in system prompt.',
  };
}
