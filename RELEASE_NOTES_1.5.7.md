# Release Notes - Enfyra Server v1.5.7

## AI Agent Improvements

### Menu Operations – Schema Fix
- **Fixed:** `menu_definition` uses `label` column, not `name`
- Added CRITICAL hint to prevent "column menu_definition.name does not exist" errors when AI queries menus
- AI agent now correctly uses `filter: { label: { _eq: "Dashboard" } }` instead of `name`

### UI Vibe – Design System Hint
- **New:** `ui_vibe` hint category for consistent extension UI
- Provides Enfyra design system guidelines: colors (primary, success, error, etc.), layout patterns, typography, spacing
- Helps AI create extensions that match the app's visual style
- Call `get_hint` with category `ui_vibe` when creating custom pages or widgets

### Extension Operations – Persistence Fix
- **Fixed:** AI agent was confirming updates without actually saving extension code
- Added CRITICAL rule: must call `update_records` to persist extension code changes
- Outputting Vue code in the response does **not** save changes – the tool must be called
- Added explicit workflow for updating existing extension code via `find_records` → `update_records`
- Prevents "I've updated" responses when no tool was used

### Tool Selection Updates
- Extended `evaluate-needs-tools` to include `menu_operations`, `extension_operations`, and `ui_vibe` for relevant user intents
- Menu/extension creation tasks now automatically receive the correct hint categories

---

## Technical Details

- **Schema:** `extension_definition.code` and `compiledCode` use type `code` → mapped to `TEXT` (PostgreSQL/SQLite) or `LONGTEXT` (MySQL) – sufficient for source and compiled Vue SFC
- **Hints:** All changes in `get-hint.executor.ts`, `llm-tools-definitions.helper.ts`, `evaluate-needs-tools.base.ts`, and `prompt-builder.ts`
