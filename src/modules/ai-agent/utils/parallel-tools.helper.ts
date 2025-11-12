import { Logger } from '@nestjs/common';

const logger = new Logger('ParallelTools');

export interface ToolCall {
  id: string;
  function: {
    name: string;
    arguments: string;
  };
}

export interface ToolDependency {
  toolIndex: number;
  dependsOn: number[];
}

/**
 * Analyzes tool calls to determine if they can be executed in parallel
 * Returns true if ALL tools are independent (no data dependencies)
 */
export function areToolsIndependent(toolCalls: ToolCall[]): boolean {
  if (toolCalls.length <= 1) {
    return false; // No benefit for single tool
  }

  try {
    // Parse all tool arguments
    const parsedTools = toolCalls.map((tc, index) => {
      try {
        return {
          index,
          name: tc.function.name,
          args: JSON.parse(tc.function.arguments),
        };
      } catch (e) {
        logger.warn(`Failed to parse tool arguments for ${tc.function.name}: ${e.message}`);
        return null;
      }
    }).filter(Boolean);

    if (parsedTools.length !== toolCalls.length) {
      // Some tools have invalid args, play it safe
      return false;
    }

    // Check for dependencies between tools
    for (let i = 0; i < parsedTools.length; i++) {
      for (let j = i + 1; j < parsedTools.length; j++) {
        if (hasDataDependency(parsedTools[i], parsedTools[j])) {
          logger.debug(
            `Tools "${parsedTools[i].name}" and "${parsedTools[j].name}" have data dependency, cannot parallelize`,
          );
          return false;
        }
      }
    }

    logger.debug(`All ${toolCalls.length} tools are independent, can execute in parallel`);
    return true;
  } catch (error: any) {
    logger.error('Error analyzing tool dependencies:', error);
    return false; // Fail-safe: don't parallelize if analysis fails
  }
}

/**
 * Checks if two tools have data dependency
 * Returns true if tool2 depends on results from tool1
 */
function hasDataDependency(tool1: any, tool2: any): boolean {
  // Same table operations - potential conflict
  if (
    tool1.args.table &&
    tool2.args.table &&
    tool1.args.table === tool2.args.table
  ) {
    // Write operations on same table - sequential required
    if (
      ['create', 'update', 'delete', 'batch_create', 'batch_update', 'batch_delete'].includes(
        tool1.args.operation,
      ) ||
      ['create', 'update', 'delete', 'batch_create', 'batch_update', 'batch_delete'].includes(
        tool2.args.operation,
      )
    ) {
      return true;
    }
  }

  // Permission check + operation - sequential required
  if (
    (tool1.name === 'get_hint' && tool1.args.category === 'permission_check') ||
    (tool2.name === 'get_hint' && tool2.args.category === 'permission_check')
  ) {
    // Permission check should run before data operations
    return true;
  }

  // get_metadata + dynamic_repository on table_definition - potential dependency
  if (
    (tool1.name === 'get_metadata' && tool2.name === 'dynamic_repository' && tool2.args.table === 'table_definition') ||
    (tool2.name === 'get_metadata' && tool1.name === 'dynamic_repository' && tool1.args.table === 'table_definition')
  ) {
    // Metadata changes require refresh
    return true;
  }

  // get_table_details on same table as operation
  if (
    (tool1.name === 'get_table_details' && tool2.name === 'dynamic_repository' && tool1.args.tableName === tool2.args.table) ||
    (tool2.name === 'get_table_details' && tool1.name === 'dynamic_repository' && tool2.args.tableName === tool1.args.table)
  ) {
    // Getting schema before operation - likely dependency
    if (['create', 'update', 'batch_create', 'batch_update'].includes(tool2.args.operation || tool1.args.operation)) {
      return true;
    }
  }

  // No obvious dependency found
  return false;
}

/**
 * Groups tool calls into parallel batches
 * Tools in the same batch can run in parallel
 * Batches must run sequentially
 */
export function groupToolsIntoBatches(toolCalls: ToolCall[]): ToolCall[][] {
  if (toolCalls.length <= 1) {
    return [toolCalls];
  }

  // Simple strategy: If all independent, one batch. Otherwise, sequential.
  if (areToolsIndependent(toolCalls)) {
    return [toolCalls]; // All in one parallel batch
  }

  // Has dependencies - run sequentially (each tool in its own batch)
  return toolCalls.map((tc) => [tc]);
}

/**
 * Executes tool calls with optimal parallelization
 * Returns results in the same order as input toolCalls
 */
export async function executeToolsOptimized<T>(
  toolCalls: ToolCall[],
  executor: (toolCall: ToolCall) => Promise<T>,
): Promise<T[]> {
  if (toolCalls.length === 0) {
    return [];
  }

  if (toolCalls.length === 1) {
    // Single tool - just execute
    return [await executor(toolCalls[0])];
  }

  // Check if tools can run in parallel
  const canParallelize = areToolsIndependent(toolCalls);

  if (canParallelize) {
    logger.log(`Executing ${toolCalls.length} tools in PARALLEL`);
    const startTime = Date.now();

    // Execute all tools in parallel
    const results = await Promise.all(toolCalls.map((tc) => executor(tc)));

    const duration = Date.now() - startTime;
    logger.log(`Parallel execution completed in ${duration}ms`);

    return results;
  } else {
    logger.log(`Executing ${toolCalls.length} tools SEQUENTIALLY (dependencies detected)`);
    const startTime = Date.now();

    // Execute tools sequentially
    const results: T[] = [];
    for (const toolCall of toolCalls) {
      const result = await executor(toolCall);
      results.push(result);
    }

    const duration = Date.now() - startTime;
    logger.log(`Sequential execution completed in ${duration}ms`);

    return results;
  }
}
