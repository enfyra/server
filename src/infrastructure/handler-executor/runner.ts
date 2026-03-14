import {
  buildCallableFunctionProxy,
  buildFunctionProxy,
  buildResponseProxy,
} from './utils/build-fn-proxy';

const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;

function stripStringsAndComments(code: string) {
  const placeholders: Array<{ placeholder: string; original: string }> = [];
  let counter = 0;

  const combinedRegex = /(`(?:[^`\\]|\\.)*`)|("(?:[^"\\]|\\.)*")|('(?:[^'\\]|\\.)*')|(\/\*[\s\S]*?\*\/)|(\/\/.*$)/gm;

  const result = code.replace(combinedRegex, (match, template, doubleQuote, singleQuote, multiComment, singleComment) => {
    let placeholder: string;

    if (template) {
      placeholder = `__TEMPLATE_${counter++}__`;
      placeholders.push({ placeholder, original: template });
    } else if (doubleQuote) {
      placeholder = `__STRING_${counter++}__`;
      placeholders.push({ placeholder, original: doubleQuote });
    } else if (singleQuote) {
      placeholder = `__STRING_${counter++}__`;
      placeholders.push({ placeholder, original: singleQuote });
    } else if (multiComment) {
      placeholder = `__COMMENT_${counter++}__`;
      placeholders.push({ placeholder, original: multiComment });
    } else if (singleComment) {
      placeholder = `__COMMENT_${counter++}__`;
      placeholders.push({ placeholder, original: singleComment });
    } else {
      placeholder = match;
    }

    return placeholder;
  });

  return { result, placeholders };
}

function restoreStringsAndComments(code: string, placeholders: Array<{ placeholder: string; original: string }>) {
  let result = code;
  for (let i = placeholders.length - 1; i >= 0; i--) {
    const { placeholder, original } = placeholders[i];
    result = result.replace(placeholder, original);
  }
  return result;
}

export const pendingCalls = new Map();

process.on('unhandledRejection', (reason: any) => {
  console.warn('[Fire-and-forget] Unhandled proxy call rejection:', reason?.message || reason);
});

process.on('uncaughtException', (error: any) => {
  try {
    let errorMessage = error.message ?? String(error);
    let errorName = error.name ?? 'UncaughtException';
    let statusCode = undefined;

    if (error.errorResponse?.message) {
      errorMessage = error.errorResponse.message;
    } else if (error.response?.message) {
      errorMessage = error.response.message;
    } else if (typeof error.response === 'string') {
      errorMessage = error.response;
    }

    if (error.errorResponse?.name) {
      errorName = error.errorResponse.name;
    } else if (error.response?.name) {
      errorName = error.response.name;
    }

    if (error.errorResponse?.statusCode) {
      statusCode = error.errorResponse.statusCode;
    } else if (error.response?.statusCode) {
      statusCode = error.response.statusCode;
    } else if (error.statusCode) {
      statusCode = error.statusCode;
    }

    process.send?.({
      type: 'error',
      error: {
        message: errorMessage,
        stack: error.stack,
        name: errorName,
        statusCode: statusCode,
      },
    });
  } catch (sendError) {
    console.error('Failed to send error:', sendError);
  }

  setTimeout(() => process.exit(1), 100);
});

process.on('message', async (msg: any) => {
  if (msg.type === 'call_result') {
    const { callId, result, error, ...others } = msg;
    const resolver = pendingCalls.get(callId);
    if (resolver) {
      pendingCalls.delete(callId);
      if (error) {
        resolver.reject({ ...error, ...others });
      } else {
        resolver.resolve(result);
      }
    }
  }
  if (msg.type === 'execute') {
    const originalRepos = msg.ctx.$repos || {};
    const packages = msg.packages;

    const ctx = msg.ctx;
    ctx.$repos = {};

    ctx.$pkgs = {};
    for (const packageName of packages) {
      try {
        ctx.$pkgs[packageName] = require(packageName);
      } catch (error) {
        console.warn(`Failed to require package "${packageName}":`, error.message);
      }
    }

    for (const serviceName of Object.keys(originalRepos)) {
      ctx.$repos[serviceName] = buildFunctionProxy(`$repos.${serviceName}`);
    }
    ctx.$throw = buildFunctionProxy('$throw');
    ctx.$helpers = buildFunctionProxy('$helpers');
    ctx.$logs = buildCallableFunctionProxy('$logs');
    ctx.$cache = buildFunctionProxy('$cache');
    ctx.$socket = buildFunctionProxy('$socket');

    if (ctx.$res) {
      ctx.$res = buildResponseProxy();
    }

    if (ctx.$uploadedFile?.buffer) {
      const bufData = ctx.$uploadedFile.buffer;
      let bufferArray: number[] = null;
      
      if (Buffer.isBuffer(bufData)) {
        bufferArray = Array.from(bufData);
      } else if (bufData && typeof bufData === 'object') {
      if (bufData.type === 'Buffer' && Array.isArray(bufData.data)) {
          bufferArray = bufData.data;
        } else {
          const keys = Object.keys(bufData);
          const numericKeys = keys.filter(k => /^\d+$/.test(k));
          if (numericKeys.length > 0) {
            const sortedKeys = numericKeys.map(k => parseInt(k, 10)).sort((a, b) => a - b);
            bufferArray = new Array(sortedKeys.length);
            for (let i = 0; i < sortedKeys.length; i++) {
              bufferArray[i] = bufData[sortedKeys[i].toString()];
            }
          }
        }
      }
      
      if (bufferArray) {
        ctx.$uploadedFile.buffer = {
          type: 'Buffer',
          data: bufferArray,
          toBuffer: () => Buffer.from(bufferArray),
          valueOf: () => Buffer.from(bufferArray),
        };
      }
    }

    let processedCode = msg.code;

    const wrappedCode = `"use strict";
return (async () => {
process.env = null    
${processedCode}
})();`;

    try {
      const asyncFn = new AsyncFunction('$ctx', wrappedCode);
      const result = await asyncFn(ctx);

      process.send({
        type: 'done',
        data: result,
        ctx,
      });
    } catch (error) {
      let errorLine = null;
      let codeContext = '';

      let codeContextArray: string[] = [];

      try {
        const stackMatch = error.stack?.match(/<anonymous>:(\d+)/);
        if (stackMatch) {
          const transformedLine = parseInt(stackMatch[1]);

          const wrapperLinesBefore = 4;

          errorLine = transformedLine - wrapperLinesBefore;

          if (errorLine > 0) {
            const originalLines = msg.code.split('\n');
            const startLine = Math.max(0, errorLine - 2);
            const endLine = Math.min(originalLines.length, errorLine + 3);

            codeContextArray = originalLines
              .slice(startLine, endLine)
              .map((line: string, idx: number) => {
                const lineNum = startLine + idx + 1;
                const isErrorLine = lineNum === errorLine;
                const marker = isErrorLine ? '>' : ' ';
                return `${marker} ${lineNum}. ${line}`;
              });

            codeContext = originalLines
              .slice(startLine, endLine)
              .map((line: string, idx: number) => {
                const lineNum = startLine + idx + 1;
                const marker = lineNum === errorLine ? '❯' : ' ';
                const padding = String(lineNum).padStart(4);
                return `${marker} ${padding} | ${line}`;
              })
              .join('\n');
          }
        }
      } catch (parseError) {
      }

      console.error('\n╭─────────────────────────────────────────╮');
      console.error('│  ❌ Handler Execution Error             │');
      console.error('╰─────────────────────────────────────────╯');
      console.error('');
      console.error(`💥 ${error.name || 'Error'}: ${error.message}`);

      if (errorLine) {
        console.error('');
        console.error(`📍 Error at line ${errorLine}`);
        console.error('');
        console.error(codeContext);
      } else {
        console.error('');
        console.error('📝 Code snippet:');
        console.error(msg.code.split('\n').slice(0, 10).map((line: string, idx: number) =>
          `    ${String(idx + 1).padStart(4)} | ${line}`
        ).join('\n'));
      }

      console.error('');
      console.error('📚 Stack trace:');
      console.error(error.stack);
      console.error('');

      let errorMessage = error.errorResponse?.message ?? error.message ?? 'Unknown error';
      let errorName = error.errorResponse?.name ?? error.name;
      let statusCode = error.errorResponse?.statusCode ?? error.statusCode;

      process.send({
        type: 'error',
        error: {
          message: errorMessage,
          name: errorName,
          statusCode: statusCode,
          errorLine: errorLine,
          codeContextArray: codeContextArray,
          codeContext: codeContext,
          stack: error.stack,
        },
      });
    }
  }
});

process.on('error', (_err) => {
});
