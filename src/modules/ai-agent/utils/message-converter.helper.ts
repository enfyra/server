import { LLMMessage } from '../services/llm.service';

export function convertMessagesToAnthropic(messages: LLMMessage[]): any[] {
  return messages
    .map((msg) => {
      if (msg.role === 'system') {
        return { role: 'system', content: msg.content || '' };
      }
      if (msg.role === 'user') {
        // Anthropic API accepts both string and array format for user messages
        // Using string format for simplicity when there's just text content
        const content = msg.content || '';
        if (!content) {
          // Skip empty user messages
          return null;
        }
        return { role: 'user', content };
      }
      if (msg.role === 'assistant') {
        if (msg.tool_calls && msg.tool_calls.length > 0) {
          const content: any[] = [];
          if (msg.content) {
            content.push({ type: 'text', text: msg.content });
          }
          content.push(...msg.tool_calls.map((tc) => ({
            type: 'tool_use',
            id: tc.id,
            name: tc.function.name,
            input: JSON.parse(tc.function.arguments),
          })));
          return {
            role: 'assistant',
            content: content.length > 0 ? content : [{ type: 'text', text: '' }],
          };
        }
        if (msg.content) {
          return { role: 'assistant', content: [{ type: 'text', text: msg.content }] };
        }
        return null;
      }
      if (msg.role === 'tool') {
        return {
          role: 'user',
          content: [
            {
              type: 'tool_result',
              tool_use_id: msg.tool_call_id,
              content: msg.content || '',
            },
          ],
        };
      }
      return { role: 'user', content: msg.content || '' };
    })
    .filter((msg) => msg !== null);
}

