import { describe, expect, it } from 'vitest';
import { toUIMessageChunk } from './to-ui-message-chunk';

describe('toUIMessageChunk', () => {
  it('keeps workflow flat tool approval request shape', () => {
    expect(
      toUIMessageChunk({
        type: 'tool-approval-request',
        approvalId: 'approval-call-1',
        toolCallId: 'call-1',
      } as any),
    ).toEqual({
      type: 'tool-approval-request',
      approvalId: 'approval-call-1',
      toolCallId: 'call-1',
    });
  });

  it('uses shared conversion with workflow source defaults', () => {
    expect(
      toUIMessageChunk({
        type: 'source',
        sourceType: 'url',
        id: 'source-1',
        url: 'https://example.com',
        title: 'Example',
      }),
    ).toEqual({
      type: 'source-url',
      sourceId: 'source-1',
      url: 'https://example.com',
      title: 'Example',
    });
  });
});
