import {
  toUIMessageChunk as toAIUIMessageChunk,
  type Experimental_LanguageModelStreamPart as ModelCallStreamPart,
  type ToolSet,
  type UIMessageChunk,
} from 'ai';

/**
 * Convert a single ModelCallStreamPart to a UIMessageChunk.
 * Returns undefined for parts that don't map to UI chunks.
 */
export function toUIMessageChunk(
  part: ModelCallStreamPart<ToolSet>,
): UIMessageChunk | undefined {
  const workflowPart = part as ModelCallStreamPart<ToolSet> & {
    toolCallId?: string;
  };

  // Workflow emits a flat approval request shape. The AI SDK stream part keeps
  // the tool call nested, so keep this small adapter here.
  if (
    workflowPart.type === 'tool-approval-request' &&
    workflowPart.toolCallId != null
  ) {
    return {
      type: 'tool-approval-request',
      approvalId: workflowPart.approvalId,
      toolCallId: workflowPart.toolCallId,
    };
  }

  return toAIUIMessageChunk(part, {
    sendSources: true,
    onError: error => (error instanceof Error ? error.message : String(error)),
  });
}

/**
 * Create a TransformStream that converts ModelCallStreamPart to UIMessageChunk.
 * Wraps toUIMessageChunk with start/start-step/finish-step lifecycle chunks.
 */
export function createModelCallToUIChunkTransform(): TransformStream<
  ModelCallStreamPart<ToolSet>,
  UIMessageChunk
> {
  return new TransformStream<ModelCallStreamPart<ToolSet>, UIMessageChunk>({
    start: controller => {
      controller.enqueue({ type: 'start' });
      controller.enqueue({ type: 'start-step' });
    },
    flush: controller => {
      controller.enqueue({ type: 'finish-step' });
      controller.enqueue({ type: 'finish' });
    },
    transform: (part, controller) => {
      const uiChunk = toUIMessageChunk(part);
      if (uiChunk) {
        controller.enqueue(uiChunk);
      }
    },
  });
}
