import { getErrorMessage, type ToolSet } from '@ai-sdk/provider-utils';
import { getResponseUIMessageId } from '../ui-message-stream/get-response-ui-message-id';
import { handleUIMessageStreamFinish } from '../ui-message-stream/handle-ui-message-stream-finish';
import type {
  InferUIMessageChunk,
  UIMessageChunk,
} from '../ui-message-stream/ui-message-chunks';
import type { UIMessage } from '../ui/ui-messages';
import type {
  TextStreamPart,
  UIMessageStreamOptions,
} from './stream-text-result';
import { toUIMessageChunk } from './to-ui-message-chunk';

/**
 * Converts a stream of `TextStreamPart<TOOLS>` chunks (as emitted by
 * `streamText`'s `fullStream`) into a stream of `UIMessageChunk`s suitable for
 * UI message streaming, including message ID injection and `onFinish` handling.
 */
export function toUIMessageChunkStream<
  TOOLS extends ToolSet,
  UI_MESSAGE extends UIMessage,
>({
  stream,
  tools,
  sendReasoning = true,
  sendSources = false,
  sendStart = true,
  sendFinish = true,
  onError = getErrorMessage,
  messageMetadata,
  originalMessages,
  generateMessageId,
  onFinish,
}: {
  stream: ReadableStream<
    TextStreamPart<TOOLS> | InferUIMessageChunk<UI_MESSAGE>
  >;
  tools?: TOOLS;
} & UIMessageStreamOptions<UI_MESSAGE>): ReadableStream<
  InferUIMessageChunk<UI_MESSAGE>
> {
  const responseMessageId =
    generateMessageId != null
      ? getResponseUIMessageId({
          originalMessages,
          responseMessageId: generateMessageId,
        })
      : undefined;

  const uiMessageChunkStream = stream.pipeThrough(
    new TransformStream<
      TextStreamPart<TOOLS> | InferUIMessageChunk<UI_MESSAGE>,
      InferUIMessageChunk<UI_MESSAGE>
    >({
      transform: async (part, controller) => {
        const isUIMessageChunk = isUIMessageStreamChunk(part);
        const messageMetadataValue = isUIMessageChunk
          ? undefined
          : messageMetadata?.({ part });

        if (isUIMessageChunk) {
          controller.enqueue(part as InferUIMessageChunk<UI_MESSAGE>);
        } else {
          const uiMessageChunk = toUIMessageChunk<TOOLS, UI_MESSAGE>(part, {
            tools,
            sendReasoning,
            sendSources,
            sendStart,
            sendFinish,
            onError,
            messageMetadata: messageMetadataValue,
            responseMessageId,
          });

          if (uiMessageChunk != null) {
            controller.enqueue(uiMessageChunk);
          }
        }

        // start and finish events already have metadata
        // so we only need to send metadata for other parts
        if (
          messageMetadataValue != null &&
          part.type !== 'start' &&
          part.type !== 'finish'
        ) {
          controller.enqueue({
            type: 'message-metadata',
            messageMetadata: messageMetadataValue,
          });
        }
      },
    }),
  );

  return handleUIMessageStreamFinish<UI_MESSAGE>({
    stream: uiMessageChunkStream,
    messageId: responseMessageId ?? generateMessageId?.(),
    originalMessages,
    onFinish,
    onError,
  });
}

function isUIMessageStreamChunk(chunk: {
  type: string;
}): chunk is UIMessageChunk {
  if (chunk.type.startsWith('data-')) {
    return true;
  }

  switch (chunk.type) {
    case 'text-delta':
    case 'reasoning-delta':
      return 'delta' in chunk;

    case 'file':
    case 'reasoning-file':
      return 'url' in chunk;

    case 'source-url':
    case 'source-document':
    case 'custom':
    case 'message-metadata':
    case 'tool-input-available':
    case 'tool-input-error':
    case 'tool-output-available':
    case 'tool-output-error':
      return true;

    case 'tool-input-start':
      return 'toolCallId' in chunk;

    case 'tool-input-delta':
      return 'inputTextDelta' in chunk;

    case 'tool-approval-request':
    case 'tool-approval-response':
      return !('toolCall' in chunk);

    case 'error':
      return 'errorText' in chunk;

    case 'start-step':
      return !('request' in chunk);

    case 'finish-step':
      return !('response' in chunk);

    case 'start':
      return 'messageId' in chunk || 'messageMetadata' in chunk;

    case 'finish':
      return !('totalUsage' in chunk) && !('rawFinishReason' in chunk);

    default:
      return false;
  }
}
