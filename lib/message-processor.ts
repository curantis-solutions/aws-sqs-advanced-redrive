import { Message } from "@aws-sdk/client-sqs";

export interface ParsedMessage<T = any> extends Omit<Message, "Body"> {
  Body?: T;
}

export class ProcessedMessages {
  public deletes: ParsedMessage[];
  public errors: ParsedMessage[];
  public skips: ParsedMessage[];
  public updates: ParsedMessage[];

  constructor() {
    this.deletes = [];
    this.errors = [];
    this.skips = [];
    this.updates = [];
  }

  combine(processedMessages: ProcessedMessages) {
    this.deletes = [...this.deletes, ...processedMessages.deletes];
    this.errors = [...this.errors, ...processedMessages.errors];
    this.skips = [...this.skips, ...processedMessages.skips];
    this.updates = [...this.updates, ...processedMessages.updates];
  }
}

export type MessageProcessorReducer = (
  processMessages: ProcessedMessages,
  message: ParsedMessage,
) => ProcessedMessages;

export type TruthyFunction = (message: ParsedMessage) => boolean;
export type UpdateMapper = (message: ParsedMessage) => ParsedMessage;

export function createMessageReducer(
  shouldSkip: TruthyFunction,
  shouldDelete: TruthyFunction,
  updateMapper: UpdateMapper,
): MessageProcessorReducer {
  return (processedMessages: ProcessedMessages, message: ParsedMessage) => {
    try {
      if (shouldDelete(message)) {
        processedMessages.deletes.push(message);
        return processedMessages;
      }
      if (shouldSkip(message)) {
        processedMessages.skips.push(message);
        return processedMessages;
      }
      processedMessages.updates.push(updateMapper(message));
    } catch (error) {
      console.error(error);
      processedMessages.errors.push(message);
    }
    return processedMessages;
  };
}

export const directRedriveReducer: MessageProcessorReducer =
  createMessageReducer(
    () => false,
    () => false,
    (message) => message,
  );

export const messageProcesserReducers: Record<string, MessageProcessorReducer> =
  {
    directRedriveReducer: directRedriveReducer,
  };
