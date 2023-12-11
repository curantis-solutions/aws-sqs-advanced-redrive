import { Message } from "@aws-sdk/client-sqs";

export interface ParsedMessage<T = any> extends Omit<Message, "Body"> {
  Body?: T;
}

export interface Skips {
  root: ParsedMessage[];
  [key: string]: ParsedMessage[];
}
export class ProcessedMessages {
  public deletes: ParsedMessage[];
  public errors: ParsedMessage[];
  public skips: Skips;
  public updates: ParsedMessage[];

  constructor() {
    this.deletes = [];
    this.errors = [];
    this.skips = {
      root: [],
    };
    this.updates = [];
  }

  combine(processedMessages: ProcessedMessages) {
    this.deletes = [...this.deletes, ...processedMessages.deletes];
    this.errors = [...this.errors, ...processedMessages.errors];
    this.updates = [...this.updates, ...processedMessages.updates];

    for (const key of Object.keys(this.skips)) {
      this.skips[key] = [...this.skips[key], ...processedMessages.skips[key]];
    }
  }
}

export type MessageProcessorReducer = (
  processMessages: ProcessedMessages,
  message: ParsedMessage,
) => ProcessedMessages;

export type SkipFunction = (message: ParsedMessage) => {
  shouldSkip: boolean;
  subdirectory?: string;
};
export type TruthyFunction = (message: ParsedMessage) => boolean;
export type UpdateMapper = (message: ParsedMessage) => ParsedMessage;

export function createMessageReducer(
  skipFunction: SkipFunction,
  shouldDelete: TruthyFunction,
  updateMapper: UpdateMapper,
): MessageProcessorReducer {
  return (processedMessages: ProcessedMessages, message: ParsedMessage) => {
    try {
      if (shouldDelete(message)) {
        processedMessages.deletes.push(message);
        return processedMessages;
      }
      const { shouldSkip, subdirectory } = skipFunction(message);
      if (shouldSkip) {
        processedMessages.skips[subdirectory ?? "root"] =
          processedMessages.skips[subdirectory ?? "root"] ?? [];
        processedMessages.skips[subdirectory ?? "root"].push(message);
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
    () => {
      return { shouldSkip: true };
    },
    () => false,
    (message) => message,
  );

export const messageProcesserReducers: Record<string, MessageProcessorReducer> =
  {
    directRedriveReducer: directRedriveReducer,
  };
