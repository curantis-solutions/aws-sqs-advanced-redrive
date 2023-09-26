import {
  MessageProcessorReducer,
  createMessageReducer,
  messageProcesserReducers,
} from "../lib/message-processor";

export function getProcessors(): Record<string, MessageProcessorReducer> {
  return {
    ...messageProcesserReducers,
    customProcessor: createMessageReducer(
      (message) => false, // Customize this function to determine whether a message should be skipped.
      (message) => false, // Customize this function to determine whether a message should be deleted (not redriven).
      (message) => message, // Customize this function to determine whether a message should be deleted (not redriven).
    ),
  };
}
