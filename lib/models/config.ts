export type Config = {
  receiveCount: number;
  dataDirectory: string;
  parseBody: boolean;
  messageProcessors?: string;
  queueConfigs: QueueConfig[];
};

export interface QueueConfig {
  source: string;
  destination: string;
  processor?: string;
}
