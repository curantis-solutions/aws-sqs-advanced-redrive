export type Config = {
  receiveCount: number;
  dataDirectory: string;
  parseBody: boolean;
  queueConfigs: QueueConfig[];
};

export interface QueueConfig {
  source: string;
  destination: string;
}
