export type Config = {
  count: number;
  dataDirectory: string;
  parseBody: boolean;
  queueConfigs: QueueConfig[];
};

export interface QueueConfig {
  source: string;
  destination: string;
}
