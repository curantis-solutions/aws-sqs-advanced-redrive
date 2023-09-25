import { SQSClient } from "@aws-sdk/client-sqs";
import { Config } from "./models/config";
import { RedriveQueue } from "./redrive-queue";

export class RedriveClient {
  public config: Config;
  private client: SQSClient;
  private redriveQueues: Record<string, RedriveQueue> = {};

  constructor(config: Config, client: SQSClient) {
    this.config = config;
    this.client = client;
  }

  get redriveQueueList() {
    return Object.values(this.redriveQueues);
  }

  static async createClient(
    config: Config,
    client: SQSClient,
  ): Promise<RedriveClient> {
    const redriveClient = new RedriveClient(config, client);
    for (const queueConfig of redriveClient.config.queueConfigs) {
      redriveClient.redriveQueues[queueConfig.source] = new RedriveQueue(
        queueConfig,
        redriveClient.client,
      );
    }
    await Promise.all(
      redriveClient.redriveQueueList.map((redriveQueue) =>
        redriveQueue.getQueueUrl(),
      ),
    );
    await Promise.all(
      redriveClient.redriveQueueList.map((redriveQueue) =>
        redriveQueue.getAttributes(),
      ),
    );
    return redriveClient;
  }

  async receiveMessages(): Promise<void> {
    await Promise.all(
      this.redriveQueueList.map((redriveQueue) =>
        redriveQueue.receiveMessages(
          this.config.dataDirectory,
          this.config.receiveCount,
          this.config.parseBody,
        ),
      ),
    );
  }

  async sendMessages(): Promise<void> {
    await Promise.all(
      this.redriveQueueList.map((redriveQueue) =>
        redriveQueue.sendMessages(
          this.config.dataDirectory,
          this.config.parseBody,
        ),
      ),
    );
  }

  async deleteMessages(): Promise<void> {
    await Promise.all(
      this.redriveQueueList.map((redriveQueue) =>
        redriveQueue.deleteMessages(this.config.dataDirectory),
      ),
    );
  }

  printQueues() {
    console.table(
      this.redriveQueueList
        .map((redriveQueue) => redriveQueue.queueAttributes)
        .sort(
          (a, b) =>
            parseInt(b.ApproximateNumberOfMessages) -
            parseInt(a.ApproximateNumberOfMessages),
        ),
    );
  }
}
