import {
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import { Config, ConfigEntry } from "./models/config";

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

  async initialize(): Promise<RedriveClient> {
    for (const configEntry of this.config) {
      this.redriveQueues[configEntry.source] = new RedriveQueue(
        configEntry,
        this.client,
      );
    }
    await Promise.all(
      this.redriveQueueList.map((redriveQueue) => redriveQueue.getQueueUrl()),
    );
    await Promise.all(
      this.redriveQueueList.map((redriveQueue) => redriveQueue.getAttributes()),
    );
    return this;
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

export class RedriveQueue {
  // During initialization
  public configEntry: ConfigEntry;
  private client: SQSClient;
  public queueUrl!: string;
  public queueAttributes!: Record<string, string>;

  // Retrieve step

  constructor(configEntry: ConfigEntry, client: SQSClient) {
    this.configEntry = configEntry;
    this.client = client;
  }

  async getQueueUrl() {
    const queueUrl = (
      await this.client.send(
        new GetQueueUrlCommand({
          QueueName: this.configEntry.source,
        }),
      )
    ).QueueUrl;
    if (!queueUrl)
      throw new Error(
        `Error retrieving queue url for ${this.configEntry.source}.`,
      );
    this.queueUrl = queueUrl;
  }

  async getAttributes() {
    const queueAttributes = (
      await this.client.send(
        new GetQueueAttributesCommand({
          QueueUrl: this.queueUrl,
          AttributeNames: [
            "ApproximateNumberOfMessages",
            "CreatedTimestamp",
            "QueueArn",
          ],
        }),
      )
    ).Attributes;
    if (!queueAttributes)
      throw new Error(`No attributes for queue ${this.configEntry.source}.`);
    this.queueAttributes = queueAttributes;
  }
}
