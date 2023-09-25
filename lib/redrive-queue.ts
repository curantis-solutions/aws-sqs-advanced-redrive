import {
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  ReceiveMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import fs from "fs";
import { QueueConfig } from "./models/config";
import { batch } from "./util";

export class RedriveQueue {
  // During initialization
  public queueConfig: QueueConfig;
  private client: SQSClient;
  public queueUrl!: string;
  public queueAttributes!: Record<string, string>;

  // Retrieve step

  constructor(queueConfig: QueueConfig, client: SQSClient) {
    this.queueConfig = queueConfig;
    this.client = client;
  }

  async getQueueUrl() {
    const queueUrl = (
      await this.client.send(
        new GetQueueUrlCommand({
          QueueName: this.queueConfig.source,
        }),
      )
    ).QueueUrl;
    if (!queueUrl)
      throw new Error(
        `Error retrieving queue url for ${this.queueConfig.source}.`,
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
      throw new Error(`No attributes for queue ${this.queueConfig.source}.`);
    this.queueAttributes = queueAttributes;
  }

  async receiveMessages(
    dataDirectory: string,
    count: number,
    parseBody: boolean,
  ): Promise<void> {
    const range = Array.from(Array(count).keys());
    const batches = batch(range, 10); // 10 is the SQS limit

    let totalMessages = 0;
    for (const batch of batches) {
      let { Messages } = await this.client.send(
        new ReceiveMessageCommand({
          AttributeNames: ["SentTimestamp"],
          MaxNumberOfMessages: batch.length,
          MessageAttributeNames: ["All"],
          QueueUrl: this.queueConfig.source,
          VisibilityTimeout: 20,
        }),
      );

      if (!Messages) return;

      // Parse Body
      if (parseBody) {
        Messages = Messages.map((message) => {
          return {
            ...message,
            Body: message.Body ? JSON.parse(message.Body) : message.Body,
          };
        });
      }

      // Write to filesystem
      const dir = `${dataDirectory}/${this.queueConfig.source}/received`;
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }
      for (const message of Messages ?? []) {
        const filename = `${dir}/${message.MessageId}.json`;
        fs.writeFileSync(filename, JSON.stringify(message, null, 2));
      }
      console.debug(
        `Received batch of ${Messages.length} messages for ${this.queueConfig.source}.`,
      );
      totalMessages += Messages.length;
    }
    console.log(
      `Received total of ${totalMessages} messages for ${this.queueConfig.source}.`,
    );
  }
}
