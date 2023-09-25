import {
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  Message,
  ReceiveMessageCommand,
  SQSClient,
  SendMessageBatchCommand,
} from "@aws-sdk/client-sqs";
import fs from "fs";
import { v4 as uuidv4 } from "uuid";
import { Constants } from "./constants/constants";
import { QueueConfig } from "./models/config";
import { batch } from "./util";

export class RedriveQueue {
  public queueConfig: QueueConfig;
  private client: SQSClient;
  public queueUrl!: string;
  public queueAttributes!: Record<string, string>;

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
    receiveCount: number,
    parseBody: boolean,
  ): Promise<void> {
    const range = Array.from(Array(receiveCount).keys());
    const batches = batch(range, Constants.sqsBatchLimit);

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

      // Setup directory structure
      const baseDirectory = `${dataDirectory}/${this.queueConfig.source}`;
      const receivedDirectory = `${baseDirectory}/${Constants.receivedDirectory}`;
      this.setupDirectories(baseDirectory);

      // Write to filesystem
      for (const message of Messages ?? []) {
        const filename = `${receivedDirectory}/${message.MessageId}.json`;
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

  setupDirectories(baseDirectory: string) {
    for (const directory of Constants.directoriesToCreate) {
      if (!fs.existsSync(`${baseDirectory}/${directory}`)) {
        fs.mkdirSync(`${baseDirectory}/${directory}`, { recursive: true });
      }
      for (const subdirectory of Constants.subdirectoriesToCreate) {
        if (directory === Constants.receivedDirectory) continue;
        if (!fs.existsSync(`${baseDirectory}/${directory}/${subdirectory}`)) {
          fs.mkdirSync(`${baseDirectory}/${directory}/${subdirectory}`);
        }
      }
    }
  }

  async sendMessages(dataDirectory: string): Promise<void> {
    const baseDir = `${dataDirectory}/${this.queueConfig.source}`;
    const updatesPendingDirectory = `${baseDir}/${Constants.updatesDirectory}/${Constants.pendingSubdirectory}`;
    const updatesErrorDirectory = `${baseDir}/${Constants.updatesDirectory}/${Constants.errorsSubdirectory}`;
    const updatesArchivedDirectory = `${baseDir}/${Constants.updatesDirectory}/${Constants.archivedSubdirectory}`;
    const files = fs.readdirSync(updatesPendingDirectory);
    if (files.length === 0) {
      console.log(`No messages to send for ${this.queueConfig.source}`);
      return;
    }

    const batchedFiles = batch(files, Constants.sqsBatchLimit);
    let totalMessages = 0;
    let totalErrors = 0;
    for (const fileBatch of batchedFiles) {
      const messages = fileBatch.map(
        (file) =>
          JSON.parse(
            fs.readFileSync(`${updatesPendingDirectory}/${file}`).toString(),
          ) as Message,
      );

      try {
        await this.client.send(
          new SendMessageBatchCommand({
            QueueUrl: this.queueConfig.destination,
            Entries: messages.map((message) => {
              return { Id: uuidv4(), MessageBody: JSON.stringify(message) };
            }),
          }),
        );
        console.debug(
          `Sent batch of ${messages.length} messages to ${this.queueConfig.destination} for ${this.queueConfig.source}.`,
        );
        for (const file of fileBatch) {
          fs.renameSync(
            `${updatesPendingDirectory}/${file}`,
            `${updatesArchivedDirectory}/${file}`,
          );
        }
        totalMessages += messages.length;
      } catch (error) {
        console.error(error);
        for (const file of fileBatch) {
          fs.renameSync(
            `${updatesPendingDirectory}/${file}`,
            `${updatesErrorDirectory}/${file}`,
          );
        }
        totalErrors += messages.length;
      }
    }
    console.log(
      `Sent total of ${totalMessages} messages to ${this.queueConfig.destination} for ${this.queueConfig.source}.`,
    );
    console.log(
      `Error sending ${totalErrors} messages to ${this.queueConfig.destination} for ${this.queueConfig.source}.`,
    );
  }
}
