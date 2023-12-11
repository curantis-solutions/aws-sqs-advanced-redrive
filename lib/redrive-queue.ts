import {
  DeleteMessageBatchCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  ReceiveMessageCommand,
  SQSClient,
  SendMessageBatchCommand,
} from "@aws-sdk/client-sqs";
import fs from "fs";
import { v4 as uuidv4 } from "uuid";
import { Constants } from "./constants/constants";
import {
  MessageProcessorReducer,
  ParsedMessage,
  ProcessedMessages,
} from "./message-processor";
import { QueueConfig } from "./models/config";
import { batch } from "./util";

export class RedriveQueue {
  public queueConfig: QueueConfig;
  private client: SQSClient;
  private baseDirectory: string;
  public queueUrl!: string;
  public queueAttributes!: Record<string, string>;

  constructor(
    queueConfig: QueueConfig,
    client: SQSClient,
    dataDirectory: string,
  ) {
    this.queueConfig = queueConfig;
    this.client = client;
    this.baseDirectory = `${dataDirectory}/${this.queueConfig.source}`;
    this.setupDirectories();
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

      if (!Messages) break;

      // Parse Body
      if (parseBody) {
        Messages = Messages.map((message) => {
          return {
            ...message,
            Body: message.Body ? JSON.parse(message.Body) : message.Body,
          };
        });
      }

      const receivedDirectory = `${this.baseDirectory}/${Constants.receivedDirectory}`;
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

  setupDirectories() {
    for (const directory of Constants.directoryList) {
      if (!fs.existsSync(`${this.baseDirectory}/${directory}`)) {
        fs.mkdirSync(`${this.baseDirectory}/${directory}`, { recursive: true });
      }
      if (Constants.directoriesWithSubdirectories.includes(directory)) {
        for (const subdirectory of Constants.subdirectoriesToCreate) {
          if (
            !fs.existsSync(`${this.baseDirectory}/${directory}/${subdirectory}`)
          ) {
            fs.mkdirSync(`${this.baseDirectory}/${directory}/${subdirectory}`);
          }
        }
      }
    }
  }

  clean(all: boolean) {
    let count = 0;
    for (const directory of Constants.directoryList) {
      // Delete each file in subdirectories
      if (Constants.directoriesWithSubdirectories.includes(directory)) {
        for (const subdirectory of Constants.subdirectoriesToCreate) {
          count = this.cleanDirectory(
            `${this.baseDirectory}/${directory}/${subdirectory}`,
            count,
            false,
          );
        }
      }
      // Delete each flie in directories without subdirectories
      else {
        // Preserve the received directory unless `all` is true
        if (!all && directory === Constants.receivedDirectory) {
          continue;
        }
        // Delete every file in each directory
        count = this.cleanDirectory(
          `${this.baseDirectory}/${directory}`,
          count,
          directory === Constants.skipsDirectory, // Recursively delete for the skips directory only
        );
      }
    }
    console.log(`Clean deleted ${count} files/directories.`);
  }

  cleanDirectory(directory: string, count: number, recursive = false): number {
    for (const file of fs.readdirSync(directory)) {
      count++;
      const filePath = `${directory}/${file}`;
      if (fs.lstatSync(filePath).isDirectory()) {
        if (recursive) {
          fs.rmSync(filePath, { recursive: true });
        }
        continue;
      }
      fs.rmSync(filePath);
    }
    return count;
  }

  async sendMessages(parseBody: boolean): Promise<void> {
    const updatesPendingDirectory = `${this.baseDirectory}/${Constants.updatesDirectory}/${Constants.pendingSubdirectory}`;
    const updatesErrorDirectory = `${this.baseDirectory}/${Constants.updatesDirectory}/${Constants.errorsSubdirectory}`;
    const updatesArchivedDirectory = `${this.baseDirectory}/${Constants.updatesDirectory}/${Constants.archivedSubdirectory}`;
    const deletesPendingDirectory = `${this.baseDirectory}/${Constants.deletesDirectory}/${Constants.pendingSubdirectory}`;

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
          ) as ParsedMessage,
      );

      let atLeastOneFailure = false;
      try {
        const { Failed } = await this.client.send(
          new SendMessageBatchCommand({
            QueueUrl: this.queueConfig.destination,
            Entries: messages.map((message) => {
              return {
                Id: uuidv4(),
                MessageBody: parseBody
                  ? JSON.stringify(message.Body)
                  : message.Body,
              };
            }),
          }),
        );

        atLeastOneFailure = (Failed?.length ?? 0) > 0;
        if (atLeastOneFailure) {
          throw new Error(
            `At least one message in batch failed to send to ${this.queueConfig.destination} for ${this.queueConfig.source}.`,
          );
        }

        console.debug(
          `Sent batch of ${fileBatch.length} messages to ${this.queueConfig.destination} for ${this.queueConfig.source}.`,
        );
        for (const file of fileBatch) {
          // Copy successful sends to the `deletes/pending` folder
          fs.cpSync(
            `${updatesPendingDirectory}/${file}`,
            `${deletesPendingDirectory}/${file}`,
          );
          // Move successful sends to archive folder
          fs.renameSync(
            `${updatesPendingDirectory}/${file}`,
            `${updatesArchivedDirectory}/${file}`,
          );
        }
        totalMessages += fileBatch.length;
      } catch (error) {
        console.error(error);
        atLeastOneFailure = true;
      }

      if (atLeastOneFailure) {
        for (const file of fileBatch) {
          fs.renameSync(
            `${updatesPendingDirectory}/${file}`,
            `${updatesErrorDirectory}/${file}`,
          );
        }
        totalErrors += fileBatch.length;
      }
    }
    console.log(
      `Sent total of ${totalMessages} messages to ${this.queueConfig.destination} for ${this.queueConfig.source}.`,
    );
    if (totalErrors > 0) {
      console.log(
        `Error sending ${totalErrors} messages to ${this.queueConfig.destination} for ${this.queueConfig.source}.`,
      );
    }
  }

  async processMessages(
    messageProcessorReducer: MessageProcessorReducer,
  ): Promise<void> {
    const receivedDirectory = `${this.baseDirectory}/${Constants.receivedDirectory}`;
    const processingErrorsDirectory = `${this.baseDirectory}/${Constants.processingErrors}`;
    const deletesPendingDirectory = `${this.baseDirectory}/${Constants.deletesDirectory}/${Constants.pendingSubdirectory}`;
    const updatesPendingDirectory = `${this.baseDirectory}/${Constants.updatesDirectory}/${Constants.pendingSubdirectory}`;
    const skipsDirectory = `${this.baseDirectory}/${Constants.skipsDirectory}`;

    const files = fs.readdirSync(receivedDirectory);
    if (files.length === 0) {
      console.log(`No messages to process for ${this.queueConfig.source}`);
      return;
    }

    const batchedFiles = batch(files, Constants.processingBatchLimit);

    const processedMessages = new ProcessedMessages();
    let skipsProcessedCount = 0;
    for (const fileBatch of batchedFiles) {
      const messages = fileBatch.map(
        (file) =>
          JSON.parse(
            fs.readFileSync(`${receivedDirectory}/${file}`).toString(),
          ) as ParsedMessage,
      );
      const batchProcessedMessages = messages.reduce(
        messageProcessorReducer,
        new ProcessedMessages(),
      );
      const skipsBatchedCount = Object.values(
        batchProcessedMessages.skips,
      ).reduce((acc, val) => acc + val.length, 0);
      console.debug(
        `Processed batch. ${batchProcessedMessages.deletes.length} deletes, ${batchProcessedMessages.errors.length} errors, ${skipsBatchedCount} skips, ${batchProcessedMessages.updates.length} updates for ${this.queueConfig.source}.`,
      );

      // Route messages
      for (const message of batchProcessedMessages.deletes) {
        fs.cpSync(
          `${receivedDirectory}/${message.MessageId!}.json`,
          `${deletesPendingDirectory}/${message.MessageId!}.json`,
        );
      }
      for (const message of batchProcessedMessages.errors) {
        fs.cpSync(
          `${receivedDirectory}/${message.MessageId!}.json`,
          `${processingErrorsDirectory}/${message.MessageId!}.json`,
        );
      }
      for (const [key, value] of Object.entries(batchProcessedMessages.skips)) {
        for (const message of value) {
          const skipSubdirectory = key === "root" ? "" : key;
          skipsProcessedCount++;
          fs.cpSync(
            `${receivedDirectory}/${message.MessageId!}.json`,
            `${skipsDirectory}/${skipSubdirectory}/${message.MessageId!}.json`,
          );
        }
      }
      for (const message of batchProcessedMessages.updates) {
        fs.writeFileSync(
          `${updatesPendingDirectory}/${message.MessageId!}.json`,
          JSON.stringify(message, null, 2),
        );
      }
      processedMessages.combine(batchProcessedMessages);
    }
    console.log(
      `Processed all messages. ${processedMessages.deletes.length} deletes, ${processedMessages.errors.length} errors, ${skipsProcessedCount} skips, ${processedMessages.updates.length} updates for ${this.queueConfig.source}.`,
    );
  }

  async deleteMessages(): Promise<void> {
    const deletesPendingDirectory = `${this.baseDirectory}/${Constants.deletesDirectory}/${Constants.pendingSubdirectory}`;
    const deletesErrorsDirectory = `${this.baseDirectory}/${Constants.deletesDirectory}/${Constants.errorsSubdirectory}`;
    const deletesArchivedDirectory = `${this.baseDirectory}/${Constants.deletesDirectory}/${Constants.archivedSubdirectory}`;

    const files = fs.readdirSync(deletesPendingDirectory);
    if (files.length === 0) {
      console.log(`No messages to delete for ${this.queueConfig.source}`);
      return;
    }

    const batchedFiles = batch(files, Constants.sqsBatchLimit);
    let totalMessages = 0;
    let totalErrors = 0;
    for (const fileBatch of batchedFiles) {
      const messages = fileBatch.map(
        (file) =>
          JSON.parse(
            fs.readFileSync(`${deletesPendingDirectory}/${file}`).toString(),
          ) as ParsedMessage,
      );

      let atLeastOneFailure = false;
      try {
        const { Failed } = await this.client.send(
          new DeleteMessageBatchCommand({
            QueueUrl: this.queueConfig.source,
            Entries: messages.map((message) => ({
              Id: message.MessageId,
              ReceiptHandle: message.ReceiptHandle,
            })),
          }),
        );

        atLeastOneFailure = (Failed?.length ?? 0) > 0;
        if (atLeastOneFailure) {
          throw new Error(
            `At least one message in batch failed to delete for ${this.queueConfig.source}.`,
          );
        }

        console.debug(
          `Deleted batch of ${fileBatch.length} messages from ${this.queueConfig.source}.`,
        );

        for (const file of fileBatch) {
          // Move successful deletes to archive folder
          fs.renameSync(
            `${deletesPendingDirectory}/${file}`,
            `${deletesArchivedDirectory}/${file}`,
          );
        }
        totalMessages += fileBatch.length;
      } catch (error) {
        console.error(error);
        atLeastOneFailure = true;
      }

      if (atLeastOneFailure) {
        for (const file of fileBatch) {
          fs.renameSync(
            `${deletesPendingDirectory}/${file}`,
            `${deletesErrorsDirectory}/${file}`,
          );
        }
        totalErrors += fileBatch.length;
      }
    }
    console.log(
      `Deleted total of ${totalMessages} messages from ${this.queueConfig.source}.`,
    );
    if (totalErrors > 0) {
      console.log(
        `Error deleting ${totalErrors} messages to from ${this.queueConfig.source}.`,
      );
    }
  }
}
