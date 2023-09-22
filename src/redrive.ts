import {
  DeleteMessageBatchCommand,
  Message,
  SQSClient,
  SendMessageBatchCommand,
} from "@aws-sdk/client-sqs";
import { v4 as uuidv4 } from "uuid";
import yargs from "yargs/yargs";
import { customMapper } from "../lib/custom-mapper";
import { getConfig } from "../lib/get-config";
import { RedriveClient } from "../lib/redrive-client";
import fs = require("fs");

interface Args {
  source?: string;
  destination?: string;
}

const client = new SQSClient({});
const dir = `scripts/cache`;

/**
 */
const parser = yargs(process.argv.slice(2))
  .option("source", {
    alias: "s",
    type: "string",
    describe: "source queue url",
  })
  .option("destination", {
    alias: "d",
    type: "string",
    describe: "destination queue url",
  })
  .option("list", {
    alias: "l",
    type: "boolean",
    describe: "list queues",
  })
  .option("config", {
    alias: "c",
    type: "string",
    describe: "path to config",
  })
  .demandOption(["config"])
  .help();

async function script() {
  const argv = await parser.argv;

  const config = getConfig(argv.config);
  const redriveClient = await new RedriveClient(config, client).initialize();

  redriveClient.printQueues();

  // let messages: Message[] | undefined;

  // // Retrieve messages either from source or from cache
  // if (argv.source) {
  //   console.log(`Retrieving messages from ${argv.source}.`);
  //   messages = await receiveMessages(argv);
  //   console.log(`Deleting messages.`);
  //   await deleteMessages(argv, messages!);
  // } else {
  //   console.log(`Retrieving messages from cache.`);
  //   messages = await getMessagesFromCache(argv);
  // }

  // if (!messages) return;
  // console.log(`Retrieved ${messages.length} messages.`);

  // // Map: At a minimum parse the Body. Modify `custom-mapper.ts` to
  // console.log(`Mapping messages.`);
  // const mappedMessages = await Promise.all(messages.map(messageMapper));

  // // Send to destination
  // if (argv.destination) {
  //   console.log(`Sending messages to ${argv.destination}.`);
  //   await sendMessages(argv, mappedMessages);
  // }
}

function getMessagesFromCache(args: Args): Message[] | undefined {
  const files = fs.readdirSync(dir);
  if (files.length > 0) {
    return files.map(
      (file) =>
        JSON.parse(fs.readFileSync(`${dir}/${file}`).toString()) as Message,
    );
  }
}

async function messageMapper(message: Message): Promise<any> {
  return message.Body
    ? await customMapper(JSON.parse(message.Body))
    : message.Body;
}

async function sendMessages(args: Args, mappedMessages: any[]) {
  await client.send(
    new SendMessageBatchCommand({
      QueueUrl: args.destination,
      Entries: mappedMessages.map((mappedMessage) => {
        return { Id: uuidv4(), MessageBody: JSON.stringify(mappedMessage) };
      }),
    }),
  );
}

async function deleteMessages(args: Args, messages: Message[]) {
  await client.send(
    new DeleteMessageBatchCommand({
      QueueUrl: args.source,
      Entries: messages.map((message) => ({
        Id: message.MessageId,
        ReceiptHandle: message.ReceiptHandle,
      })),
    }),
  );
}

script();
