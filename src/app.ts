import {
  DeleteMessageBatchCommand,
  Message,
  SQSClient,
} from "@aws-sdk/client-sqs";
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
  .option("receive", {
    alias: "r",
    type: "boolean",
    describe: "receive messages and save to filesystem",
  })
  .option("send", {
    alias: "s",
    type: "boolean",
    describe: "send messages from the filesystem to their destination",
  })
  .demandOption(["config"])
  .help();

async function script() {
  const argv = await parser.argv;

  const config = getConfig(argv.config);
  const redriveClient = await RedriveClient.createClient(config, client);

  if (argv.list) {
    redriveClient.printQueues();
  }

  if (argv.receive) {
    await redriveClient.receiveMessages();
  }

  if (argv.send) {
    await redriveClient.sendMessages();
  }

  // // Map: At a minimum parse the Body. Modify `custom-mapper.ts` to
  // console.log(`Mapping messages.`);
  // const mappedMessages = await Promise.all(messages.map(messageMapper));

  // // Send to destination
  // if (argv.destination) {
  //   console.log(`Sending messages to ${argv.destination}.`);
  //   await sendMessages(argv, mappedMessages);
  // }
}

async function messageMapper(message: Message): Promise<any> {
  return message.Body
    ? await customMapper(JSON.parse(message.Body))
    : message.Body;
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
