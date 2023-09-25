import { SQSClient } from "@aws-sdk/client-sqs";
import yargs from "yargs/yargs";
import { getConfig } from "../lib/get-config";
import { RedriveClient } from "../lib/redrive-client";

const client = new SQSClient({});

const parser = yargs(process.argv.slice(2))
  .option("list", {
    alias: "l",
    type: "boolean",
    describe: "List queues.",
  })
  .option("config", {
    alias: "c",
    type: "string",
    describe: "Path to config.",
  })
  .option("receive", {
    alias: "r",
    type: "boolean",
    describe: "Receive messages and save to filesystem.",
  })
  .option("send", {
    alias: "s",
    type: "boolean",
    describe: "Send messages from the filesystem to their destination.",
  })
  .option("process", {
    alias: "p",
    type: "boolean",
    describe:
      "Copies, with potential filtering and modifications, the message from `received` to `updates/pending`, `skip`, or `deletes/pending`.",
  })
  .option("delete", {
    alias: "d",
    type: "boolean",
    describe:
      "Deletes messages from the filesystem from their source. True if `send` is enabled..",
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

  if (argv.process) {
    await redriveClient.processMessages();
  }

  if (argv.send) {
    await redriveClient.sendMessages();
  }

  if (argv.send || argv.delete) {
    await redriveClient.deleteMessages();
  }
}

script();
