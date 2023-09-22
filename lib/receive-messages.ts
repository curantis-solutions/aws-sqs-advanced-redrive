import fs from "fs";
import { ReceiveMessageCommand, SQSClient } from "@aws-sdk/client-sqs";

export async function receiveMessages(
  client: SQSClient,
  queueUrl: string,
  dir: string,
) {
  const { Messages } = await client.send(
    new ReceiveMessageCommand({
      AttributeNames: ["SentTimestamp"],
      MaxNumberOfMessages: 10,
      MessageAttributeNames: ["All"],
      QueueUrl: queueUrl,
      VisibilityTimeout: 20,
    }),
  );

  if (!Messages) return;

  // Write to cache
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
  for (const message of Messages ?? []) {
    const filename = `${dir}/${message.MessageId}.json`;
    fs.writeFileSync(filename, JSON.stringify(message, null, 2));
  }

  return Messages;
}
