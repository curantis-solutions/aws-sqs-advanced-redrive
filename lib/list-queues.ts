import { GetQueueAttributesCommand, SQSClient } from "@aws-sdk/client-sqs";

export async function listQueues(client: SQSClient, queueUrls: string[]) {
  return await Promise.all(
    queueUrls.map((queueUrl) =>
      client.send(
        new GetQueueAttributesCommand({
          QueueUrl: queueUrl,
          AttributeNames: ["All"],
        }),
      ),
    ),
  );
}
