// @ts-check

const {
  EventHubConsumerClient,
  EventHubProducerClient,
  earliestEventPosition,
} = require("@azure/event-hubs");

const {
  BlobCheckpointStore,
} = require("@azure/eventhubs-checkpointstore-blob");

const { ContainerClient } = require("@azure/storage-blob");

const path = require("path");

const readline = require("readline/promises");

const ehConnectionString =
  "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true";
const ehName = "eh1";
const ehConsumerGroup = "$Default";

async function main() {
  while (true) {
    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
    });
    const answer = await rl.question(
      "Commands: { 's':send, 'sp':send-poison, 'ra':receive-all, 'ru':receive-update-cp, 'rc':receive-from-last-cp, 'q':quit }\n"
    );
    rl.close();

    switch (answer.trim()) {
      case "s":
        await send();
        break;
      case "sp":
        await sendPoison();
        break;
      case "ra":
        await receiveLoop(
          /*useCheckpointStore*/ false,
          /*updateCheckpointStore*/ false
        );
        break;
      case "ru":
        await receiveLoop(
          /*useCheckpointStore*/ true,
          /*updateCheckpointStore*/ true
        );
        break;
      case "rc":
        await receiveLoop(
          /*useCheckpointStore*/ true,
          /*updateCheckpointStore*/ false
        );
        break;
      case "q":
        process.exit(0);
      default:
        console.log(`Unknown command: ${answer.trim()}`);
        break;
    }
  }
}

async function send() {
  const producer = new EventHubProducerClient(ehConnectionString, ehName);

  const batch = await producer.createBatch();
  batch.tryAdd({ body: "First event" });
  batch.tryAdd({ body: "Second event" });
  batch.tryAdd({ body: "Third event" });

  await producer.sendBatch(batch);

  await producer.close();

  console.log("A batch of three events have been sent to the event hub");
}

async function sendPoison() {
  const producer = new EventHubProducerClient(ehConnectionString, ehName);

  const batch = await producer.createBatch();
  batch.tryAdd({ body: "poison" });
  await producer.sendBatch(batch);
  await producer.close();

  console.log("A poison event have been sent to the event hub");
}

/**
 * @param {boolean} useCheckpointStore
 * @param {boolean} updateCheckpointStore
 */
async function receiveLoop(useCheckpointStore, updateCheckpointStore) {
  while (true) {
    try {
      await receive(useCheckpointStore, updateCheckpointStore);
      return;
    } catch (e) {
      console.log(`Error receiving events: ${e}`);
      await sleep(1000);
    }
  }
}

class BlobAttemptStore {
  #containerClient;

  /**
   * @param {ContainerClient} containerClient
   */
  constructor(containerClient) {
    this.#containerClient = containerClient;
  }

  /**
   * @param {import("@azure/event-hubs").ReceivedEventData} event
   * @param {import("@azure/event-hubs").PartitionContext} context
   * @returns {Promise<number>} Number of times the event has been attempted. Returns '0' before the first attempt.
   */
  async getAttempts(event, context) {
    const blobName = this.#getBlobName(context);
    const blobClient = this.#containerClient.getBlobClient(blobName);
    try {
      const props = await blobClient.getProperties();
    }
    catch {
    }
    return 0;
  }

  /**
   * @param {import("@azure/event-hubs").ReceivedEventData} event
   * @param {import("@azure/event-hubs").PartitionContext} context
   * @param {number} attempts
   */
  async setAttempts(event, context, attempts) {
  }

  /**
   * @param {import("@azure/event-hubs").PartitionContext} context
   * @returns {string} Unique blob name derived from properties of the context. Example: "my-eh.servicebus.windows.net/eh1/$default/attempt/0"
   */
  #getBlobName(context) {
    return path.join(context.fullyQualifiedNamespace, context.eventHubName, context.consumerGroup, "attempt", context.partitionId);
  }
}

/**
 * @param {boolean} useCheckpointStore
 * @param {boolean} updateCheckpointStore
 */
async function receive(useCheckpointStore, updateCheckpointStore) {
  const checkpointStoreConnectionString =
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;";
  const checkpointStoreContainerName = "my-checkpoint-store";
  const attemptStoreContainerName = "my-attempt-store";

  const containerClient = useCheckpointStore
    ? new ContainerClient(
        checkpointStoreConnectionString,
        checkpointStoreContainerName
      )
    : undefined;

  if (containerClient) {
    await containerClient.createIfNotExists();
  }

  const checkpointStore = containerClient
    ? new BlobCheckpointStore(containerClient)
    : undefined;

  const consumerClient = checkpointStore
    ? new EventHubConsumerClient(
        ehConsumerGroup,
        ehConnectionString,
        ehName,
        checkpointStore,
        { loadBalancingOptions: { strategy: "greedy" } }
      )
    : new EventHubConsumerClient(ehConsumerGroup, ehConnectionString, ehName, {
        loadBalancingOptions: { strategy: "greedy" },
      });

  const attemptContainerClient = useCheckpointStore
    ? new ContainerClient(
        checkpointStoreConnectionString,
        attemptStoreContainerName
      )
    : undefined;

  if (attemptContainerClient) {
    await attemptContainerClient.createIfNotExists();
  }

  const attemptStore = attemptContainerClient
    ? new BlobAttemptStore(attemptContainerClient)
    : undefined;

  const subscriptionTerminated = createManualPromise();

  const subscription = consumerClient.subscribe(
    {
      processEvents: async (events, context) => {
        if (events.length === 0) {
          console.log(
            `No events received within wait time. Waiting for next interval`
          );
          return;
        }

        // TODO: Track attempts based on events[0] in storage blobs.  If exceed max attempts (say 5), drop message and advance checkpoint to prevent getting stuck.
        // attemptStore

        for (const event of events) {
          console.log(
            `Received event: '${event.body}' from offset '${event.offset}' and sequence '${event.sequenceNumber}' in  partition: '${context.partitionId}' and consumer group: '${context.consumerGroup}'`
          );

          if (event.body == "poison") {
            console.log("poison!!!");
            throw new Error("poisoned message");
          }
        }

        if (updateCheckpointStore) {
          if (!useCheckpointStore) {
            throw new Error(
              "Cannot set updateCheckpointStore=true if useCheckpointStore=false"
            );
          }
          try {
            await context.updateCheckpoint(events[events.length - 1]);
          } catch (err) {
            console.log(
              `Error when checkpointing on partition ${context.partitionId}: `,
              err
            );
            throw err;
          }
          console.log(
            `Successfully checkpointed event with sequence number: ${
              events[events.length - 1].sequenceNumber
            } from partition: 'partitionContext.partitionId'`
          );
        }
      },
      processError: async (err, context) => {
        console.log(`processError: ${err}`);
        // subscription.close();
        // @ts-ignore
        subscriptionTerminated.resolve(err);
      },
    },
    { startPosition: earliestEventPosition }
  );

  while (true) {
    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
    });
    const answer = await Promise.any([
      subscriptionTerminated.promise,
      rl.question("Press 'q' to quit...}\n"),
    ]);
    console.log(`answer: ${answer}`);
    rl.close();
    if (typeof answer === "string") {
      switch (answer.trim()) {
        case "q":
          await subscription.close();
          await consumerClient.close();
          return;
        default:
          console.log(`Unknown command: ${answer.trim()}`);
          break;
      }
    } else {
      console.log("subscription terminated");
      subscription.close();
      throw answer;
    }
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function createManualPromise() {
  let resolve, reject;

  // Create a new promise and capture the resolve and reject functions
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });

  // Return the promise along with the resolve and reject functions
  return {
    promise,
    resolve,
    reject,
  };
}

main().catch((err) => {
  console.log("Error occurred: ", err);
});
