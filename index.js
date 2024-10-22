// @ts-check

const { BlobAttemptStore } = require("./blob-attempt-store");

const {
  EventHubConsumerClient,
  EventHubProducerClient,
  earliestEventPosition,
} = require("@azure/event-hubs");

const {
  BlobCheckpointStore,
} = require("@azure/eventhubs-checkpointstore-blob");

const { ContainerClient } = require("@azure/storage-blob");

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
      "Commands: { 's':send, 'sp':send-poison, 'ra':receive-all, 'ru':receive-update-cp, 'rc':receive-from-last-cp, 'rua':receive-update-events, 'q':quit }\n"
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
          /*updateCheckpointStore*/ false,
          /*updateAttemptStore*/ false
        );
        break;
      case "rc":
        await receiveLoop(
          /*useCheckpointStore*/ true,
          /*updateCheckpointStore*/ false,
          /*updateAttemptStore*/ false
        );
        break;
      case "ru":
        await receiveLoop(
          /*useCheckpointStore*/ true,
          /*updateCheckpointStore*/ true,
          /*updateAttemptStore*/ false
        );
        break;
      case "rua":
        await receiveLoop(
          /*useCheckpointStore*/ true,
          /*updateCheckpointStore*/ true,
          /*updateAttemptStore*/ true
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
 * @param {boolean} updateAttemptStore
 */
async function receiveLoop(
  useCheckpointStore,
  updateCheckpointStore,
  updateAttemptStore
) {
  while (true) {
    try {
      await receive(
        useCheckpointStore,
        updateCheckpointStore,
        updateAttemptStore
      );
      return;
    } catch (e) {
      console.log(`Error receiving events: ${e}`);
      await sleep(1000);
    }
  }
}

/**
 * @param {boolean} useCheckpointStore
 * @param {boolean} updateCheckpointStore
 */
async function receive(
  useCheckpointStore,
  updateCheckpointStore,
  updateAttemptStore
) {
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

  const attemptContainerClient = updateAttemptStore
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
        console.log(`processEvents(events.length=${events.length})`);

        if (events.length === 0) {
          console.log(
            `No events received within wait time. Waiting for next interval`
          );
          return;
        }

        if (attemptStore) {
          let attempts = await attemptStore.getAttempts(events[0], context);
          console.log(`attempts: ${attempts}`);

          if (attempts < 5) {
            try {
              attempts++;
              await attemptStore.setAttempts(events[0], context, attempts);
              console.log(`setAttempts: ${attempts}`);
            } catch (e) {
              /** @type Error */
              const error = e;
              console.log(`Error setting attempts: ${error.stack}`);

              // If any error, it's safe to no-op, since this is the default behavior without tracking attempts
            }
          } else {
            // Drop event by advancing checkpoint, even though the event has never been successfully processed
            context.updateCheckpoint(events[0]);

            // Return early.  Next callback to processEvents() should start after the dropped event.
            return;
          }
        }

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
