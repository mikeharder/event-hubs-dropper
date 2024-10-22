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
      "Commands: { 's':send, 'ra':receive-all, 'ru':receive-update-cp, 'rc':receive-from-last-cp, 'q':quit }\n"
    );
    rl.close();

    switch (answer.trim()) {
      case "s":
        await send();
        break;
      case "ra":
        await receive(
          /*startPosition*/ earliestEventPosition,
          /*useCheckpointStore*/ false,
          /*updateCheckpointStore*/ false
        );
        break;
      case "ru":
        await receive(/*startPosition*/ undefined, /*useCheckpointStore*/ true, /*updateCheckpointStore*/ true);
        break;
      case "rc":
        await receive(/*startPosition*/ undefined, /*useCheckpointStore*/ true, /*updateCheckpointStore*/ false);
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
  // Create a producer client to send messages to the event hub.
  const producer = new EventHubProducerClient(ehConnectionString, ehName);

  // Prepare a batch of three events.
  const batch = await producer.createBatch();
  batch.tryAdd({ body: "First event" });
  batch.tryAdd({ body: "Second event" });
  batch.tryAdd({ body: "Third event" });

  // Send the batch to the event hub.
  await producer.sendBatch(batch);

  // Close the producer client.
  await producer.close();

  console.log("A batch of three events have been sent to the event hub");
}

/**
 * @param {import("@azure/event-hubs").EventPosition | undefined} startPosition
 * @param {boolean} useCheckpointStore
 * @param {boolean} updateCheckpointStore
 */
async function receive(startPosition, useCheckpointStore, updateCheckpointStore) {
  const checkpointStoreConnectionString =
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;";
  const checkpointStoreContainerName = "my-checkpoint-store";

  const checkpointStore = useCheckpointStore
    ? new BlobCheckpointStore(
        new ContainerClient(
          checkpointStoreConnectionString,
          checkpointStoreContainerName
        )
      )
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

  const subscription = consumerClient.subscribe(
    {
      processEvents: async (events, context) => {
        if (events.length === 0) {
          console.log(
            `No events received within wait time. Waiting for next interval`
          );
          return;
        }

        for (const event of events) {
          console.log(
            `Received event: '${event.body}' from offset '${event.offset}' and sequence '${event.sequenceNumber}' in  partition: '${context.partitionId}' and consumer group: '${context.consumerGroup}'`
          );
        }

        if (updateCheckpointStore) {
          if (!useCheckpointStore) {
            throw new Error("Cannot set updateCheckpointStore=true if useCheckpointStore=false");
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
        console.log(`Error : ${err}`);
      },
    },
    { startPosition: startPosition }
  );

  while (true) {
    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
    });
    const answer = await rl.question("Press 'q' to quit...}\n");
    rl.close();
    switch (answer.trim()) {
      case "q":
        await subscription.close();
        await consumerClient.close();
        return;
      default:
        console.log(`Unknown command: ${answer.trim()}`);
        break;
    }
  }
}

//   // Subscribe to the events, and specify handlers for processing the events and errors.
//   const subscription = consumerClient.subscribe(
//     {
//       processEvents: async (events, context) => {
//         if (events.length === 0) {
//           console.log(
//             `No events received within wait time. Waiting for next interval`
//           );
//           return;
//         }

//         for (const event of events) {
//           console.log(
//             `Received event: '${event.body}' from partition: '${context.partitionId}' and consumer group: '${context.consumerGroup}'`
//           );
//         }

//         try {
//           await context.updateCheckpoint(events[events.length - 1]);
//         } catch (err) {
//           console.log(
//             `Error when checkpointing on partition ${context.partitionId}: `,
//             err
//           );
//           throw err;
//         }

//         console.log(
//           `Successfully checkpointed event with sequence number: ${
//             events[events.length - 1].sequenceNumber
//           } from partition: 'partitionContext.partitionId'`
//         );
//       },

//       processError: async (err, context) => {
//         console.log(`Error : ${err}`);
//       },
//     },
//     { startPosition: earliestEventPosition }
//   );

//   // After 30 seconds, stop processing.
//   await new Promise((resolve) => {
//     setTimeout(async () => {
//       await subscription.close();
//       await consumerClient.close();
//       resolve();
//     }, 30000);
//   });
// }

main().catch((err) => {
  console.log("Error occurred: ", err);
});
