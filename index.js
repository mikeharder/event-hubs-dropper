const {
  EventHubConsumerClient,
  EventHubProducerClient,
  earliestEventPosition,
} = require("@azure/event-hubs");

const connectionString =
  "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true";
const eventHubName = "eh1";
const consumerGroup = "$Default"; // name of the default consumer group

async function main() {
  // Create a producer client to send messages to the event hub.
  const producer = new EventHubProducerClient(connectionString, eventHubName);

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

  const consumerClient = new EventHubConsumerClient(
    consumerGroup,
    connectionString,
    eventHubName
  );

  // Subscribe to the events, and specify handlers for processing the events and errors.
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
            `Received event: '${event.body}' from partition: '${context.partitionId}' and consumer group: '${context.consumerGroup}'`
          );
        }
      },

      processError: async (err, context) => {
        console.log(`Error : ${err}`);
      },
    },
    { startPosition: earliestEventPosition }
  );

  // After 30 seconds, stop processing.
  await new Promise((resolve) => {
    setTimeout(async () => {
      await subscription.close();
      await consumerClient.close();
      resolve();
    }, 30000);
  });
}

main().catch((err) => {
  console.log("Error occurred: ", err);
});
