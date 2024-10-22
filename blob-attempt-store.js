// @ts-check

const path = require("path");
const { ContainerClient, RestError } = require("@azure/storage-blob");

// Design heavily influenced by @azure/eventhubs-checkpointstore-blob/BlobCheckpointStore
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
      if (
        props.metadata?.offset == event.offset.toString() &&
        props.metadata?.sequencenumber == event.sequenceNumber.toString()
      ) {
        return Number.parseInt(props.metadata.attempts);
      } else {
        // If offset or sequencenumber don't match, it must be before the first attempt of this event
        return 0;
      }
    } catch (e) {
      /** @type RestError */
      const restError = e;
      if (restError.statusCode == 404) {
        // If blob does not exist, it must be before the first attempt of any event, so not an error
        return 0;
      } else {
        // Propagate any other errors
        throw e;
      }
    }
  }

  /**
   * @param {import("@azure/event-hubs").ReceivedEventData} event
   * @param {import("@azure/event-hubs").PartitionContext} context
   * @param {number} attempts
   */
  async setAttempts(event, context, attempts) {
    const blobName = this.#getBlobName(context);
    const blobClient = this.#containerClient.getBlockBlobClient(blobName);

    const metadata = {
      offset: event.offset.toString(),
      sequencenumber: event.sequenceNumber.toString(),
      attempts: attempts.toString(),
    };

    await blobClient.upload("", 0, { metadata: metadata });
  }

  /**
   * @param {import("@azure/event-hubs").PartitionContext} context
   * @returns {string} Unique blob name derived from properties of the context. Example: "my-eh.servicebus.windows.net/eh1/$default/attempt/0"
   */
  #getBlobName(context) {
    // Path copied from BlobCheckpointStore, replacing "checkpoint" with "attempt"
    return path.join(
      context.fullyQualifiedNamespace,
      context.eventHubName,
      context.consumerGroup,
      "attempt",
      context.partitionId
    );
  }
}

exports.BlobAttemptStore = BlobAttemptStore;
