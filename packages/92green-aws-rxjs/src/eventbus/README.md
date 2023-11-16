# 92Green AWS RXJS Eventbus Functions

## batchPut
Gather multiple eventbridge messages together before sending in a batch to reduce AWS API calls.
This function will group messages up until the batch count is reached or the total message size exceeds the maximum permitted size. It can also retry specified errors before they are eventually thrown.

### Parameter
| Parameter | Type | Required | Description |
| eventBridgeClient | EventBridgeClient | Yes | The AWS SDK V3 EventBridge client to use |
| source | string | Yes | The event source string to use for the event |
| eventBusName | string | Yes | The name of the EventBus to send events to |
| retryOn | string[] | No | An array of error code strings that should be retried. This will only work if maxAttempts is also set |
| maxAttempts | number | No | The number of times to retry errors defined in the retryOn parameter. Defaults to 0 for no retries. |
| throttleMs | number | No | The time in milliseconds to throttle retry attempts. Defaults to 500ms |
| maxMessageSize | number | No | The maximum size permitted for the message batch to reach before sending. Defaults to 256kb which is the maximum for EventBridge |
| maxBatchCount | number | No | The maximum number of messages to include in a single batch. Defaults to 10 |

### Handling errors
batchPut will throw an error when items fail to send. If errors are not appropriatly handled using rxjs `catchError` then any failed messages may cause the remainder of the batch to fail. The thrown `BatchPutError` extends Error with the following additional properties
* code: The AWS error code or `UNKNOWN` if it is not defined
* data: The original message data it was trying to send to eventbridge
