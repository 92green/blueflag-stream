import batchPut, {BatchPutError} from '../batchPut';
import {from, lastValueFrom} from "rxjs";
import {EventBridgeClient, PutEventsCommand, PutEventsCommandOutput} from "@aws-sdk/client-eventbridge";
import {mockClient} from "aws-sdk-client-mock";
import { toArray, distinct } from 'rxjs/operators';

describe('batchPut', () => {
    it('retrys on ThrottlingException', async  () => {
        let eventBridgeClient = new EventBridgeClient({});
        let responsePayload1: PutEventsCommandOutput = {
            $metadata: {},
            Entries: [
                {
                    ErrorCode: "ThrottlingException"
                }
            ]
        }

        let responsePayload2 = {
            Entries: [
                {EventId: "abcd"}
            ]
        }

        let fn = jest.fn()
            .mockImplementationOnce(() => Promise.resolve(responsePayload1))
            .mockImplementationOnce(() => Promise.resolve(responsePayload2))

        mockClient(eventBridgeClient)
            .on(PutEventsCommand)
            .callsFake(fn)

        await lastValueFrom(from([{test: 'test1'}])
            .pipe(
                batchPut({
                    eventBridgeClient,
                    detailType: 'test',
                    source: 'learningrecord.test',
                    eventBusName: 'eventbustest',
                    maxAttempts: 10,
                    throttleMs: 0
                })
            ),
        )
        expect(fn).toBeCalledTimes(2);
    });

    it('Throws error on alternative failure', async  () => {
        expect.assertions(3);
        let eventBridgeClient = new EventBridgeClient({});
        let responsePayload: PutEventsCommandOutput = {
            $metadata: {},
            Entries: [
                {
                    ErrorCode: "DifferentError"
                }
            ]
        }

        let fn = jest.fn()
            .mockImplementationOnce(() => Promise.resolve(responsePayload))

        mockClient(eventBridgeClient)
            .on(PutEventsCommand)
            .callsFake(fn)

        const message = {test: 'test1'};
        try {
            await lastValueFrom(from([message])
                .pipe(
                    batchPut({
                        eventBridgeClient,
                        detailType: 'test',
                        source: 'learningrecord.test',
                        eventBusName: 'eventbustest',
                        maxAttempts: 10,
                        throttleMs: 0
                    })
                )
            );
        } catch (err) {
            expect(err).toBeInstanceOf(Error);
            expect((err as BatchPutError).code).toEqual('DifferentError');
            expect((err as BatchPutError).data).toEqual(expect.objectContaining({Detail: JSON.stringify(message)}));
        }
    });
    it('buffers entrys into one call', async () => {
        let eventBridgeClient = new EventBridgeClient({});
        let responsePayload = {
            Entries: [{EventId: "abcd"}, {EventId: "efg"}, {EventId: "hij"}]
        }
        let fn = jest.fn()
        .mockImplementation(() => Promise.resolve(responsePayload))
        mockClient(eventBridgeClient)
            .on(PutEventsCommand)
            .callsFake(fn)

        const messages = [
            {test: 'test1'},
            {test: 'test2'},
            {test: 'test3'}
        ];
        await lastValueFrom(
            from(messages)
            .pipe(
                distinct(({test}) => test),
                batchPut({
                    eventBridgeClient,
                    detailType: 'test',
                    source: 'learningrecord.test',
                    eventBusName: 'eventbustest',
                    maxAttempts: 1,
                    throttleMs: 0
                }),
                toArray()
            ))

        expect(fn).toHaveBeenCalled();
        expect(fn.mock.calls[0][0].Entries).toHaveLength(messages.length);
    });
    it('uses the event bus name specified in the config', async () => {
        let eventBridgeClient = new EventBridgeClient({});
        let responsePayload = {
            Entries: [{EventId: "abcd"}]
        }
        let fn = jest.fn()
        .mockImplementation(() => Promise.resolve(responsePayload))
        mockClient(eventBridgeClient)
            .on(PutEventsCommand)
            .callsFake(fn)
        await lastValueFrom(from([{test: 'test1'}])
            .pipe(
                batchPut({
                    eventBridgeClient,
                    detailType: 'test',
                    source: 'learningrecord.test',
                    eventBusName: 'eventbustest',
                    maxAttempts: 1,
                    throttleMs: 0
                })
            ))
            expect(fn.mock.calls[0][0].Entries).toContainEqual(
                expect.objectContaining(
                {
                    EventBusName: 'eventbustest'
                })
            );
    });
    it('Calls putEvents on the supplied eventBridge.', async () => {
        let eventBridgeClient = new EventBridgeClient({});
        let responsePayload = {
            Entries: [{
                EventId: "abcd"
            }]
        }
        let fn = jest.fn()
            .mockImplementation(() => Promise.resolve(responsePayload))
        mockClient(eventBridgeClient)
            .on(PutEventsCommand)
            .callsFake(fn)
        const $obs = from([{test: 'test1'}])
            .pipe(
                batchPut({
                    eventBridgeClient,
                    detailType: 'test',
                    source: 'learningrecord.test',
                    eventBusName: 'eventbustest',
                    maxAttempts: 1,
                    throttleMs: 0
                })
                // toArray()
            )

        await lastValueFrom($obs);
        expect(fn.mock.calls.length).toBe(1);
    });

    it('JSON encodes the event detail and puts it on the event bridge', async () => {
        let eventBridgeClient = new EventBridgeClient({});
        let responsePayload = {
            Entries: [{EventId: "abcd"}]
        }
        let fn = jest.fn()
        .mockImplementation(() => Promise.resolve(responsePayload))
            mockClient(eventBridgeClient)
                .on(PutEventsCommand)
                .callsFake(fn)

        let [event] = await lastValueFrom(from([{test: 'test1'}])
            .pipe(
                batchPut({
                    eventBridgeClient,
                    detailType: 'test',
                    source: 'learningrecord.test',
                    eventBusName: 'eventbustest',
                    maxAttempts: 1,
                    throttleMs: 0
                })
            ))
        let result = JSON.parse(event.Detail || "{}");
        expect(result).toEqual({test: 'test1'});
    });

    it('sets the source and specified in the config', async () => {
        let eventBridgeClient = new EventBridgeClient({});
        let responsePayload = {
            Entries: [{EventId: "abcd"}]
        }
        let fn = jest.fn()
        .mockImplementation(() => Promise.resolve(responsePayload))
            mockClient(eventBridgeClient)
                .on(PutEventsCommand)
                .callsFake(fn)

        await lastValueFrom(from([{test: 'test1'}])
            .pipe(
                batchPut({
                    throttleMs: 0,
                    eventBridgeClient,
                    detailType: 'test',
                    source: 'learningrecord.test',
                    eventBusName: 'test',
                    maxAttempts: 1
                })
            )
        )
        expect(fn.mock.calls[0][0].Entries).toContainEqual(
            expect.objectContaining(
            {
                Source: 'learningrecord.test'
            })
        );
    });


  it('does not put groups of messages over the size limit', async () => {
    const sizeLimit = 10 * 1024; // 10 KB
    const largeMessage = { test: 'a'.repeat(sizeLimit / 2) }; // Creates a large message half the size of the limit
    const messages = [largeMessage, largeMessage, largeMessage]; // This should exceed the limit when combined

    let eventBridgeClient = new EventBridgeClient({});
    let putCmd = jest.fn().mockImplementation(({Entries}) => Promise.resolve({
        Entries: Entries.map((_: unknown, index: number) => ({ EventId: `event_${index}` }))
    }));
    mockClient(eventBridgeClient)
        .on(PutEventsCommand)
        .callsFake(putCmd)

    await lastValueFrom(
      from(messages).pipe(
        batchPut({
          eventBridgeClient,
          detailType: 'test',
          source: 'learningrecord.test',
          eventBusName: 'eventbustest',
          maxAttempts: 1,
          throttleMs: 0,
          maxMessageSize: sizeLimit,
          maxBatchCount: 10 // Set a higher batch count to test size limit
        }),
        toArray()
      )
    );

    // The function should have split the messages into separate batches due to size constraints
    expect(putCmd).toHaveBeenCalledTimes(3);
  });

  it('does not put groups of messages over the count limit', async () => {
    const countLimit = 10;
    const messages = new Array(countLimit + 5).fill({ test: 'test' }); // Creates more messages than the count limit


    let eventBridgeClient = new EventBridgeClient({});
    let putCmd = jest.fn().mockImplementation(({Entries}) => Promise.resolve({
        Entries: Entries.map((_: unknown, index: number) => ({ EventId: `event_${index}` }))
    }));
    mockClient(eventBridgeClient)
        .on(PutEventsCommand)
        .callsFake(putCmd)

    await lastValueFrom(
      from(messages).pipe(
        batchPut({
          eventBridgeClient,
          detailType: 'test',
          source: 'learningrecord.test',
          eventBusName: 'eventbustest',
          maxAttempts: 1,
          throttleMs: 0,
          maxBatchCount: countLimit // Set the batch count limit
        }),
        toArray()
      )
    );

    // The function should have split the messages into 2 batches: one with the count limit and one with the remainder
    expect(putCmd).toHaveBeenCalledTimes(2);
    expect(putCmd.mock.calls[0][0].Entries).toHaveLength(countLimit);
    expect(putCmd.mock.calls[1][0].Entries).toHaveLength(5);
  });
})
