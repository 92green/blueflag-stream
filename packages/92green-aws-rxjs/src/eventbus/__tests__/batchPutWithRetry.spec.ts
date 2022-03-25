import batchPut from '../batchPut';
import {from, lastValueFrom} from "rxjs";
import {EventBridgeClient, PutEventsCommand, PutEventsCommandOutput} from "@aws-sdk/client-eventbridge";
import {mockClient} from "aws-sdk-client-mock";
import { toArray } from 'rxjs/operators';

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
        await lastValueFrom(from([
            {test: 'test1'},
            {test: 'test2'},
            {test: 'test3'}

        ])
            .pipe(
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
            
        expect(fn.mock.calls[0][0].Entries.length).toBe(3);
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
})
