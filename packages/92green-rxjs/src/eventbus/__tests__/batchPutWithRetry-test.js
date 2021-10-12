import batchPut from '../batchPut';
const AWS = require('aws-sdk-mock');
import {from} from "rxjs";
import { first } from 'rxjs/operators';
describe('batchPut', () => {
    it('retrys on ThrottlingException', async  () => {
        let responsePayload = {
            Entries: [
                {
                    ErrorCode: "ThrottlingException"
                }
            ]
        }
        let putEvents = jest.fn().mockImplementation(() => ({
            promise: () => Promise.resolve(responsePayload)
        }));
        let tt = await from([{test: 'test1'}])
            .pipe(
                batchPut({
                    eventBridge: {
                        putEvents
                    },
                    detailType: 'test',
                    source: 'learningrecord.test',
                    eventBusName: 'eventbustest',
                    maxAttempts: 2
                })
            )
            .toPromise();
            expect(putEvents.mock.calls.length).toBe(2)
    });
    it('buffers', async () => {
        let responsePayload = {
            Entries: []
        }
        let putEvents = jest.fn().mockImplementation(() => ({
            promise: () => Promise.resolve(responsePayload)
        }));
        let tt = await from([
            {test: 'test1'},
            {test: 'test1'},
            {test: 'test1'}

        ])
            .pipe(
                batchPut({
                    eventBridge: {
                        putEvents
                    },
                    detailType: 'test',
                    source: 'learningrecord.test',
                    eventBusName: 'eventbustest',
                    maxAttempts: 1
                })
            )
            .toPromise();
            expect(putEvents.mock.calls[0][0].Entries.length).toBe(3);
    });
    it('it specifys detail type from config', async () => {
        let responsePayload = {
            Entries: []
        }
        let putEvents = jest.fn().mockImplementation(() => ({
            promise: () => Promise.resolve(responsePayload)
        }));
        let tt = await from([{test: 'test1'}])
            .pipe(
                batchPut({
                    eventBridge: {
                        putEvents
                    },
                    detailType: 'test',
                    source: 'learningrecord.test',
                    eventBusName: 'eventbustest',
                    maxAttempts: 1
                })
            )
            .toPromise();
            expect(putEvents.mock.calls[0][0].Entries).toContainEqual(
                expect.objectContaining(
                {
                    EventBusName: 'eventbustest'
                })
            );
    });
    it('uses the event bus name specified in the config', async () => {
        let responsePayload = {
            Entries: []
        }
        let putEvents = jest.fn().mockImplementation(() => ({
            promise: () => Promise.resolve(responsePayload)
        }));
        let tt = await from([{test: 'test1'}])
            .pipe(
                batchPut({
                    eventBridge: {
                        putEvents
                    },
                    detailType: 'test',
                    source: 'learningrecord.test',
                    eventBusName: 'eventbustest',
                    maxAttempts: 1
                })
            )
            .toPromise();
            expect(putEvents.mock.calls[0][0].Entries).toContainEqual(
                expect.objectContaining(
                {
                    EventBusName: 'eventbustest'
                })
            );
    });


    it('Calls putEvents on the supplied eventBridge.', async () => {
        let responsePayload = {
            Entries: []
        }
        let putEvents = jest.fn().mockImplementation(() => ({
            promise: () => Promise.resolve(responsePayload)
        }));
        let tt = await from([{test: 'test1'}])
            .pipe(
                batchPut({
                    eventBridge: {
                        putEvents
                    },
                    detailType: 'test',
                    source: 'learningrecord.test',
                    eventBusName: 'test',
                    maxAttempts: 1
                })
            )
            .toPromise();
        expect(putEvents.mock.calls.length).toBe(1);
    });

    it('JSON encodes the event detail and puts it on the event bridge', async () => {
        let responsePayload = {
            Entries: [{}]
        }
        let putEvents = jest.fn().mockImplementation(() => ({
            promise: () => Promise.resolve(responsePayload)
        }));
        let [event] = await from([{test: 'test1'}])
            .pipe(
                batchPut({
                    eventBridge: {
                        putEvents
                    },
                    detailType: 'test',
                    source: 'learningrecord.test',
                    eventBusName: 'test',
                    maxAttempts: 1
                }),
                first()
            )
            .toPromise();
        let result = JSON.parse(event.Detail);
        expect(result).toEqual({test: 'test1'});
    });

    it('sets the source and specified in the config', async () => {
        let responsePayload = {
            Entries: []
        }
        let putEvents = jest.fn().mockImplementation(() => ({
            promise: () => Promise.resolve(responsePayload)
        }));
        let tt = await from([{test: 'test1'}])
            .pipe(
                batchPut({
                    eventBridge: {
                        putEvents
                    },
                    detailType: 'test',
                    source: 'learningrecord.test',
                    eventBusName: 'test',
                    maxAttempts: 1
                })
            )
            .toPromise();
        expect(putEvents.mock.calls[0][0].Entries).toContainEqual(
            expect.objectContaining(
            {
                Source: 'learningrecord.test'
            })
        );
    });
})
