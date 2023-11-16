import {Observable, interval, zip, of, EMPTY} from "rxjs";
import {map, concatMap, filter, expand, scan, share, throttle, endWith} from 'rxjs/operators';
const DEFAULT_THROTTLE_MS = 500;
const MAX_TOTAL_MESSAGE_SIZE = 262144; // 256 * 1024 - https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-putevent-size.html
const DEFAULT_BATCH_SIZE = 10;

import type {EventBridgeClient, PutEventsRequestEntry, PutEventsResultEntry} from '@aws-sdk/client-eventbridge'
import {PutEventsCommand} from '@aws-sdk/client-eventbridge';

const END_SYMBOL = Symbol('END');

export class BatchPutError extends Error {
    code: string;
    data: unknown;

    constructor(message: string, code: string, data: unknown) {
        super(message);
        this.code = code;
        this.data = data;
    }
}

export type BufferSizeParams<T> = {
    bufferSize: number,
    sizeFn: (data: T) => number,
    maxBufferCount: number
};

export const bufferSize = <T>({bufferSize, sizeFn, maxBufferCount}: BufferSizeParams<T>) => (obs: Observable<T>): Observable<T[]> => {

    return obs.pipe(
        endWith(END_SYMBOL),
        scan((acc, value) => {
            if(value === END_SYMBOL) {
                acc.result = acc.buffer;
                acc.buffer = [];
                acc.emit = true;
                return acc;
            }

            const size = sizeFn(value);
            if(size > bufferSize) {
                throw new Error(`Message size ${size} is larger than buffer size ${bufferSize}`);
            }
            if (acc.size + size > bufferSize || acc.buffer.length >= maxBufferCount) {
                acc.result = acc.buffer;
                acc.buffer = [value];
                acc.size = size;
                acc.emit = true;
                return acc;
            }
            acc.buffer.push(value);
            acc.emit = false;
            acc.size += size;
            return acc;
        }, {buffer: [] as T[], emit: false, size: 0, result: [] as T[]}),
        filter(({emit}) => emit),
        map(({result}) => result)
    );
}


type Config = {
    eventBridgeClient: EventBridgeClient,
    retryOn?: string[],
    detailType: string,
    source: string,
    eventBusName: string,
    maxAttempts?: number,
    throttleMs?: number,
    maxMessageSize?: number,
    maxBatchCount?: number
};

const RETRY_ON = [
    "ThrottlingException",
    "InternalFailure"
];

type MetaData = {
    attempts: number
    result?: PutEventsResultEntry
}

type RecordTuple= [PutEventsRequestEntry, MetaData]

export default <T>(config: Config) => (obs: Observable<T>) => {
    const maxMessageSize = config.maxMessageSize || MAX_TOTAL_MESSAGE_SIZE;
    const maxBatchCount = config.maxBatchCount || DEFAULT_BATCH_SIZE;
    const maxAttempts = config.maxAttempts || 0;

    const retryOn = config.retryOn || RETRY_ON;
    const sendToEventBus = (obs: Observable<RecordTuple>): Observable<RecordTuple> => {
        let input  = obs.pipe(share());
        let results = obs.pipe(
            map((tuple) => tuple[0]),
            bufferSize({
                sizeFn: (data: PutEventsRequestEntry) => JSON.stringify(data).length,
                bufferSize: maxMessageSize,
                maxBufferCount: maxBatchCount
            }),
            concatMap(records => config
                .eventBridgeClient
                .send(new PutEventsCommand({Entries: records})
            )),
            concatMap(ii => ii.Entries || EMPTY)
        );
        return zip(
            input,
            results
        ).pipe(
            map(([input, result]) => {
                let [record, info] = input;
                return [record, {
                    ...info,
                    result,
                    attempts: ++info.attempts
                }];
            })
        );
    };
    return obs.pipe(
        map((obj: T) => {
            return [{
                Detail: JSON.stringify(obj),
                DetailType: config.detailType,
                EventBusName: config.eventBusName,
                Source: config.source,
                Time: new Date()
            }, {attempts: 0}] as RecordTuple;
        }),
        sendToEventBus,
        expand(ii => of(ii)
            .pipe(
            filter(([item, info]) => {
                    // Event already pushed to EventBus, Don't retry;
                    if(info.result?.EventId) {
                        return false
                    }

                    const errorCode =  info.result?.ErrorCode || 'UNKNOWN';

                    if(
                        info.attempts < maxAttempts
                        && retryOn.includes(errorCode)
                    ) {
                        return true;
                    }


                    throw new BatchPutError(`${errorCode} Error sending record to eventbridge`, errorCode, item);
                }),
                throttle(() => interval(config.throttleMs || DEFAULT_THROTTLE_MS)),
                sendToEventBus
            )
        )

    );
};
