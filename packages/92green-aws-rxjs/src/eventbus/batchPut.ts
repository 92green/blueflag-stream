// @flow
import {Observable, interval, zip, of, EMPTY} from "rxjs";
import {map, filter, expand, bufferCount, concatMap,share, throttle} from 'rxjs/operators';
const MAX_EVENT_BRIDGE_PUT = 10;
const DEFAULT_THROTTLE_MS = 500;

import type {EventBridgeClient, PutEventsRequestEntry, PutEventsResultEntry} from '@aws-sdk/client-eventbridge'
import {PutEventsCommand} from '@aws-sdk/client-eventbridge';

type Config = {
    eventBridgeClient: EventBridgeClient,
    retryOn?: string[],
    detailType: string,
    source: string,
    eventBusName: string,
    maxAttempts: number,
    throttleMs: number
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

export default <T extends object>(config: Config) => (obs: Observable<T>) => {
    const retryOn = config.retryOn || RETRY_ON;
    const sendToEventBus = (obs: Observable<RecordTuple>): Observable<RecordTuple> => {
        let input  = obs.pipe(share());
        let results = obs.pipe(
            map(([record]) => record),
            bufferCount(MAX_EVENT_BRIDGE_PUT),
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
            filter(([, info]) => {
                    // Bail no result..
                    if(!info.result){
                        return false;
                    }
                    // Bail can't check error code;
                    if(!info.result.ErrorCode){
                        return false
                    }
                    // Event already pushed to EventBus, Don't retry;
                    if(info.result.EventId)
                        return false

                    return retryOn.includes(info.result.ErrorCode) &&
                        info.attempts < config.maxAttempts;
                }),
                throttle(() => interval(config.throttleMs || DEFAULT_THROTTLE_MS)),
                sendToEventBus
            )
        )

    );
};