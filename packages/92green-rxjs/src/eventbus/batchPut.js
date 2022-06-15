// @flow
// Deprecated use 92green-aws-rxjs
import {Observable, interval, zip, of} from "rxjs";
import {map, filter, expand, endWith, scan, concatMap,share, throttle} from 'rxjs/operators';
const MAX_EVENT_BRIDGE_PUT = 10;
const MAX_SIZE = 256000;
const DEFAULT_THROTTLE_MS = 500;
type Config = {
    eventBridge: any,
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

let END = Symbol('THE_END');

const getSize = (incoming) => Buffer.byteLength(Buffer.from(JSON.stringify(incoming)));

export default (config: Config) => (obs: Observable) => {
    const sendToEventBus = (obs) => {
        let input  = obs.pipe(share());
        let results = obs.pipe(
            endWith([END]),
            map(([record]) => record),
            scan((recordBuffer, incoming) => {
                if(incoming === END) {
                    return {...recordBuffer, remainder: END};
                }
                let {remainder, records, size} = recordBuffer;
                // TODO: switch size calculation to https://github.com/miktam/sizeof/blob/master/index.js
                if(recordBuffer.remainder) {
                    const sizeOfRemainder = getSize(remainder);
                    if(sizeOfRemainder > MAX_SIZE) {
                        throw new Error('Incoming event is too big for event bridge');
                    }
                    records = [remainder];
                    size = sizeOfRemainder;
                }

                const incomingSize = getSize(incoming);
                if(incomingSize + size > MAX_SIZE || records.length >= MAX_EVENT_BRIDGE_PUT) {
                    return {records, size, remainder: incoming};
                }

                return {
                    records: [...records, incoming],
                    size: size + incomingSize,
                    remainder: undefined
                };
            }, {records: [], remainder: undefined, size: 0}),
            filter(({remainder}) => !!remainder),
            //bufferCount(MAX_EVENT_BRIDGE_PUT),
            concatMap(({records}) => config.eventBridge.putEvents({Entries: records}).promise()),
            concatMap(ii => ii.Entries)
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
        map((obj: Object) => {
            return [{
                Detail: JSON.stringify(obj),
                DetailType: config.detailType,
                EventBusName: config.eventBusName,
                Source: config.source,
                Time: new Date()
            }, {attempts: 0}];
        }),
        sendToEventBus,
        expand(ii => of(ii)
            .pipe(
                filter(([, info]) => {
                    return !info.result.EventId &&
                        RETRY_ON.includes(info.result.ErrorCode) &&
                        info.attempts < config.maxAttempts;
                }),
                throttle(() => interval(config.throttleMs || DEFAULT_THROTTLE_MS)),
                sendToEventBus
            )
        )

    );
};
