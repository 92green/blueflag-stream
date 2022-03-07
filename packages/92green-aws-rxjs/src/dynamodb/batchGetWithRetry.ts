import {Observable, UnaryFunction} from 'rxjs';
import {EMPTY} from 'rxjs';
import {pipe} from 'rxjs';
import {of} from 'rxjs';
import {from} from 'rxjs';
import {expand} from 'rxjs/operators';
import {bufferCount} from 'rxjs/operators';
import {mergeMap} from 'rxjs/operators';
import {map} from 'rxjs/operators';
import {BatchGetItemCommand, BatchGetItemCommandInput} from "@aws-sdk/client-dynamodb";
import type {AttributeValue, DynamoDBClient, BatchGetItemCommandOutput} from "@aws-sdk/client-dynamodb"
import {marshall, unmarshall} from '@aws-sdk/util-dynamodb';
type Config = {
    dynamoDBClient: DynamoDBClient,
    tableName: string
};

type FeedbackPipe<T> = (obs: Observable<T>) => Observable<T>;

const MAX_BATCH_READ = 100;

export default <Key, Value>(config: Config, feedbackPipe: FeedbackPipe<BatchGetItemCommandOutput> = obs => obs) : UnaryFunction<Observable<Key>, Observable<Value>> => {
    let sendQuery = (params: BatchGetItemCommandInput): Observable<BatchGetItemCommandOutput> => {
        return new Observable((subscriber: any) => {
            config
                .dynamoDBClient
                .send(new BatchGetItemCommand(params))
                .then((response) => {
                    subscriber.next(response);
                    subscriber.complete();
                }, (err: any) => {
                    subscriber.error(err);
                });
        });
    };

    let sendQueryWithRetry = (params: Array<Key>) => sendQuery({
        RequestItems: {
            [config.tableName]: {
                Keys: params.map(key => marshall(key))
            }
        }
    }).pipe(
        expand((response: BatchGetItemCommandOutput) => {
            let {UnprocessedKeys} = response;
            if(UnprocessedKeys && Object.keys(UnprocessedKeys).length > 0){
                return of(response).pipe(
                    feedbackPipe,
                    mergeMap(() => sendQuery({
                        RequestItems: UnprocessedKeys
                    }))
                );
            } else {
                return EMPTY;
            }
        })
    );

    return pipe(
        bufferCount(MAX_BATCH_READ),
        mergeMap((keyArray) => {
            return sendQueryWithRetry(keyArray);
        }),
        mergeMap(response => {
            return response.Responses ? from(response.Responses[config.tableName]) : EMPTY;
        }),
        map((ii:{[key: string]: AttributeValue;}) => unmarshall(ii) as Value)
    );
};
