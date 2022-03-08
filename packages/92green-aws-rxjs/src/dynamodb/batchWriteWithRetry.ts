import {Observable, UnaryFunction} from 'rxjs';
import {EMPTY} from 'rxjs';
import {pipe} from 'rxjs';
import {of} from 'rxjs';
import {from} from 'rxjs';
import {expand, map, mergeMap} from 'rxjs/operators';
import {bufferCount} from 'rxjs/operators';
import {concatMap} from 'rxjs/operators';
import {BatchWriteItemCommand, BatchWriteItemCommandInput, BatchWriteItemCommandOutput} from "@aws-sdk/client-dynamodb";
import type {BatchGetItemCommandOutput, DynamoDBClient} from "@aws-sdk/client-dynamodb"
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';

type Config = {
    dynamoDBClient: DynamoDBClient,
    tableName: string,
    returnItems?: boolean
};

type FeedbackPipe<T> = (obs: Observable<T>) => Observable<T>;

const MAX_BATCH_WRITE = 25;

export default function <Value>(config: Config, feedbackPipe: FeedbackPipe<BatchGetItemCommandOutput> = obs => obs): UnaryFunction<Observable<Value>, Observable<Value>> {
    let sendQuery = (params: BatchWriteItemCommandInput): Observable<BatchWriteItemCommandOutput> => {
        return new Observable((subscriber: any) => {
            config
                .dynamoDBClient
                .send(new BatchWriteItemCommand(params))
                .then((response) => {
                    subscriber.next(response);
                    subscriber.complete();
                }, (err: any) => {
                    subscriber.error(err);
                });
        });
    };

    let sendQueryWithRetry = (params: any) => sendQuery(params).pipe(
        expand((response: BatchWriteItemCommandOutput) => {
            let { UnprocessedItems } = response;
            if (UnprocessedItems && Object.keys(UnprocessedItems).length > 0) {
                return of(response).pipe(
                    feedbackPipe,
                    mergeMap(() => sendQuery({
                        RequestItems: UnprocessedItems
                    }))
                );
            }
            return EMPTY;
        })
    );

    return pipe(
        map(ii => marshall(ii)),
        bufferCount(MAX_BATCH_WRITE),
        concatMap((itemArray) => {
            let sendObs = sendQueryWithRetry({
                RequestItems: {
                    [config.tableName]: itemArray
                }
            });

            return sendObs.pipe(
                mergeMap(() => from(itemArray)),
                map(ii => unmarshall(ii) as Value)
            );
        })
    );
};
