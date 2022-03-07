import {Observable, UnaryFunction} from 'rxjs';
import {EMPTY} from 'rxjs';
import {pipe} from 'rxjs';
import {of} from 'rxjs';
import {from} from 'rxjs';
import {expand, mergeMap} from 'rxjs/operators';
import {bufferCount} from 'rxjs/operators';
import {concatMap} from 'rxjs/operators';
import {BatchWriteItemCommand, BatchWriteItemCommandInput, BatchWriteItemCommandOutput} from "@aws-sdk/client-dynamodb";
import type {AttributeValue, BatchGetItemCommandOutput, DynamoDBClient} from "@aws-sdk/client-dynamodb"

type Config = {
    dynamoDBClient: DynamoDBClient,
    tableName: string,
    returnItems?: boolean
};

type FeedbackPipe<T> = (obs: Observable<T>) => Observable<T>;

const MAX_BATCH_WRITE = 25;

export default function (config: Config, feedbackPipe: FeedbackPipe<BatchGetItemCommandOutput> = obs => obs): UnaryFunction<Observable<{[key: string]: AttributeValue;}>, Observable<BatchWriteItemCommandOutput | {[key: string]: AttributeValue;}>> {
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
        bufferCount(MAX_BATCH_WRITE),
        concatMap((itemArray) => {
            let sendObs = sendQueryWithRetry({
                RequestItems: {
                    [config.tableName]: itemArray
                }
            });

            if (!config.returnItems) {
                return sendObs;
            }

            return sendObs.pipe(
                mergeMap(() => from(itemArray))
            );
        })
    );
};
