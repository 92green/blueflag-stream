import {Observable, UnaryFunction} from 'rxjs';
import {EMPTY} from 'rxjs';
import {pipe} from 'rxjs';
import {from} from 'rxjs';
import {
    expand,
    mergeMap,
    bufferCount,
    last
} from 'rxjs/operators';
import {DynamoDBDocumentClient, BatchWriteCommand, BatchWriteCommandOutput} from '@aws-sdk/lib-dynamodb';

type Config = {
    docClient: DynamoDBDocumentClient,
    tableName: string
};

const MAX_BATCH_WRITE = 25;

export default function <T>(config: Config): UnaryFunction<Observable<T>, Observable<T>> {
    const {docClient, tableName} = config;

    return pipe(
        bufferCount(MAX_BATCH_WRITE),
        mergeMap((items) => {
            return from(docClient.send(new BatchWriteCommand({
                RequestItems: {
                    [tableName]: items
                }
            }))).pipe(
                expand((response: BatchWriteCommandOutput) => {
                    if(response.UnprocessedItems && response.UnprocessedItems[tableName]) {
                        const command = new BatchWriteCommand({
                            RequestItems: {
                                [tableName]: response.UnprocessedItems[tableName]
                            }
                        });
                        return from(docClient.send(command));
                    }
                    return EMPTY;
                }),
                // Wait for the observable to finish and then emit the incoming items back out
                last(),
                mergeMap(() => items)
            );
        })
    );
}
