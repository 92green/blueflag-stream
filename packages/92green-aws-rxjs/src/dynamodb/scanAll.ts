import {Observable} from 'rxjs';
import {of} from 'rxjs';
import {EMPTY} from 'rxjs';
import {concatMap, map} from 'rxjs/operators';
import {mergeMap} from 'rxjs/operators';
import {expand} from 'rxjs/operators';
import {ScanCommand, ScanCommandInput} from "@aws-sdk/client-dynamodb";
import type {AttributeValue, ScanCommandOutput, DynamoDBClient} from "@aws-sdk/client-dynamodb"
import { unmarshall } from '@aws-sdk/util-dynamodb';

type FeedbackPipe<T> = (obs: Observable<T>) => Observable<T>;

export default (dynamoDBClient: DynamoDBClient, params: any, feedbackPipe: FeedbackPipe<ScanCommandOutput> = obs => obs): Observable<{[key: string]: AttributeValue;}> => {

    let sendQuery = (params: ScanCommandInput): Observable<ScanCommandOutput> => {
        return new Observable((subscriber: any) => {
            dynamoDBClient
                .send(new ScanCommand(params))
                .then((response: ScanCommandOutput) => {
                    subscriber.next(response);
                    subscriber.complete();
                }, (err: any) => {
                    subscriber.error(err);
                });
        });
    };

    return sendQuery(params).pipe(
        expand((response: ScanCommandOutput) => {
            let {LastEvaluatedKey} = response;
            if(LastEvaluatedKey) {
                return of(response).pipe(
                    feedbackPipe,
                    mergeMap(() => sendQuery({
                        ...params,
                        ExclusiveStartKey: LastEvaluatedKey
                    }))
                );
            }
            return EMPTY;
        }),
        concatMap(response => response.Items ? response.Items : []),
        map(ii => unmarshall(ii))
    );
};
