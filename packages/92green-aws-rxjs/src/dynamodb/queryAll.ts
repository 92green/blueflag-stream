import {DynamoDBClient, QueryCommand, QueryCommandInput, QueryCommandOutput, AttributeValue} from '@aws-sdk/client-dynamodb';
import { unmarshall } from '@aws-sdk/util-dynamodb';
import {Observable} from 'rxjs';
import {of} from 'rxjs';
import {EMPTY} from 'rxjs';
import {map, mergeMap} from 'rxjs/operators';
import {concatMap} from 'rxjs/operators';
import {expand} from 'rxjs/operators';
import {takeWhile} from 'rxjs/operators';

type FeedbackPipe = (obs: Observable<QueryCommandOutput>) => Observable<QueryCommandOutput>;

export default (dynamoClient: DynamoDBClient, params: any, feedbackPipe: FeedbackPipe = obs => obs): Observable<{[key: string]: AttributeValue;}> => {
    
    let sendQuery = (params: QueryCommandInput): Observable<QueryCommandOutput> => {
        return new Observable((subscriber: any) => {
            dynamoClient
                .send(new QueryCommand(params))
                .then((response) => {
                    subscriber.next(response);
                    subscriber.complete();
                }, (err: any) => {
                    subscriber.error(err);
                });
        });
    };

    return sendQuery(params).pipe(
        expand((response) => {
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
        takeWhile(response => Boolean(response.LastEvaluatedKey), true),
        concatMap(response => response.Items ? response.Items : []),
        map(ii => unmarshall(ii))
    );
};
