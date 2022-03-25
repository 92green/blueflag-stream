//@flow

// Deprecated use 92green-aws-rxjs
import {Observable} from 'rxjs';
import {of} from 'rxjs';
import {EMPTY} from 'rxjs';
import {concatMap} from 'rxjs/operators';
import {flatMap} from 'rxjs/operators';
import {expand} from 'rxjs/operators';
import {takeWhile} from 'rxjs/operators';

type QueryResult = {
    promise: () => Promise<any>
};

type Query = (params: any) => QueryResult;

type DocClient = {
    query: Query
};

type Response = {
    Items: any[],
    LastEvaluatedKey?: string
};

type FeedbackPipe = (obs: Observable) => Observable;

export default (docClient: DocClient, params: any, feedbackPipe: FeedbackPipe = obs => obs): Observable => {

    let sendQuery = (params: any): Observable => {
        return new Observable((subscriber: any) => {
            docClient
                .query(params)
                .promise()
                .then((response: Response) => {
                    subscriber.next(response);
                    subscriber.complete();
                }, (err: any) => {
                    subscriber.error(err);
                });
        });
    };

    return sendQuery(params).pipe(
        expand((response: Response) => {
            let {LastEvaluatedKey} = response;
            if(LastEvaluatedKey) {
                return of(response).pipe(
                    feedbackPipe,
                    flatMap(() => sendQuery({
                        ...params,
                        ExclusiveStartKey: LastEvaluatedKey
                    }))
                );
            }
            return EMPTY;
        }),
        takeWhile(response => response.LastEvaluatedKey, true),
        concatMap(response => response.Items)
    );
};
