//@flow
// Deprecated use 92green-aws-rxjs

import {Observable} from 'rxjs';
import {EMPTY} from 'rxjs';
import {pipe} from 'rxjs';
import {of} from 'rxjs';
import {from} from 'rxjs';
import {expand} from 'rxjs/operators';
import {bufferCount} from 'rxjs/operators';
import {flatMap} from 'rxjs/operators';

type QueryResult = {
    promise: () => Promise<any>
};

type Query = (params: any) => QueryResult;

type DocClient = {
    batchGet: Query
};

type Config = {
    docClient: DocClient,
    tableName: string,
    returnItems?: boolean
};

type Response = {
    Items: any[],
    UnprocessedKeys: {[key: string]: any}
};

type FeedbackPipe = (obs: Observable) => Observable;

const MAX_BATCH_READ = 100;


export default (config: Config, feedbackPipe: FeedbackPipe = obs => obs) => {

    let sendQuery = (params: any): Observable => {
        return new Observable((subscriber: any) => {
            config
                .docClient
                .batchGet(params)
                .promise()
                .then((response: Response) => {
                    subscriber.next(response);
                    subscriber.complete();
                }, (err: any) => {
                    subscriber.error(err);
                });
        });
    };

    let sendQueryWithRetry = (params: any) => sendQuery(params).pipe(
        expand((response: Response) => {
            let {UnprocessedKeys} = response;
            if(Object.keys(UnprocessedKeys).length > 0) {
                return of(response).pipe(
                    feedbackPipe,
                    flatMap(() => sendQuery({
                        RequestItems: UnprocessedKeys
                    }))
                );
            }
            return EMPTY;
        })
    );

    return pipe(
        bufferCount(MAX_BATCH_READ),
        flatMap((keyArray) => {
            return sendQueryWithRetry({
                RequestItems: {
                    [config.tableName]: {
                        Keys: keyArray
                    }
                }
            });
        }),
        flatMap(response => {
            if(!config.returnItems) {
                return of(response);
            }
            return from(response.Responses[config.tableName]);
        })
    );
};
