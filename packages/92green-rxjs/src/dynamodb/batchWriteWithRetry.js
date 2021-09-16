//@flow
import {Observable} from 'rxjs';
import {EMPTY} from 'rxjs';
import {pipe} from 'rxjs';
import {of} from 'rxjs';
import {from} from 'rxjs';
import {expand} from 'rxjs/operators';
import {bufferCount} from 'rxjs/operators';
import {flatMap} from 'rxjs/operators';
import {concatMap} from 'rxjs/operators';

type QueryResult = {
    promise: () => Promise<any>
};

type Query = (params: any) => QueryResult;

type DocClient = {
    batchWrite: Query
};

type Config = {
    docClient: DocClient,
    tableName: string,
    returnItems?: boolean
};

type Response = {
    Items: any[],
    UnprocessedItems: {[key: string]: any}
};

type FeedbackPipe = (obs: Observable) => Observable;

const MAX_BATCH_WRITE = 25;

export default (config: Config, feedbackPipe: FeedbackPipe = obs => obs) => {

    let sendQuery = (params: any): Observable => {
        return new Observable((subscriber: any) => {
            config
                .docClient
                .batchWrite(params)
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
            let {UnprocessedItems} = response;
            if(UnprocessedItems && Object.keys(UnprocessedItems).length > 0) {
                return of(response).pipe(
                    feedbackPipe,
                    flatMap(() => sendQuery({
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

            if(!config.returnItems) {
                return sendObs;
            }

            return sendObs.pipe(
                flatMap(() => from(itemArray))
            );
        })
    );
};
