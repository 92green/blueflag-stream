import {DynamoDBClient, QueryCommand, QueryCommandInput, AttributeValue} from '@aws-sdk/client-dynamodb';
import {
    QueryCommand as DocClientQueryCommand,
    QueryCommandInput as DocClientQueryCommandInput,
    DynamoDBDocumentClient
} from "@aws-sdk/lib-dynamodb";
import {unmarshall} from '@aws-sdk/util-dynamodb';
import {Observable} from 'rxjs';
import {from} from 'rxjs';
import {EMPTY} from 'rxjs';
import {
    map,
    concatMap,
    expand
} from 'rxjs/operators';

// For those that _really_ want to handle marshalling and unmarshalling themselves
export const docClientQueryAll = (docClient: DynamoDBDocumentClient, params: DocClientQueryCommandInput): Observable<Record<string, string>> => {

    return from(docClient.send(new DocClientQueryCommand(params))).pipe(
        expand((response) => {
            if(response.LastEvaluatedKey) {
                return from(docClient.send(new DocClientQueryCommand({
                    ...params,
                    ExclusiveStartKey: response.LastEvaluatedKey
                })));
            }
            return EMPTY;
        }),
        concatMap(response => response.Items ? response.Items : []),
    );
};



export const queryAll = (dynamoClient: DynamoDBClient, params: QueryCommandInput): Observable<{[key: string]: AttributeValue;}> => {

    return from(dynamoClient.send(new QueryCommand(params))).pipe(
        expand((response) => {
            if(response.LastEvaluatedKey) {
                return from(dynamoClient.send(new QueryCommand({
                    ...params,
                    ExclusiveStartKey: response.LastEvaluatedKey
                })));
            }
            return EMPTY;
        }),
        concatMap(response => response.Items ? response.Items : []),
        map(ii => unmarshall(ii)),
    );
};
