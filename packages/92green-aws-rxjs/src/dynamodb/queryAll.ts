import {
    DynamoDBClient,
    QueryCommand as DyanmoClientQueryCommand,
    QueryCommandInput as DynamoClientQueryCommandInput,
    AttributeValue
} from '@aws-sdk/client-dynamodb';
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

/**
* Query all using the DynamoDBDocumentClient
* By using the document client all marshalling and unmarshalling is handled for you
* it is worth noting that this is so sufficently simple that it probably makes sense to just write the rxjs logic directly into your workflow instead of using this function
**/
queryAll = (docClient: DynamoDBDocumentClient, params: DocClientQueryCommandInput): Observable<Record<string, string>> => {
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

/**
* Query all using the DynamoDBClient
* Unlike the default queryAll this takes and returns unmarshalled data and loosely aligns with the previous queryAll function from 92green-aws-rxjs
* Alike the default version it this is sufficently simple that it probably makes sense to just write the rxjs logic directly into your workflow instead of using this function
**/
const queryAllDynamoDBClient = (dynamoClient: DynamoDBClient, params: DynamoClientQueryCommandInput): Observable<{[key: string]: AttributeValue;}> => {
    return from(dynamoClient.send(new QueryCommand(params))).pipe(
        expand((response) => {
            if(response.LastEvaluatedKey) {
                return from(dynamoClient.send(new DynamoClientQueryCommand({
                    ...params,
                    ExclusiveStartKey: response.LastEvaluatedKey
                })));
            }
            return EMPTY;
        }),
        concatMap(response => response.Items ? response.Items : [])
    );
};
