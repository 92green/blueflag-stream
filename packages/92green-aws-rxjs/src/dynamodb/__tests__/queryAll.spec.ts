import queryAll from '../queryAll';
import {toArray} from 'rxjs/operators';
import {mockClient} from "aws-sdk-client-mock";
import {marshall} from '@aws-sdk/util-dynamodb';
import {lastValueFrom} from 'rxjs';

import {DynamoDBClient} from '@aws-sdk/client-dynamodb';
import {DynamoDBDocumentClient, QueryCommand, QueryCommandOutput} from "@aws-sdk/lib-dynamodb";

describe('queryAll', () => {

    it('should return items from the QueryCommand', async () => {
        let responsePayload: QueryCommandOutput = {
            $metadata:{},
            Items: [
                //marshall({number: 1}),
                //marshall({number: 2}),
                //marshall({number: 3})
                {number: 1},
                {number: 2},
                {number: 3}
            ],
            Count: 3
        };
        let dynamoDBClient = new DynamoDBClient({});
        let docClient = DynamoDBDocumentClient.from(dynamoDBClient);
        mockClient(docClient).on(QueryCommand).resolves(responsePayload)
        let params = {
            TableName: 'fake-table'
        };


        let response = await lastValueFrom(
            queryAll(docClient, params)
                .pipe(toArray())
        );


        expect(response).toContainEqual(
            {number: 1}
        );
        expect(response).toContainEqual(
            {number: 2}
        );
        expect(response).toContainEqual(
            {number: 3}
        )
    });

    it('queryAll should re-request with ExclusiveStartKey if LastEvaluatedKey is present on response', async () => {
        let dynamoDBClient = new DynamoDBClient({});
        let params = {
            TableName: 'fake-table'
        };

        let item = {
            a: 'b'
        }
        let item2 = {
            a: 'c'
        }
        let fn = jest.fn()
            .mockImplementationOnce(() => Promise.resolve(
                {
                    $metadata: {},
                    Items: [
                        item
                    ],
                    LastEvaluatedKey: marshall({key: "abc"})
                } as QueryCommandOutput
            ))
            .mockImplementationOnce(() => Promise.resolve({$metadata: {},
                Items: [
                    item2
                ]
            } as QueryCommandOutput)
        )
        mockClient(dynamoDBClient).on(QueryCommand).callsFake(fn)


        let response = await lastValueFrom(
            queryAll(dynamoDBClient, params).pipe(toArray())
        )

        expect(response).toContainEqual(item);
        expect(response).toContainEqual(item2);
    });

    it('Should handle errors', async () => {
        expect.assertions(1);

        let dynamoDBClient = new DynamoDBClient({});
        let docClient = DynamoDBDocumentClient.from(dynamoDBClient);
        mockClient(docClient).rejects('!!!')
        await lastValueFrom(
             queryAll(
                    docClient,
                    // @ts-ignore - test with invalid params
                    {tableName: 'fake-table'}
                )
            )
            .catch((e: Error) => {
                expect(e.message).toBe('!!!');
            });
    });
});
