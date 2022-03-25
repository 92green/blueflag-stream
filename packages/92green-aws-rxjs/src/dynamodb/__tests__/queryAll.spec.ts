import queryAll from '../queryAll';
import {toArray} from 'rxjs/operators';
import {mockClient} from "aws-sdk-client-mock";
import {marshall} from '@aws-sdk/util-dynamodb';
import {lastValueFrom} from 'rxjs';

import {QueryCommand, DynamoDBClient, QueryCommandOutput} from '@aws-sdk/client-dynamodb';

describe('queryAll', () => {

    it('should return items from the QueryCommand', async () => {
        let responsePayload: QueryCommandOutput = {
            $metadata:{},
            Items: [
                marshall({number: 1}), 
                marshall({number: 2}), 
                marshall({number: 3})
            ],
            Count: 3
        };
        let dynamoDBClient = new DynamoDBClient({});
        mockClient(dynamoDBClient).on(QueryCommand).resolves(responsePayload)
        let params = {
            TableName: 'fake-table'
        };


        let response = await lastValueFrom(
            queryAll(dynamoDBClient, params)
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
                        marshall(item)
                    ],
                    LastEvaluatedKey: marshall({key: "abc"})
                } as QueryCommandOutput
            ))
            .mockImplementationOnce(() => Promise.resolve({$metadata: {},
                Items: [
                    marshall(item2)
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

    it('should accept feedback pipe that gets used in each feedback loop', async () => {
        let dynamoDBClient = new DynamoDBClient({});
        let params = {
            TableName: 'fake-table'
        };

        let responsePayloads = [
            {
                Items: [100, 200, 300],
                LastEvaluatedKey: 'foo'
            },
            {
                Items: [400, 500, 600],
                LastEvaluatedKey: 'bar'
            },
            {
                Items: [700, 800, 900]
            }
        ];

        let fn = jest.fn()
                .mockImplementationOnce(() => Promise.resolve(responsePayloads[0]))
                .mockImplementationOnce(() => Promise.resolve(responsePayloads[1]))
                .mockImplementationOnce(() => Promise.resolve(responsePayloads[2]))
        mockClient(dynamoDBClient).on(QueryCommand).callsFake(fn)

        let feedbackPipeFn = jest.fn(obs => obs);

        await lastValueFrom(
            queryAll(dynamoDBClient, params, feedbackPipeFn)
                .pipe(toArray())
            )

        expect(feedbackPipeFn).toHaveBeenCalledTimes(2);

    });

    it('Should handle errors', async () => {
        expect.assertions(1);

        let dynamoDBClient = new DynamoDBClient({});
        mockClient(dynamoDBClient).rejects('!!!')
        await lastValueFrom(
             queryAll(
                    dynamoDBClient,
                    {tableName: 'fake-table'}
                )
            )
            .catch((e: Error) => {
                expect(e.message).toBe('!!!');
            });
    });
});
