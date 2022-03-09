import batchGetWithRetry from '../batchGetWithRetry';
import {from, lastValueFrom} from 'rxjs';
import {toArray} from 'rxjs/operators'; //tap
import {mockClient} from "aws-sdk-client-mock";
import {BatchGetItemCommand, DynamoDBClient} from "@aws-sdk/client-dynamodb"; //BatchGetItemCommandOutput
import {marshall} from '@aws-sdk/util-dynamodb';

describe('batchGetWithRetry', () => {    
    it('returns items from the batch get.', async () => {
        const dynamoDBClient = new DynamoDBClient({});
        // let dynamoDBClientMock = 
        mockClient(dynamoDBClient).on(BatchGetItemCommand).resolves({
            UnprocessedKeys: {},
            Responses: {
                ['fake-table']:
                [marshall({'test1': 'test2'})]
            }
        });
        let params = [{}];
        let response = await lastValueFrom(
            from(params)
                .pipe(
                    batchGetWithRetry({
                        dynamoDBClient,
                        tableName: 'fake-table'
                    }),
                    toArray()
                )
            )
        expect(response).toContainEqual({'test1': 'test2'});
    });

    it('returns as many items that a returned from dynamodb.', async () => {
        const dynamoDBClient = new DynamoDBClient({});
        // let dynamoDBClientMock = 
        mockClient(dynamoDBClient).on(BatchGetItemCommand).resolves({
            UnprocessedKeys: {},
            Responses: {
                ['fake-table']:
                [marshall({'test1': 'test2'}), marshall({'test1': 'test2'}), marshall({'test1': 'test2'})]
            }
        });

        let params = [{}];
        let response = await lastValueFrom(
            from(params)
                .pipe(
                    batchGetWithRetry({
                        dynamoDBClient,
                        tableName: 'fake-table'
                    }),
                    toArray()
                )
            ) 
        expect(response.length).toBe(3);
    });

    it('should retry unprocessed items', async () => {
        let result1 = {
            a: 'b'
        }

        let result2 = {
            b: 'c'
        }
        const dynamoDBClient = new DynamoDBClient({});
        mockClient(dynamoDBClient).callsFake(
            jest.fn()
                .mockImplementationOnce(() => Promise.resolve({
                    UnprocessedKeys: {
                        ['fake-table']: 
                            {
                                Keys: [
                                    marshall({"Key": "b"})
                                ]
                            }
                        
                    },
                    Responses: {
                        ['fake-table']:  [
                            marshall(result1)
                        ]
                    }
                }))
                .mockImplementationOnce(() => Promise.resolve({UnprocessedKeys: {},
                        Responses: {
                            ['fake-table']:  [
                                marshall(result2)
                            ]
                        }})
                ))
        let params = [{Key: "a"}, {Key: "b"}]
        let response = await lastValueFrom(
            from(params)
                .pipe(
                    batchGetWithRetry({
                        dynamoDBClient,
                        tableName: 'fake-table'
                    }),
                    toArray()
                )
            ) 
        expect(response).toContainEqual(result1);
        expect(response).toContainEqual(result2);
    });


    it('batchGetWithRetry should handle errors', async () => {
        expect.assertions(1);
        const dynamoDBClient = new DynamoDBClient({});
        mockClient(dynamoDBClient).rejects('!!!')

        await lastValueFrom(from([{id: 'abc'}])
            .pipe(
                batchGetWithRetry({
                    dynamoDBClient,
                    tableName: 'fake-table'
                })
            )
        )
        .catch((e: Error) => {
            expect(e.message).toBe('!!!');
        });
    });
});
