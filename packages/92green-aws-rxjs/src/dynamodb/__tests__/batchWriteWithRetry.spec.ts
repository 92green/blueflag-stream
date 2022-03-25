// @flow
import batchWriteWithRetry from '../batchWriteWithRetry';
import {from, lastValueFrom} from 'rxjs';
// import {tap} from 'rxjs/operators';
import {mockClient} from "aws-sdk-client-mock";
import {marshall} from '@aws-sdk/util-dynamodb';
import {BatchWriteItemCommand, BatchWriteItemCommandOutput, DynamoDBClient} from "@aws-sdk/client-dynamodb"; //BatchGetItemCommandOutput
import {toArray} from 'rxjs/operators';


describe('batchWriteWithRetry', () => {
    it('should retry unprocessed items', async () => {
        let dynamoDBClient = new DynamoDBClient({});
        let responsePayloads: Array<BatchWriteItemCommandOutput> = [
            {
                $metadata: {},
                UnprocessedItems: {
                    'fake-table': [
                        {
                            PutRequest: {
                                Item: marshall({
                                    foo: 300
                                })
                            }
                        }
                    ]
                }
            },
            {
                $metadata: {}
            }
        ];

        let fn = jest.fn()
            .mockImplementationOnce(() => Promise.resolve(responsePayloads[0]))
            .mockImplementationOnce(() => Promise.resolve(responsePayloads[1]))
        mockClient(dynamoDBClient).on(BatchWriteItemCommand).callsFake(fn)
        
        await lastValueFrom(from([{}])
            .pipe(
                batchWriteWithRetry({
                    dynamoDBClient,
                    tableName: 'fake-table'
                }),
            )
        )

        expect(fn).toBeCalledTimes(2);
    });

    it('should return items', async () => {

        let dynamoDBClient = new DynamoDBClient({});
        mockClient(dynamoDBClient).on(BatchWriteItemCommand).resolves(
            {}
        );

        let params = [
            {
                Item: {
                    foo: 100
                }  
            },
            {
                Item: {
                    foo: 200
                }
            },
            {
                Item: {
                    foo: 300
                }
            }
        ];

        let result = await lastValueFrom(
            from(params)
            .pipe(
                batchWriteWithRetry({
                    dynamoDBClient,
                    tableName: 'fake-table'
                }),
                toArray()
            )
        );

        expect(result).toContainEqual(params[0]);
        expect(result).toContainEqual(params[1]);
        expect(result).toContainEqual(params[2]);
    });

    it('Should handle errors', async () => {
        expect.assertions(1);

        let dynamoDBClient = new DynamoDBClient({});
        mockClient(dynamoDBClient).rejects('!!!')
        await lastValueFrom(
            from([123])
            .pipe(
                batchWriteWithRetry({
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
