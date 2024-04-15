import batchWriteWithRetry from '../batchWriteWithRetry';
import {from, lastValueFrom} from 'rxjs';
import {mockClient} from "aws-sdk-client-mock";
import {marshall} from '@aws-sdk/util-dynamodb';
import {DynamoDBClient} from "@aws-sdk/client-dynamodb"; //BatchGetItemCommandOutput
import {DynamoDBDocumentClient, BatchWriteCommand, BatchWriteCommandOutput} from '@aws-sdk/lib-dynamodb';
import {toArray} from 'rxjs/operators';


describe('batchWriteWithRetry', () => {
    it('should retry unprocessed items', async () => {
        let dynamoDBClient = new DynamoDBClient({});
        const docClient = DynamoDBDocumentClient.from(dynamoDBClient);
        let responsePayloads: Array<BatchWriteCommandOutput> = [
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
        mockClient(docClient).on(BatchWriteCommand).callsFake(fn)

        await lastValueFrom(from([{}])
            .pipe(
                batchWriteWithRetry({
                    docClient,
                    tableName: 'fake-table'
                }),
            )
        )

        expect(fn).toBeCalledTimes(2);
    });

    it('should return items', async () => {

        let dynamoDBClient = new DynamoDBClient({});
        const docClient = DynamoDBDocumentClient.from(dynamoDBClient);
        mockClient(docClient).on(BatchWriteCommand).resolves(
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
                    docClient,
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
        const docClient = DynamoDBDocumentClient.from(dynamoDBClient);
        mockClient(docClient).on(BatchWriteCommand).rejects('!!!')

        await lastValueFrom(
            from([{Item: { foo: 100}}])
            .pipe(
                batchWriteWithRetry({
                    docClient,
                    tableName: 'fake-table'
                })
            )
        )
        .catch((e: Error) => {
            expect(e.message).toBe('!!!');
        });
    });

});
