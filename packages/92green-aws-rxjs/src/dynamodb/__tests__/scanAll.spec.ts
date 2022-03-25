
import scanAll from '../scanAll';
import {lastValueFrom} from 'rxjs'
import {toArray} from 'rxjs/operators';
import {DynamoDBClient, ScanCommandOutput, ScanCommand} from '@aws-sdk/client-dynamodb';
import {marshall} from '@aws-sdk/util-dynamodb';
import {mockClient} from "aws-sdk-client-mock";


describe('scanAll', () => {

    it('should return items from the ScanCommand', async () => {
        let items = [
            {number: 1},
            {number: 2},
            {number: 3}
        ]
        let responsePayload: ScanCommandOutput = {
            $metadata:{},
            Items: items.map(ii => marshall(ii)),
            Count: 3
        };

        let dynamoDBClient = new DynamoDBClient({});
        mockClient(dynamoDBClient).on(ScanCommand).resolves(responsePayload)
        let params = {
            TableName: 'fake-table'
        };

        let result = await lastValueFrom(
            scanAll(dynamoDBClient, params)
                .pipe(toArray())
            );
        expect(result).toContainEqual(items[0]);
        expect(result).toContainEqual(items[1]);
        expect(result).toContainEqual(items[2]);

    });

    it('scanAll should re-request with ExclusiveStartKey if LastEvaluatedKey is present on response', async () => {

        let params = {
            TableName: 'fake-table'
        };

        const items1 = [
            {number: 1}
        ]

        const items2 = [
            {number: 2}
        ]
        let ScanResponse1: ScanCommandOutput = {
            $metadata: {},
            Items: items1.map(ii => marshall(ii)),
            LastEvaluatedKey: {}
        }

        let ScanResponse2: ScanCommandOutput = {
            $metadata: {},
            Items: items2.map(ii => marshall(ii))
        }
        let dynamoDBClient = new DynamoDBClient({});

        let fn = jest.fn()
            .mockImplementationOnce(() => Promise.resolve(ScanResponse1))
            .mockImplementationOnce(() => Promise.resolve(ScanResponse2))
        mockClient(dynamoDBClient).on(ScanCommand).callsFake(fn)

        let result = await lastValueFrom(
            scanAll(dynamoDBClient, params)
                .pipe(toArray())
        )
        expect(result).toContainEqual(items1[0])
        expect(result).toContainEqual(items2[0])
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
        mockClient(dynamoDBClient).on(ScanCommand).callsFake(fn)

        let feedbackPipeFn = jest.fn(obs => obs);

        await lastValueFrom(
            scanAll(dynamoDBClient, params, feedbackPipeFn)
                .pipe(toArray())
            )

        expect(feedbackPipeFn).toHaveBeenCalledTimes(2);

    });

    it('Should handle errors', async () => {
        expect.assertions(1);

        let dynamoDBClient = new DynamoDBClient({});
        mockClient(dynamoDBClient).rejects('!!!')
        await lastValueFrom(
             scanAll(
                    dynamoDBClient,
                    {tableName: 'fake-table'}
                )
            )
            .catch((e: Error) => {
                expect(e.message).toBe('!!!');
            });
    });

});
