// @flow
import batchWriteWithRetry from '../batchWriteWithRetry';
import {from} from 'rxjs';
import {tap} from 'rxjs/operators';

describe('batchWriteWithRetry', () => {

    it('batchWriteWithRetry should pass params and make request', async () => {

        let responsePayload = {
            UnprocessedItems: {}
        };

        let docClient = {
            batchWrite: jest.fn()
                .mockImplementation(() => ({
                    promise: () => Promise.resolve(responsePayload)
                }))
        };

        let tapFn = jest.fn();

        let params = [
            {
                PutRequest: {
                    Item: {
                        foo: 100
                    }
                }
            },
            {
                PutRequest: {
                    Item: {
                        foo: 200
                    }
                }
            },
            {
                PutRequest: {
                    Item: {
                        foo: 300
                    }
                }
            }
        ];

        await from(params)
            .pipe(
                batchWriteWithRetry({
                    docClient,
                    tableName: 'fake-table'
                }),
                tap(tapFn)
            )
            .toPromise();

        expect(tapFn).toHaveBeenCalledTimes(1);
        expect(tapFn.mock.calls[0][0]).toEqual(responsePayload);

        expect(docClient.batchWrite).toHaveBeenCalledTimes(1);
        expect(docClient.batchWrite.mock.calls[0][0]).toEqual({
            RequestItems: {
                'fake-table': params
            }
        });
    });

    it('batchWriteWithRetry should batch in groups of 25', async () => {

        let responsePayload = {
            UnprocessedItems: {}
        };

        let docClient = {
            batchWrite: jest.fn()
                .mockImplementation(() => ({
                    promise: () => Promise.resolve(responsePayload)
                }))
        };

        let tapFn = jest.fn();

        let params = [];

        for(let i = 0; i < 30; i++) {
            params.push({
                PutRequest: {
                    Item: {
                        foo: i
                    }
                }
            });
        }

        await from(params)
            .pipe(
                batchWriteWithRetry({
                    docClient,
                    tableName: 'fake-table'
                }),
                tap(tapFn)
            )
            .toPromise();

        expect(tapFn).toHaveBeenCalledTimes(2);
        expect(tapFn.mock.calls[0][0]).toEqual(responsePayload);
        expect(tapFn.mock.calls[1][0]).toEqual(responsePayload);

        expect(docClient.batchWrite).toHaveBeenCalledTimes(2);
        expect(docClient.batchWrite.mock.calls[0][0]).toEqual({
            RequestItems: {
                'fake-table': params.slice(0,25)
            }
        });
        expect(docClient.batchWrite.mock.calls[1][0]).toEqual({
            RequestItems: {
                'fake-table': params.slice(25)
            }
        });
    });

    it('batchWriteWithRetry should retry unprocessed items', async () => {

        let responsePayloads = [
            {
                UnprocessedItems: {
                    'fake-table': [
                        {
                            PutRequest: {
                                Item: {
                                    foo: 300
                                }
                            }
                        }
                    ]
                }
            },
            {
                UnprocessedItems: {}
            }
        ];

        let docClient = {
            batchWrite: jest.fn()
                .mockImplementationOnce(() => ({
                    promise: () => Promise.resolve(responsePayloads[0])
                }))
                .mockImplementationOnce(() => ({
                    promise: () => Promise.resolve(responsePayloads[1])
                }))
        };

        let tapFn = jest.fn();

        let params = [
            {
                PutRequest: {
                    Item: {
                        foo: 100
                    }
                }
            },
            {
                PutRequest: {
                    Item: {
                        foo: 200
                    }
                }
            },
            {
                PutRequest: {
                    Item: {
                        foo: 300
                    }
                }
            }
        ];

        await from(params)
            .pipe(
                batchWriteWithRetry({
                    docClient,
                    tableName: 'fake-table'
                }),
                tap(tapFn)
            )
            .toPromise();

        expect(docClient.batchWrite).toHaveBeenCalledTimes(2);
        expect(docClient.batchWrite.mock.calls[0][0]).toEqual({
            RequestItems: {
                'fake-table': params
            }
        });
        expect(docClient.batchWrite.mock.calls[1][0]).toEqual({
            RequestItems: {
                'fake-table': [
                    {
                        PutRequest: {
                            Item: {
                                foo: 300
                            }
                        }
                    }
                ]
            }
        });

        expect(tapFn).toHaveBeenCalledTimes(2);
        expect(tapFn.mock.calls[0][0]).toEqual(responsePayloads[0]);
        expect(tapFn.mock.calls[1][0]).toEqual(responsePayloads[1]);
    });

    it('batchWriteWithRetry should return items if configured', async () => {

        let responsePayload = {
            UnprocessedItems: {}
        };

        let docClient = {
            batchWrite: jest.fn()
                .mockImplementation(() => ({
                    promise: () => Promise.resolve(responsePayload)
                }))
        };

        let tapFn = jest.fn();

        let params = [
            {
                PutRequest: {
                    Item: {
                        foo: 100
                    }
                }
            },
            {
                PutRequest: {
                    Item: {
                        foo: 200
                    }
                }
            },
            {
                PutRequest: {
                    Item: {
                        foo: 300
                    }
                }
            }
        ];

        await from(params)
            .pipe(
                batchWriteWithRetry({
                    docClient,
                    tableName: 'fake-table',
                    returnItems: true
                }),
                tap(tapFn)
            )
            .toPromise();

        expect(tapFn).toHaveBeenCalledTimes(3);
        expect(tapFn.mock.calls[0][0]).toEqual(params[0]);
        expect(tapFn.mock.calls[1][0]).toEqual(params[1]);
        expect(tapFn.mock.calls[2][0]).toEqual(params[2]);

        expect(docClient.batchWrite).toHaveBeenCalledTimes(1);
        expect(docClient.batchWrite.mock.calls[0][0]).toEqual({
            RequestItems: {
                'fake-table': params
            }
        });
    });

    it('batchWriteWithRetry should handle errors', async () => {
        expect.assertions(1);

        let docClient = {
            batchWrite: jest.fn()
                .mockImplementation(() => ({
                    promise: () => Promise.reject('!!!')
                }))
        };

        await from([123])
            .pipe(
                batchWriteWithRetry({
                    docClient,
                    tableName: 'fake-table'
                })
            )
            .toPromise()
            .catch(e => {
                expect(e).toBe('!!!');
            });
    });

    it('batchWriteWithRetry should cope with undefined unprocessed items', async () => {

        let responsePayloads = [
            {}
        ];

        let docClient = {
            batchWrite: jest.fn()
                .mockImplementationOnce(() => ({
                    promise: () => Promise.resolve(responsePayloads[0])
                }))
        };

        let tapFn = jest.fn();

        let params = [
            {
                PutRequest: {
                    Item: {
                        foo: 100
                    }
                }
            }
        ];

        await from(params)
            .pipe(
                batchWriteWithRetry({
                    docClient,
                    tableName: 'fake-table'
                }),
                tap(tapFn)
            )
            .toPromise();

        expect(docClient.batchWrite).toHaveBeenCalledTimes(1);
        expect(docClient.batchWrite.mock.calls[0][0]).toEqual({
            RequestItems: {
                'fake-table': params
            }
        });

        expect(tapFn).toHaveBeenCalledTimes(1);
        expect(tapFn.mock.calls[0][0]).toEqual(responsePayloads[0]);
    });
});
