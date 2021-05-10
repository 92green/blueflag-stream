import {createDataloader} from '../src';
import Dataloader from 'dataloader';

jest.mock('dataloader', () => {
  return jest.fn().mockImplementation(() => undefined);
});

afterEach(() => {
     jest.clearAllMocks();
});

describe('createDataloader with mocked Dataloader', () => {

    it('should pass through correct defaults', async () => {

        createDataloader<string,string>(async () => [], {resultToKey: str => str});

        // @ts-ignore
        expect(Dataloader.mock.calls[0][1]).toEqual({
            maxBatchSize: Number.POSITIVE_INFINITY,
            cacheKeyFn: JSON.stringify,
            cache: true,
            batchScheduleFn: undefined,
            cacheMap: new Map()
        });

    });

    it('should allow overriding defaults', async () => {

        const cacheKeyFn = (_thing: any): string => ':)';
        const batchScheduleFn = (callback: () => void) => setTimeout(callback, 10);

        createDataloader<string,string>(async () => [], {
            resultToKey: str => str,
            maxBatchSize: 2,
            cacheKeyFn,
            cache: false,
            batchScheduleFn
        });

        // @ts-ignore
        expect(Dataloader.mock.calls[0][1]).toEqual({
            maxBatchSize: 2,
            cacheKeyFn,
            cache: false,
            batchScheduleFn,
            cacheMap: new Map()
        });

    });

});
