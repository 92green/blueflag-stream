import Dataloader from 'dataloader';
import {LRUMap} from 'lru_map';

export type BatchLoader<T,K> = (keys: readonly K[]) => Promise<T[]>;

export type Options<T,K> = {
    resultToKey: (item: T) => K;
    cacheKeyFn?: (key: K) => string;
    maxBatchSize?: number;
    maxCachedItems?: number;
    cache?: boolean;
    batchScheduleFn?: (callback: () => void) => void;
};

export function createDataloader<T,K>(
    batchLoader: BatchLoader<T,K>,
    options: Options<T,K>
) {
    const {
        resultToKey,
        cacheKeyFn = JSON.stringify,
        maxBatchSize = Number.POSITIVE_INFINITY,
        maxCachedItems = 0,
        cache = true,
        batchScheduleFn
    } = options;

    return new Dataloader(async (keys: readonly K[]): Promise<(T|undefined)[]> => {
        const items = await batchLoader(keys);
        const itemsByKey = new Map<string,T>(items.map(item => {
            return [cacheKeyFn(resultToKey(item)), item];
        }));

        return keys.map(key => itemsByKey.get(cacheKeyFn(key)));
    }, {
        cacheMap: maxCachedItems ? new LRUMap(maxCachedItems) : new Map(),
        maxBatchSize,
        cacheKeyFn,
        cache,
        batchScheduleFn
    });
}
