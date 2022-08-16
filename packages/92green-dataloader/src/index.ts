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
): Dataloader<K,T|undefined,string> {
    const {
        resultToKey,
        cacheKeyFn = JSON.stringify,
        maxBatchSize = Number.POSITIVE_INFINITY,
        maxCachedItems = 0,
        cache = true,
        batchScheduleFn
    } = options;

    return new Dataloader<K,T|undefined,string>(async (keys: readonly K[]): Promise<(T|Error|undefined)[]> => {
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

export type PoolBatchLoader<T,K> = (poolId: string, keys: readonly K[]) => Promise<T[]>;

export type PoolOptions<T,K> = Options<T,K> & {
    maxPools?: number;
};

export type DataloaderPool<K,T> = {
    get: (poolKey: string) => Dataloader<K,T|undefined,string>;
    _cacheMap: unknown;
};

export function createDataloaderPool<T,K>(
    batchLoader: PoolBatchLoader<T,K>,
    options: PoolOptions<T,K>
): DataloaderPool<K,T> {

    const {maxPools, ...loaderOptions} = options;

    const _cacheMap = maxPools
        ? new LRUMap<string,Dataloader<K,T|undefined,string>>(maxPools)
        : new Map<string,Dataloader<K,T|undefined,string>>();

    const get = (poolKey: string): Dataloader<K,T|undefined,string> => {
        const loader = _cacheMap.get(poolKey);
        if(loader) return loader;

        const newLoader = createDataloader((keys) => batchLoader(poolKey, keys), loaderOptions);
        _cacheMap.set(poolKey, newLoader);
        return newLoader;
    };

    return {
        _cacheMap,
        get
    };
}
