import {createDataloader, createDataloaderPool} from '../src';

type Person = {
    id: string;
    name: string;
};

const people = new Map<string,Person>([
    ['a', {id: 'a', name: 'Ankle'}],
    ['b', {id: 'b', name: 'Bankle'}],
    ['d', {id: 'd', name: 'Dankle'}]
]);

type TenantPerson = {
    tenantId: string;
    id: string;
    name: string;
};

type TenantPersonKey = [string,string];

const tenantPeople = new Map<string,TenantPerson>([
    ['X#a', {tenantId: 'X', id: 'a', name: 'Ankle'}],
    ['X#b', {tenantId: 'X', id: 'b', name: 'Bankle'}],
    ['Y#a', {tenantId: 'Y', id: 'a', name: 'Angly'}]
]);

// make the batchScheduleFn use setTimeout so we can control test with jest fake timers
jest.useFakeTimers();
const batchScheduleFn = (callback: () => void) => setTimeout(callback, 10);

afterEach(() => {
     jest.clearAllMocks();
});

describe('createDataloader', () => {

    it('loads string keys', async () => {

        const batchLoader = jest.fn(async (ids: readonly string[]): Promise<Person[]> => {
            return ids
                .map(id => people.get(id))
                .filter((p): p is Person => !!p);
        });

        const resultToKey = jest.fn((person: Person): string => person.id);

        const loader = createDataloader<Person,string>(
            batchLoader,
            {
                resultToKey,
                batchScheduleFn
            }
        );

        const promises = [
            loader.load('a'),
            loader.load('b'),
            loader.load('b'),
            loader.load('b'),
            loader.load('c'),
            loader.load('a')
        ];

        expect(batchLoader).toHaveBeenCalledTimes(0);
        jest.runAllTimers();
        expect(batchLoader).toHaveBeenCalledTimes(1);
        // notice how keys are deduped
        expect(batchLoader.mock.calls[0][0]).toEqual(['a','b','c']);

        const results = await Promise.all(promises);

        expect(resultToKey).toHaveBeenCalledTimes(2);
        expect(resultToKey.mock.calls[0][0]).toEqual({
            id: 'a',
            name: 'Ankle'
        });

        expect(results).toEqual([
            {id: 'a', name: 'Ankle'},
            {id: 'b', name: 'Bankle'},
            {id: 'b', name: 'Bankle'},
            {id: 'b', name: 'Bankle'},
            undefined,
            {id: 'a', name: 'Ankle'}
        ]);

        //
        // try a second batch
        //

        const morePromises = [
            loader.load('a'),
            loader.load('d')
        ];

        jest.runAllTimers();
        expect(batchLoader).toHaveBeenCalledTimes(2);
        expect(batchLoader.mock.calls[1][0]).toEqual(['d']);

        const moreResults = await Promise.all(morePromises);

        expect(moreResults).toEqual([
            {id: 'a', name: 'Ankle'},
            {id: 'd', name: 'Dankle'}
        ]);

    });

    it('loads object or tuple keys', async () => {

        const batchLoader = jest.fn(async (ids: readonly TenantPersonKey[]): Promise<TenantPerson[]> => {
            return ids
                .map(([tenantId, id]) => tenantPeople.get(`${tenantId}#${id}`))
                .filter((p): p is TenantPerson => !!p);
        });

        const resultToKey = jest.fn((person: TenantPerson): TenantPersonKey => [person.tenantId, person.id]);

        const loader = createDataloader<TenantPerson,TenantPersonKey>(
            batchLoader,
            {
                resultToKey,
                batchScheduleFn
            }
        );

        const promises = [
            loader.load(['X','a']),
            loader.load(['X','b']),
            loader.load(['Y','a'])
        ];

        expect(batchLoader).toHaveBeenCalledTimes(0);
        jest.runAllTimers();
        expect(batchLoader).toHaveBeenCalledTimes(1);
        expect(batchLoader.mock.calls[0][0]).toEqual([['X','a'], ['X','b'], ['Y','a']]);

        const results = await Promise.all(promises);

        expect(results).toEqual([
            {tenantId: 'X', id: 'a', name: 'Ankle'},
            {tenantId: 'X', id: 'b', name: 'Bankle'},
            {tenantId: 'Y', id: 'a', name: 'Angly'}
        ]);

    });

    it('caches a limited number of items', async () => {

        const batchLoader = jest.fn(async (ids: readonly string[]): Promise<Person[]> => {
            return ids
                .map(id => people.get(id))
                .filter((p): p is Person => !!p);
        });

        const resultToKey = jest.fn((person: Person): string => person.id);

        const loader = createDataloader<Person,string>(
            batchLoader,
            {
                batchScheduleFn,
                resultToKey,
                maxCachedItems: 2
            }
        );

        const promises = [
            loader.load('a'),
            loader.load('b')
        ];

        jest.runAllTimers();
        const results = await Promise.all(promises);
        expect(results).toEqual([
            {id: 'a', name: 'Ankle'},
            {id: 'b', name: 'Bankle'}
        ]);


        // @ts-expect-error
        expect(loader._cacheMap.size).toBe(2);

        //
        // try a second batch
        //

        const morePromises = [
            loader.load('d')
        ];

        jest.runAllTimers();
        const moreResults = await Promise.all(morePromises);
        expect(moreResults).toEqual([
            {id: 'd', name: 'Dankle'}
        ]);


        // @ts-expect-error
        expect(loader._cacheMap.size).toBe(2);

    });
});

describe('createDataloaderPool', () => {

    const zombies = new Map<string,Person>([
        ['a', {id: 'a', name: 'AAAAA'}]
    ]);

    const batchLoader = jest.fn(async (poolKey: string, ids: readonly string[]): Promise<Person[]> => {
        return ids
            .map(id => poolKey === 'people' ? people.get(id) : zombies.get(id))
            .filter((p): p is Person => !!p);
    });

    const resultToKey = jest.fn((person: Person): string => person.id);

    it('should create pools as accessed', async () => {

        const pool = createDataloaderPool<Person,string>(
            batchLoader,
            {
                batchScheduleFn,
                resultToKey
            }
        );

        const promises = [
            pool.get('people').load('a'),
            pool.get('people').load('b'),
            pool.get('zombies').load('a')
        ];

        jest.runAllTimers();
        const results = await Promise.all(promises);
        expect(results).toEqual([
            {id: 'a', name: 'Ankle'},
            {id: 'b', name: 'Bankle'},
            {id: 'a', name: 'AAAAA'}
        ]);

    });

    it('should have max pools', async () => {

        const pool = createDataloaderPool<Person,string>(
            batchLoader,
            {
                batchScheduleFn,
                resultToKey,
                maxPools: 2
            }
        );

        const promises = [
            pool.get('people').load('a'),
            pool.get('people').load('b'),
            pool.get('zombies').load('a')
        ];

        jest.runAllTimers();
        await Promise.all(promises);

        //
        // try to make another pool and hit the limit
        //

        const morePromises = [
            pool.get('zombies2').load('a')
        ];

        jest.runAllTimers();
        await Promise.all(morePromises);

        // @ts-expect-error
        expect(pool._cacheMap.size).toBe(2);

    });

});
