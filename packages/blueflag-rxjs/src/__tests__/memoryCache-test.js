// @flow
import {TestScheduler} from 'rxjs/testing';
import memoryCache from '../memoryCache';

import {of} from 'rxjs';
import {merge} from 'rxjs';
import {delay} from 'rxjs/operators';
import {flatMap} from 'rxjs/operators';
import {map} from 'rxjs/operators';
import {tap} from 'rxjs/operators';

describe('memoryCache', () => {

    it('should remember values and not re-fetch them', () => {

        const fetch = jest.fn((id) => of(values[id]).pipe(delay(3)));

        let values = {
            a: {
                id: 'a',
                item: 'item-a'
            },
            b: {
                id: 'b',
                item: 'item-b'
            }
        };

        const testScheduler = new TestScheduler((actual, expected) => {
            expect(actual).toEqual(expected);
            expect(fetch).toHaveBeenCalledTimes(1);
        });

        testScheduler.run(helpers => {
            const {cold, expectObservable, expectSubscriptions} = helpers;

            const inputObs = cold('--a----a-a-|');
            const subs =          '^----------!';
            const expected =      '-----a-a-a-|';

            let fetcher = flatMap((payload) => {
                if(payload.item) { // when used in multiCache, this check is done for you
                    return of(payload);
                }
                return fetch(payload.id);
            });

            let cache = memoryCache();

            expectObservable(inputObs.pipe(
                map(id => ({id})),
                cache.load,
                fetcher,
                cache.save
            )).toBe(expected, values);

            expectSubscriptions(inputObs.subscriptions).toBe(subs);
        });

    });

    it('should return even when ids from backend match existing cache item', async () => {
        const fetch = jest.fn((id) => of({test: '123'}).pipe(delay(3)));
        let fetcher = flatMap((payload) => {
            if(payload.item) { // when used in multiCache, this check is done for you
                return of(payload);
            }
            return fetch(payload.id);
        });

        let cache = memoryCache();

        let result1 = await of("a").pipe(
            cache.load,
            fetcher,
            map(ii => ({id: "a"})),
            cache.save
        ).toPromise()
        let result = await of("b").pipe(
            cache.load,
            fetcher,
            map(ii => ({id: "a"})),
            cache.save
        ).toPromise()
        expect(result).toEqual(result1)    
    })

    it('should collect values with a common id and return the result of the same fetch', () => {

        const fetch = jest.fn((id) => of(values[id]).pipe(delay(3)));

        let values = {
            a: {
                id: 'a',
                item: 'item-a'
            },
            b: {
                id: 'b',
                item: 'item-b'
            }
        };

        const testScheduler = new TestScheduler((actual, expected) => {
            expect(actual).toEqual(expected);
            expect(fetch).toHaveBeenCalledTimes(1);
        });

        testScheduler.run(helpers => {
            const {cold, expectObservable, expectSubscriptions} = helpers;

            const inputObs = cold('--aaa------|');
            const subs =          '^----------!';
            const expected =      '-----(aaa)-|';

            let fetcher = flatMap((payload) => {
                if(payload.item) { // when used in multiCache, this check is done for you
                    return of(payload);
                }
                return fetch(payload.id);
            });

            let cache = memoryCache();

            expectObservable(inputObs.pipe(
                map(id => ({id})),
                cache.load,
                fetcher,
                cache.save
            )).toBe(expected, values);

            expectSubscriptions(inputObs.subscriptions).toBe(subs);
        });

    });

    it('should remember values by id and not re-fetch them', () => {

        const fetch = jest.fn((id) => of(values[id]).pipe(delay(3)));

        let values = {
            a: {
                id: 'a',
                item: 'item-a'
            },
            b: {
                id: 'b',
                item: 'item-b'
            }
        };

        const testScheduler = new TestScheduler((actual, expected) => {
            expect(actual).toEqual(expected);
            expect(fetch).toHaveBeenCalledTimes(2);
        });

        testScheduler.run(helpers => {
            const {cold, expectObservable, expectSubscriptions} = helpers;

            const inputObs = cold('--a-b----a-b-|');
            const subs =          '^------------!';
            const expected =      '-----a-b-a-b-|';

            let fetcher = flatMap((payload) => {
                if(payload.item) { // when used in multiCache, this check is done for you
                    return of(payload);
                }
                return fetch(payload.id);
            });

            let cache = memoryCache();

            expectObservable(inputObs.pipe(
                map(id => ({id})),
                cache.load,
                fetcher,
                cache.save
            )).toBe(expected, values);

            expectSubscriptions(inputObs.subscriptions).toBe(subs);
        });

    });

    it('should clear values from cache', () => {

        const fetch = jest.fn((id) => of(values[id]).pipe(delay(3)));

        let values = {
            a: {
                id: 'a',
                item: 'item-a'
            },
            b: {
                id: 'b',
                item: 'item-b'
            },
            B: { // B is just b with only an id
                id: 'b'
            }
        };

        const testScheduler = new TestScheduler((actual, expected) => {
            expect(actual).toEqual(expected);
            expect(fetch).toHaveBeenCalledTimes(3);
        });

        testScheduler.run(helpers => {
            const {cold, expectObservable, expectSubscriptions} = helpers;

            const inputObs = cold('--a-b------a-b----|');
            const clearObs = cold('---------b--------|');
            const subs =          '^-----------------!';
            const expected =      '-----a-b-B-a----b-|'; // B is just b with only an id

            let fetcher = flatMap((payload) => {
                if(payload.item) { // when used in multiCache, this check is done for you
                    return of(payload);
                }
                return fetch(payload.id);
            });

            let cache = memoryCache();

            let obs = merge(
                inputObs.pipe(
                    map(id => ({id})),
                    cache.load,
                    fetcher,
                    cache.save
                ),
                clearObs.pipe(
                    map(id => ({id})),
                    cache.clear
                )
            );

            expectObservable(obs).toBe(expected, values);

            expectSubscriptions(inputObs.subscriptions).toBe(subs);
        });

    });

});
