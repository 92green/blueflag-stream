// @flow
import {TestScheduler} from 'rxjs/testing';
import chain from '../chain';

describe('chain', () => {

    it('chain should chain observables togther (concatAll() style) while emitting all items from all observables', () => {

        const testScheduler = new TestScheduler((actual, expected) => {
            expect(actual).toEqual(expected);
        });

        testScheduler.run(helpers => {
            const {cold, expectObservable, expectSubscriptions} = helpers;

            const a =   cold('-a-b-c-|');
            const aSubs =    '^------!';
            const b =          cold('-d-e-f-|');
            const bSubs =    '-------^------!';
            const c =                 cold('-g-h-i-|');
            const cSubs =    '--------------^------!';
            const expected = '-a-b-c--d-e-f--g-h-i-|';

            expectObservable(
                chain(a, b, c)
            ).toBe(expected);

            expectSubscriptions(a.subscriptions).toBe(aSubs);
            expectSubscriptions(b.subscriptions).toBe(bSubs);
            expectSubscriptions(c.subscriptions).toBe(cSubs);
        });
    });

    it('chain should be able to unsubscribe early', () => {

        const testScheduler = new TestScheduler((actual, expected) => {
            expect(actual).toEqual(expected);
        });

        testScheduler.run(helpers => {
            const {cold, expectObservable, expectSubscriptions} = helpers;

            const a =   cold('--a-|');
            const subs =     '^---!';
            const expected = '--a-|';

            expectObservable(
                chain(a)
            ).toBe(expected);

            expectSubscriptions(a.subscriptions).toBe(subs);
        });
    });

});
