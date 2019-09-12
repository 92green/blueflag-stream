// @flow

import {Observable, Subscription} from 'rxjs';

export type ObservableGenerator<T> = Observable<T> | ((v: T) => Observable<T>);

// from https://stackblitz.com/edit/rxjs-ruj1m3
// and discussed here https://github.com/ReactiveX/rxjs/issues/4711

export default function chain<T>(seed: Observable<T>, ...obsGenerators: Array<ObservableGenerator<T>>) {
    return new Observable<T>(masterSubscriber => {
        let lastValueFromPrev: T;
        let childSubscription: Subscription;

        const switchTo = (obs: Observable<T>) => {
            childSubscription = obs.subscribe({
                next: v => {
                    lastValueFromPrev = v;
                    masterSubscriber.next(v);
                },
                error: masterSubscriber.error,
                complete: () => {
                    /* istanbul ignore next */
                    if(childSubscription) {
                        childSubscription.unsubscribe();
                    }
                    const next = obsGenerators.shift();
                    if(!next) {
                        masterSubscriber.complete();
                    } else {
                        /* istanbul ignore next */
                        switchTo(typeof next === 'function' ? next(lastValueFromPrev) : next);
                    }
                }
            });
        };

        switchTo(seed);

        return () => {
            childSubscription.unsubscribe();
        };
    });
}
