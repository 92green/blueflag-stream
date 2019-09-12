# blueflag-rxjs

Rxjs algorithms.

- [chain](#chain)
- [zipDiff](#zipDiff)
- [operators/bufferDistinct](#operators/bufferDistinct)
- [operators/complete](#operators/complete)
- [dynamodb/batchGet](#dynamodb/batchGet)
- [dynamodb/batchWrite](#dynamodb/batchWrite)
- [dynamodb/queryAll](#dynamodb/queryAll)

## chain

Takes multiple observables and concats them, but emits all items.

```js
import {chain} from 'blueflag-rxjs';

chain(
    obsA: Observable,
    obsB: Observable,
    ...
): Observable
```

```js
chain(obsA, obsB)

// obsA adds "1"
// obsA adds "2"
// obsB adds "3"
// obsB adds "4"

// output: "1", "2", "3", "4"

```

## zipDiff

Takes two observables and finds which items exist in one or both, according to a key.
It emits items with matching items zipped together on an object: `{a: itemA, b: itemB}`,
and emits unmatched items on objects with single keys like `{a: itemA}`, `{b: itemB}`.

The `keyBy` function is called on each item. The data returned is used as an identifier to
work out when two items are matching.

A second `keyBy` function can be added if you need a different keyBy function for the second observable.

Items are emitted as early as they can be, so the internal buffer can try and be as small as possible. Pairs of matching items are emitted as soon as they match, and non-matching items are emitted as soon as it's known that it's impossible that there will be a match.
(e.g. if the buffer contains items from A, and input observable B completes, then it's known there will never be matches for any buffered A's, so they are all emitted)

```js
import {zipDiff} from 'blueflag-rxjs';

zipDiff(
    obsA: Observable,
    obsB: Observable,
    keyBy: (item) => any,
    keyByB?: (item) => any
): Observable
```

```js
zipDiff(obsA, obsB, itemA => itemA.id)

// obsA adds {id: "1", name: "One"}
// obsA adds {id: "2", name: "Two"}
// obsB adds {id: "3", name: "Three"}
// obsB adds {id: "2", name: "Too"}

// output:

{
    a: {id: "2", name: "Two"},
    b: {id: "2", name: "Too"}
}

{
    a: {id: "1", name: "One"}
}

{
    b: {id: "3", name: "Three"}
}

```

## operators/bufferDistinct

Buffers items while the given predicate function returns the same thing, and emits the buffer contents when the given predicate function returns something else or the stream closes. Value comparison is performed using `Object.is()`.

```js
import {bufferDistinct} from 'blueflag-rxjs/operators';

bufferDistinct(item => comparisonValue)
```

```js
obs.pipe(
    bufferDistinct(item => item.id)
);

// Obs adds {id: 'a', value: 1}
// Obs adds {id: 'a', value: 2}
// Obs adds {id: 'b', value: 3}
// Obs adds {id: 'c', value: 4}
// Obs adds {id: 'c', value: 5}

// output:

[
    {id: 'a', value: 1},
    {id: 'a', value: 2}
]

[
    {id: 'b', value: 3}
]

[
    {id: 'c', value: 4},
    {id: 'c', value: 5}
]
```


## operators/complete

Emits a single item upon completion of the observable.

```js
import {complete} from 'blueflag-rxjs/operators';

complete()
```

```js
obs.pipe(
    complete(),
    tap(() => {
        console.log("I'm done mate");
    })
);
```

## dynamodb/batchGet

Turns AWS `DocClient.batchGet()` into a pipeable observable which accepts an observable of ids and calls `batchGet()`, batching items to 100 at a time and retrying dynamo's `UnprocessedKeys` automatically.


```js
import {batchGet} from 'blueflag-rxjs/dynamodb';

batchGet({
    docClient: DocClient,
    tableName: string
}): Observable => Observable
```

```js
let keys = [
    {id: 1},
    {id: 2},
    {id: 3}
];

from(keys)
    .pipe(batchGet)
    .toPromise();
```


## dynamodb/batchWrite

Turns AWS `DocClient.batchWrite()` into a pipeable observable which accepts an observable of params and calls `batchWrite()`, batching items to 25 at a time and retrying dynamo's `UnprocessedItems` automatically.


```js
import {batchWrite} from 'blueflag-rxjs/dynamodb';

batchWrite({
    docClient: DocClient,
    tableName: string
}): Observable => Observable
```

```js
from([{
    {
        PutRequest: {
            Item: {
                foo: 200
            }
        }
    }
    ...
}])
    .pipe(batchWrite)
    .toPromise();
```


## dynamodb/queryAll

Turns AWS `DocClient.query()` into an observable which will by default keep requesting whenever there is more data to be paginated.


```js
import {queryAll} from 'blueflag-rxjs/dynamodb';

queryAll(
    docClient: DocClient,
    params: Params,
    feedbackObservable: ?Observable
): Observable
```
