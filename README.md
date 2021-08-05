# 92green-stream

Streaming algorithms and helpful stuff.

Right now we're mostly doing rx.

## [92green-rxjs](packages/92green-rxjs)

```
yarn add 92green-rxjs
```

Rxjs algorithms.

- [multiCache](#multiCache)
- [memoryCache](#memoryCache)
- [zipDiff](#zipDiff)
- [operators/bufferDistinct](#operatorsbufferDistinct)
- [operators/complete](#operatorscomplete)
- [operators/route](#operatorsroute)
- [dynamodb/batchGetWithRetry](#dynamodbbatchGetWithRetry)
- [dynamodb/batchWriteWithRetry](#dynamodbbatchWriteWithRetry)
- [dynamodb/queryAll](#dynamodbqueryAll)
- [dynamodb/scanAll](#dynamodbscanAll)

### multiCache

Accepts an array of sources (intended to be things like in-memory caches, database caches and actual data sources) and for each item attempts to retrieve the item from each source sequentially until it is found.
It also saves items in caches once found.
it does not guarantee item order.

```js
import {multiCache} from '92green-rxjs';

let myMemoryCacheOperator = {
    name: 'memory-cache',
    load, // operator that loads from cache
    save, // operator that saves to cache (not needed on data source)
    clear, // operator that clears item from cache (not needed on data source)
};

// ^ all of the above operators must accept a payload of {id, item} and return {id, item}
//   item may be undefined if not known or not found

let cache = multiCache([
    myMemoryCacheOperator, // first cache
    myDatabaseCacheOperator, // deeper cache
    myDataSource // data source
])

from([
    {id: 'a'},
    {id: 'b'},
    {id: 'c'}
])
    .pipe(cache.load);

// Example output observable:

// {id: 'a', item: {id: 'a', name: 'Hi'}, source: 'data-source'}
// {id: 'b', item: {id: 'b', name: 'Hello'}, source: 'memory-cache'}
// {id: 'c', item: undefined, source: undefined}

```

### memoryCache

An rx cache that works a lot like [graphql/dataloader](https://github.com/graphql/dataloader).

Usage with `multiCache`:

```js
import {from} from 'rxjs'
import {flatMap} from 'rxjs/operators'
import {multiCache} from '92green-rxjs';
import {memoryCache} from '92green-rxjs';

let dataSource = {
    name: 'data-source',
    load: flatMap(async (payload) => ({
        ...payload,
        item: await fetch(payload.id)
    }))
};

let cache = multiCache([
    memoryCache(),
    dataSource
]);

from([
    {id: 'a'},
    {id: 'b'},
    {id: 'a'}
])
    .pipe(cache.load);


// Example output observable:

// {id: 'a', item: {id: 'a', name: 'Hi'}, source: 'data-source'}
// {id: 'b', item: {id: 'b', name: 'Hello'}, source: 'data-source'}
// {id: 'a', item: {id: 'a', name: 'Hi'}, source: 'memory-cache'}
// a is only requested once
```

### zipDiff

Takes two observables and finds which items exist in one or both, according to a key.
It emits items with matching items zipped together on an object: `{a: itemA, b: itemB}`,
and emits unmatched items on objects with single keys like `{a: itemA}`, `{b: itemB}`.

The `keyBy` function is called on each item. The data returned is used as an identifier to
work out when two items are matching.

A second `keyBy` function can be added if you need a different keyBy function for the second observable.

Items are emitted as early as they can be, so the internal buffer can try and be as small as possible. Pairs of matching items are emitted as soon as they match, and non-matching items are emitted as soon as it's known that it's impossible that there will be a match.
(e.g. if the buffer contains items from A, and input observable B completes, then it's known there will never be matches for any buffered A's, so they are all emitted)

```js
import {zipDiff} from '92green-rxjs';

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

### operators/bufferDistinct

Buffers items while the given predicate function returns the same thing, and emits the buffer contents when the given predicate function returns something else or the stream closes. Value comparison is performed using `Object.is()`.

If `flushObs` is passed, the buffer will be flushed any time `flushObs` emits.

```js
import {bufferDistinct} from '92green-rxjs/operators';

bufferDistinct(item => comparisonValue, flushObs: ?Observable)
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


### operators/complete

Emits a single item upon completion of the observable.

```js
import {complete} from '92green-rxjs/operators';

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

### operators/route

Routes based on a selector. 

First parameter is a function that returns the string that is used for selecting from the routes.

Second parameter is an object that keys correspond to the strings that are returned from the selector and values are operators that operate on the different streams.   


```js
route(selector: (any) => string, routes: { [string]: (Observable) }) => (obs: Observable): Observable
```

```js
import {route} from '92green-rxjs/operators';  

from([
    {selector: 'negOne', value: 1},
    {selector: 'negOne', value: 2},
    {selector: 'addOne', value: 1},
    {selector: 'addOne', value: 2},
    {selector: 'addOne', value: 3}
]).pipe(
    route(
        (item) => item.selector,
        {
            'negOne': (obs) => obs.pipe(map(ii => ii.value - 1)),
            'addOne': (obs) => obs.pipe(map(ii => ii.value + 1))
        }
    )
)
//Result 0, 1, 2, 3, 4

```


### dynamodb/batchGetWithRetry

Turns AWS `DocClient.batchGet()` into a pipeable observable which accepts an observable of ids and calls `batchGet()`, batching items to 100 at a time and retrying dynamo's `UnprocessedKeys` automatically.


```js
import {batchGetWithRetry} from '92green-rxjs/dynamodb';

batchGetWithRetry({
    docClient: DocClient,
    tableName: string,
    returnItems?: boolean
}): Observable => Observable
```

```js
let keys = [
    {id: 1},
    {id: 2},
    {id: 3}
];

from(keys)
    .pipe(batchGetWithRetry(...))
    .toPromise();
```


### dynamodb/batchWriteWithRetry

Turns AWS `DocClient.batchWrite()` into a pipeable observable which accepts an observable of params and calls `batchWrite()`, batching items to 25 at a time and retrying dynamo's `UnprocessedItems` automatically.


```js
import {batchWriteWithRetry} from '92green-rxjs/dynamodb';

batchWriteWithRetry({
    docClient: DocClient,
    tableName: string,
    returnItems?: boolean
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
    .pipe(batchWriteWithRetry(...))
    .toPromise();
```


### dynamodb/queryAll

Turns AWS `DocClient.query()` into an observable which will by default keep requesting whenever there is more data to be paginated.


```js
import {queryAll} from '92green-rxjs/dynamodb';

queryAll(
    docClient: DocClient,
    params: Params,
    feedbackObservable: ?Observable
): Observable
```

### dynamodb/scanAll

Turns AWS `DocClient.scan()` into an observable which will by default keep requesting whenever there is more data to be paginated.


```js
import {scanAll} from '92green-rxjs/dynamodb';

scanAll(
    docClient: DocClient,
    params: Params,
    feedbackObservable: ?Observable
): Observable
```

