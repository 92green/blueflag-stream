// @flow
import {queryAll} from '../index';
import {scanAll} from '../index';
import {batchGetWithRetry} from '../index';
import {batchWriteWithRetry} from '../index';
import queryAllOriginal from '../queryAll';
import scanAllOriginal from '../scanAll';
import batchGetWithRetryOriginal from '../batchGetWithRetry';
import batchWriteWithRetryOriginal from '../batchWriteWithRetry';

test('index should export everything', () => {
    expect(queryAll).toBe(queryAllOriginal);
    expect(scanAll).toBe(scanAllOriginal);
    expect(batchGetWithRetry).toBe(batchGetWithRetryOriginal);
    expect(batchWriteWithRetry).toBe(batchWriteWithRetryOriginal);
});
