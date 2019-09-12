// @flow
import {chain} from '../index';
import chainOriginal from '../chain';
import {zipDiff} from '../index';
import zipDiffOriginal from '../zipDiff';

test('index should export everything', () => {
    expect(chain).toBe(chainOriginal);
    expect(zipDiff).toBe(zipDiffOriginal);
});
