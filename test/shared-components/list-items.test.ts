import * as RNEA from 'fp-ts/ReadonlyNonEmptyArray';
import { pipe } from 'fp-ts/function';
import { templateListItems } from '../../src/shared-components';
import { toHtmlFragment } from '../../src/types/html-fragment';

describe('list-items template', () => {
  it.each([
    [['Alice'] as const, toHtmlFragment('<li class="item">Alice</li>\n')],
    [['Alice', 'Bob'] as const, toHtmlFragment('<li class="item">Alice</li>\n<li class="item">Bob</li>\n')],
  ])('returns <li> elements on %s', (items, expected) => {
    const actual = pipe(
      items,
      RNEA.map(toHtmlFragment),
      templateListItems,
    );

    expect(actual).toBe(expected);
  });
});
