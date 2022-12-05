import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as T from 'fp-ts/Task';
import { flow, pipe } from 'fp-ts/function';
import { groupPage, groupPageTabs } from '../../src/group-page/group-page';
import * as DE from '../../src/types/data-error';
import { arbitraryWord } from '../helpers';
import { shouldNotBeCalled } from '../should-not-be-called';

describe('group page', () => {
  describe('when the group does not exist', () => {
    it('returns a notFound error', async () => {
      const result = await pipe(
        {
          slug: arbitraryWord(),
          user: O.none,
          page: 1,
        },
        groupPage({
          fetchStaticFile: shouldNotBeCalled,
          getAllEvents: T.of([]),
          selectAllListsOwnedBy: shouldNotBeCalled,
          getUserDetailsBatch: shouldNotBeCalled,
          getGroupBySlug: () => E.left(DE.notFound),
        })(groupPageTabs.lists),
        T.map(flow(
          E.matchW(
            (res) => res.type,
            shouldNotBeCalled,
          ),
          DE.isNotFound,
        )),
      )();

      expect(result).toBe(true);
    });
  });
});
