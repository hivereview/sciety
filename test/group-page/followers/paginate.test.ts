import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import { pipe } from 'fp-ts/function';
import { paginate } from '../../../src/group-page/followers/paginate';
import * as DE from '../../../src/types/data-error';
import { arbitraryNumber } from '../../helpers';
import { shouldNotBeCalled } from '../../should-not-be-called';
import { arbitraryGroupId } from '../../types/group-id.helper';
import { arbitraryUserId } from '../../types/user-id.helper';

const generateFollowers = (followerCount: number) => pipe(
  Array(followerCount).keys(),
  Array.from,
  RA.map(() => ({
    userId: arbitraryUserId(),
    listCount: arbitraryNumber(0, 10),
    followedGroupCount: arbitraryNumber(0, 10),
  })),
);

/* eslint-disable jest/no-commented-out-tests */
describe('paginate', () => {
  describe('when the group has multiple followers', () => {
    it('limits the number of followers to the requested page size', () => {
      const partialViewModel = {
        followerCount: 3,
        followers: generateFollowers(3),
      };
      const result = pipe(
        partialViewModel,
        paginate(arbitraryGroupId(), 1, 2),
        E.getOrElseW(shouldNotBeCalled),
      );

      expect(result.followers).toHaveLength(2);
    });

    it('returns the specified page of the followers', () => {
      const userId = arbitraryUserId();
      const [follower1, follower2] = generateFollowers(2);
      const partialViewModel = {
        followerCount: 3,
        followers: [
          follower1,
          {
            userId,
            listCount: arbitraryNumber(0, 10),
            followedGroupCount: arbitraryNumber(0, 10),
          },
          follower2,
        ],
      };
      const result = pipe(
        partialViewModel,
        paginate(arbitraryGroupId(), 2, 1),
        E.getOrElseW(shouldNotBeCalled),
      );

      expect(result.followers).toStrictEqual([
        expect.objectContaining({
          userId,
        }),
      ]);
    });

    it.each([
      [9, 1, O.none],
      [11, 1, O.some(2)],
      [20, 1, O.some(2)],
      [20, 2, O.none],
      [21, 2, O.some(3)],
      [21, 3, O.none],
    ])('given %d followers and a request for page %d, returns the next page', (followerCount, page, nextPage) => {
      const partialViewModel = {
        followerCount,
        followers: generateFollowers(followerCount),
      };
      const result = pipe(
        partialViewModel,
        paginate(arbitraryGroupId(), page, 10),
        E.getOrElseW(shouldNotBeCalled),
      );

      expect(result.nextPage).toStrictEqual(nextPage);
    });

    it('returns not-found when asked for a page that does not exist', () => {
      const result = pipe(
        {
          followerCount: 3,
          followers: generateFollowers(3),
        },
        paginate(arbitraryGroupId(), 3, 3),
      );

      expect(result).toStrictEqual(E.left(DE.notFound));
    });

    it.todo('returns an empty page 1 when there are no followers');
  });
});
