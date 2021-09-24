import * as TE from 'fp-ts/TaskEither';
import * as TO from 'fp-ts/TaskOption';
import { pipe } from 'fp-ts/function';
import { groupEvaluatedArticle } from '../../../src/domain-events';
import { groupEvaluatedArticleCard } from '../../../src/sciety-feed-page/cards';
import * as DE from '../../../src/types/data-error';
import { shouldNotBeCalled } from '../../should-not-be-called';
import { arbitraryDoi } from '../../types/doi.helper';
import { arbitraryGroupId } from '../../types/group-id.helper';
import { arbitraryGroup } from '../../types/group.helper';
import { arbitraryReviewId } from '../../types/review-id.helper';

describe('group-evaluated-article-card', () => {
  describe('when the article details cannot be fetched', () => {
    it('returns a valid card', async () => {
      const card = await pipe(
        groupEvaluatedArticle(arbitraryGroupId(), arbitraryDoi(), arbitraryReviewId()),
        groupEvaluatedArticleCard(
          () => TO.some(arbitraryGroup()),
          () => TE.left(DE.unavailable),
        ),
        TE.getOrElseW(shouldNotBeCalled),
      )();

      expect(card).toContain('evaluated an article');
    });

    it('contains no article details', async () => {
      const card = await pipe(
        groupEvaluatedArticle(arbitraryGroupId(), arbitraryDoi(), arbitraryReviewId()),
        groupEvaluatedArticleCard(
          () => TO.some(arbitraryGroup()),
          () => TE.left(DE.unavailable),
        ),
        TE.getOrElseW(shouldNotBeCalled),
      )();

      expect(card).not.toContain('sciety-feed-card__details');
    });
  });
});
