import * as O from 'fp-ts/Option';
import * as R from 'fp-ts/Record';
import { pipe } from 'fp-ts/function';
import { groupEvaluatedArticle } from '../../src/domain-events';
import { constructAllArticleActivityReadModel } from '../../src/shared-read-models/construct-all-article-activity-read-model';
import { arbitraryDoi } from '../types/doi.helper';
import { arbitraryGroupId } from '../types/group-id.helper';
import { arbitraryReviewId } from '../types/review-id.helper';

describe('construct-all-article-activity-read-model', () => {
  const articleId = arbitraryDoi();

  describe('when an article has no evaluations', () => {
    const articleActivity = pipe(
      [],
      constructAllArticleActivityReadModel,
      R.lookup(articleId.value),
    );

    it('article is not in the read model', () => {
      expect(O.isNone(articleActivity)).toBe(true);
    });
  });

  describe('when an article has one or more evaluations', () => {
    const earlierDate = new Date(1900);
    const laterDate = new Date(2000);
    const articleActivity = pipe(
      [
        groupEvaluatedArticle(arbitraryGroupId(), articleId, arbitraryReviewId(), earlierDate),
        groupEvaluatedArticle(arbitraryGroupId(), articleId, arbitraryReviewId(), laterDate),
      ],
      constructAllArticleActivityReadModel,
      R.lookup(articleId.value),
    );

    it.skip('returns the activity for that article', () => {
      expect(articleActivity).toStrictEqual(O.some({
        doi: articleId,
        latestActivityDate: laterDate,
        evaluationCount: 2,
      }));
    });
  });
});
