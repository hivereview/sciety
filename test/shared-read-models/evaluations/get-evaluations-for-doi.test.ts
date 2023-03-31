import * as RA from 'fp-ts/ReadonlyArray';
import { pipe } from 'fp-ts/function';
import { evaluationRecorded } from '../../../src/domain-events';
import { arbitraryDoi } from '../../types/doi.helper';
import { arbitraryGroupId } from '../../types/group-id.helper';
import { arbitraryReviewId } from '../../types/review-id.helper';
import { handleEvent, initialState } from '../../../src/shared-read-models/evaluations';
import { getEvaluationsForDoi } from '../../../src/shared-read-models/evaluations/get-evaluations-for-doi';

describe('get-evaluations-for-doi', () => {
  const article1 = arbitraryDoi();
  const article2 = arbitraryDoi();
  const group1 = arbitraryGroupId();
  const group2 = arbitraryGroupId();
  const reviewId1 = arbitraryReviewId();
  const reviewId2 = arbitraryReviewId();
  const reviewId3 = arbitraryReviewId();

  it.each([
    // ['two evaluations', article1, [reviewId1, reviewId3]],
    // ['one evaluation', article2, [reviewId2]],
    ['no evaluations', arbitraryDoi(), []],
  ])('finds the correct evaluations when the article has %s', async (_, articleDoi, expectedEvaluations) => {
    const readmodel = pipe(
      [
        evaluationRecorded(group1, article1, reviewId1, [], new Date(), new Date('2020-05-19T00:00:00Z')),
        evaluationRecorded(group1, article2, reviewId2, [], new Date(), new Date('2020-05-21T00:00:00Z')),
        evaluationRecorded(group2, article1, reviewId3, [], new Date(), new Date('2020-05-20T00:00:00Z')),
      ],
      RA.reduce(initialState(), handleEvent),
    );
    const actualEvaluations = pipe(
      articleDoi,
      getEvaluationsForDoi(readmodel),
      RA.map((evaluation) => evaluation.reviewId),
    );

    expect(actualEvaluations).toStrictEqual(expectedEvaluations);
  });
});
