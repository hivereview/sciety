import { Doi } from '../../src/types/doi';
import { HypothesisAnnotationId } from '../../src/types/hypothesis-annotation-id';
import { ReviewId, toReviewId } from '../../src/types/review-id';

describe('review-id', () => {
  describe('when is a DOI', () => {
    it('can be serialized and deserialized', () => {
      const doiValue = '10.1111/12345678';
      const reviewId: ReviewId = new Doi(doiValue);
      const serialization = reviewId.toString();
      const deserialized = toReviewId(serialization);

      expect(deserialized).toStrictEqual(reviewId);
    });
  });

  describe('when is a Hypothesis annotation id', () => {
    it('can be serialized and deserialized', () => {
      const hypothesisValue = 'GFEW8JXMEeqJQcuc-6NFhQ';
      const reviewId: ReviewId = new HypothesisAnnotationId(hypothesisValue);
      const serialization = reviewId.toString();
      const deserialized = toReviewId(serialization);

      expect(deserialized).toStrictEqual(reviewId);
    });
  });
});
