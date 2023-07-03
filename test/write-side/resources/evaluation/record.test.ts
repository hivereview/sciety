import { pipe } from 'fp-ts/function';
import * as E from 'fp-ts/Either';
import { record } from '../../../../src/write-side/resources/evaluation/record';
import { arbitraryDate, arbitraryString } from '../../../helpers';
import { arbitraryArticleId } from '../../../types/article-id.helper';
import { arbitraryGroupId } from '../../../types/group-id.helper';
import { arbitraryEvaluationLocator } from '../../../types/evaluation-locator.helper';
import { evaluationRecordedHelper } from '../../../types/evaluation-recorded-event.helper';

const arbitraryEvaluationType = () => 'curation-statement';

describe('record', () => {
  const evaluationLocator = arbitraryEvaluationLocator();
  const input = {
    groupId: arbitraryGroupId(),
    articleId: arbitraryArticleId(),
    evaluationLocator,
    publishedAt: arbitraryDate(),
    authors: [arbitraryString(), arbitraryString()],
    type: arbitraryEvaluationType(),
  };

  describe('when the evaluation locator has NOT already been recorded', () => {
    const events = pipe(
      [],
      record(input),
    );

    it.failing('returns an EvaluationRecorded event', () => {
      expect(events).toStrictEqual(E.right([expect.objectContaining({
        type: 'EvaluationRecorded',
        groupId: input.groupId,
        articleId: input.articleId,
        evaluationLocator: input.evaluationLocator,
        publishedAt: input.publishedAt,
        authors: input.authors,
        evaluationType: input.type,
      })]));
    });
  });

  describe('when the evaluation locator has already been recorded', () => {
    const events = pipe(
      [
        evaluationRecordedHelper(arbitraryGroupId(), arbitraryArticleId(), evaluationLocator, [], arbitraryDate()),
      ],
      record(input),
    );

    it('returns no events', () => {
      expect(events).toStrictEqual(E.right([]));
    });
  });
});
