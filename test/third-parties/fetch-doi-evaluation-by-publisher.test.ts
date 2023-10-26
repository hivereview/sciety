import * as E from 'fp-ts/Either';
import * as TE from 'fp-ts/TaskEither';
import { URL } from 'url';
import * as DE from '../../src/types/data-error';
import { fetchDoiEvaluationByPublisher } from '../../src/third-parties/fetch-doi-evaluation-by-publisher';
import { Evaluation } from '../../src/types/evaluation';
import { arbitraryNumber, arbitrarySanitisedHtmlFragment, arbitraryUri } from '../helpers';

const arbitraryEvaluation: Evaluation = {
  fullText: arbitrarySanitisedHtmlFragment(),
  url: new URL(arbitraryUri()),
};
const arbitraryDoiPrefix = () => `10.${arbitraryNumber(1, 9999)}`;

describe('fetch-doi-evaluation-by-publisher', () => {
  let result: E.Either<DE.DataError, Evaluation>;

  describe('when a doi with a configured prefix is passed in', () => {
    describe('when the delegated doi fetcher returns a right', () => {
      const configuredDoiPrefix = arbitraryDoiPrefix();
      const evaluationFetchersConfiguration = {
        [configuredDoiPrefix]: () => TE.right(arbitraryEvaluation),
      };

      beforeEach(async () => {
        result = await fetchDoiEvaluationByPublisher(evaluationFetchersConfiguration)(`${configuredDoiPrefix}/123`)();
      });

      it('returns a right', () => {
        expect(result).toStrictEqual(E.right(arbitraryEvaluation));
      });
    });

    describe('when the delegated doi fetcher returns a left', () => {
      it.todo('returns a left');
    });
  });

  describe('when a doi with an unknown prefix is passed in', () => {
    const unknownDoiPrefix = arbitraryDoiPrefix();
    const evaluationFetchersConfiguration = {
      [arbitraryDoiPrefix()]: () => TE.right(arbitraryEvaluation),
    };

    beforeEach(async () => {
      result = await fetchDoiEvaluationByPublisher(evaluationFetchersConfiguration)(`${unknownDoiPrefix}/123`)();
    });

    it('returns unavailable', () => {
      expect(result).toStrictEqual(E.left(DE.unavailable));
    });
  });
});
