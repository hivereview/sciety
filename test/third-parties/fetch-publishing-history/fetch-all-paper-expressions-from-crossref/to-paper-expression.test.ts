import * as E from 'fp-ts/Either';
import { pipe } from 'fp-ts/function';
import { CrossrefWork } from '../../../../src/third-parties/fetch-publishing-history/fetch-all-paper-expressions-from-crossref/crossref-work';
import { toPaperExpression } from '../../../../src/third-parties/fetch-publishing-history/fetch-all-paper-expressions-from-crossref/to-paper-expression';
import { arbitraryUri } from '../../../helpers';
import { shouldNotBeCalled } from '../../../should-not-be-called';
import { arbitraryExpressionDoi } from '../../../types/expression-doi.helper';

describe('to-paper-expression', () => {
  describe('when the Crossref work is of type posted-content', () => {
    const crossrefWork: CrossrefWork = {
      type: 'posted-content',
      DOI: arbitraryExpressionDoi(),
      posted: { 'date-parts': [[2021, 10, 3]] },
      resource: { primary: { URL: arbitraryUri() } },
      relation: { },
    };
    const expressionType = pipe(
      crossrefWork,
      toPaperExpression,
      E.getOrElseW(shouldNotBeCalled),
      (expression) => expression.expressionType,
    );

    it('returns an expression of type preprint', () => {
      expect(expressionType).toBe('preprint');
    });
  });

  describe('when the Crossref work is of type journal-article', () => {
    const crossrefWork: CrossrefWork = {
      type: 'journal-article',
      DOI: arbitraryExpressionDoi(),
      published: { 'date-parts': [[2021, 10, 3]] },
      resource: { primary: { URL: arbitraryUri() } },
      relation: { },
    };
    const expressionType = pipe(
      crossrefWork,
      toPaperExpression,
      E.getOrElseW(shouldNotBeCalled),
      (expression) => expression.expressionType,
    );

    it('returns an expression of type journal-article', () => {
      expect(expressionType).toBe('journal-article');
    });
  });

  describe('when the Crossref work is of unknown type', () => {
    const crossrefWork: CrossrefWork = {
      type: 'other',
      DOI: arbitraryExpressionDoi(),
      relation: { },
    };
    const result = toPaperExpression(crossrefWork);

    it('rejects the work', () => {
      expect(E.isLeft(result)).toBe(true);
    });
  });
});
