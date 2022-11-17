import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import * as R from 'fp-ts/Record';
import { pipe } from 'fp-ts/function';
import {
  elifeGroupId, handleEvent, initialState, ReadModel,
} from '../../../src/add-article-to-elife-subject-area-list/read-model';
import { evaluationRecorded } from '../../../src/domain-events/evaluation-recorded-event';
import { subjectAreaRecorded } from '../../../src/domain-events/subject-area-recorded-event';
import { fromString as doiFromString } from '../../../src/types/doi';
import { arbitraryArticleId } from '../../types/article-id.helper';
import { arbitraryReviewId } from '../../types/review-id.helper';
import { arbitrarySubjectArea } from '../../types/subject-area.helper';

// eslint-disable-next-line @typescript-eslint/no-unused-vars
const getOneArticleIdInEvaluatedAndSubjectAreaKnownState = (readModel: ReadModel) => () => pipe(
  readModel,
  R.filter((item) => item === 'evaluated-and-subject-area-known'),
  R.keys,
  RA.head,
  O.chain(doiFromString),
);

describe('get-one-article-id-in-evaluated-and-subject-area-known-state', () => {
  describe('given a bunch of events', () => {
    const articleIdA = arbitraryArticleId();
    const subjectArea = arbitrarySubjectArea();

    const readModel = pipe(
      [
        evaluationRecorded(elifeGroupId, articleIdA, arbitraryReviewId()),
        subjectAreaRecorded(articleIdA, subjectArea),
      ],
      RA.reduce(initialState(), handleEvent),
    );

    it.failing('returns one article', () => {
      expect(getOneArticleIdInEvaluatedAndSubjectAreaKnownState(readModel)()).toStrictEqual(O.some({
        articleId: articleIdA,
        subjectArea,
      }));
    });
  });
});
