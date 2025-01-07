import * as E from 'fp-ts/Either';
import * as RA from 'fp-ts/ReadonlyArray';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { CrossrefWork, crossrefWorkCodec, PostedContent } from '../../../../src/third-parties/fetch-publishing-history/fetch-all-paper-expressions-from-crossref/crossref-work';
import { QueryCrossrefService } from '../../../../src/third-parties/fetch-publishing-history/fetch-all-paper-expressions-from-crossref/query-crossref-service';
import { State } from '../../../../src/third-parties/fetch-publishing-history/fetch-all-paper-expressions-from-crossref/state';
import { walkRelationGraph } from '../../../../src/third-parties/fetch-publishing-history/fetch-all-paper-expressions-from-crossref/walk-relation-graph';
import * as DE from '../../../../src/types/data-error';
import * as EDOI from '../../../../src/types/expression-doi';
import { dummyLogger } from '../../../dummy-logger';
import { arbitraryString, arbitraryUri } from '../../../helpers';
import { shouldNotBeCalled } from '../../../should-not-be-called';
import { arbitraryExpressionDoi } from '../../../types/expression-doi.helper';

const arbitraryPostedContentCrossrefWork = (): PostedContent => ({
  type: 'posted-content',
  DOI: arbitraryExpressionDoi(),
  posted: { 'date-parts': [[2021, 10, 3]] },
  resource: { primary: { URL: arbitraryUri() } },
  relation: { },
});

const mockQueryCrossrefService = (createWork: () => CrossrefWork) => (url: string) => {
  if (url.includes('filter')) {
    const stubbedResponseForFetchWorksThatPointToIndividualWork = { message: { items: [] } };
    return TE.right(stubbedResponseForFetchWorksThatPointToIndividualWork);
  }
  const stubbedResponseForFetchIndividualWork = {
    message: crossrefWorkCodec.encode(createWork()),
  };
  return TE.right(stubbedResponseForFetchIndividualWork);
};

describe('walk-relation-graph', () => {
  let queryCrossrefService: QueryCrossrefService;
  const executeWalkRelationGraph = async (state: State) => walkRelationGraph(
    queryCrossrefService,
    dummyLogger,
    arbitraryExpressionDoi(),
  )(state)();
  let result: E.Either<DE.DataError, ReadonlyArray<CrossrefWork>>;

  describe('if the queue is empty', () => {
    const crossrefWork = arbitraryPostedContentCrossrefWork();
    const state: State = {
      queue: [],
      collectedWorks: new Map([
        [arbitraryExpressionDoi(), crossrefWork],
      ]),
    };

    beforeEach(async () => {
      queryCrossrefService = shouldNotBeCalled;
      result = await executeWalkRelationGraph(state);
    });

    it('returns the current collected works on the right', () => {
      expect(result).toStrictEqual(E.right([crossrefWork]));
    });
  });

  describe('if the queue is not empty', () => {
    describe('if there are currently more than 20 collected works', () => {
      const crossrefWorks = Array.from({ length: 21 }, arbitraryPostedContentCrossrefWork);
      const state: State = {
        queue: [arbitraryString()],
        collectedWorks: pipe(
          crossrefWorks,
          RA.map((work) => [work.DOI, work] as const),
          (entries) => new Map(entries),
        ),
      };

      beforeEach(async () => {
        queryCrossrefService = shouldNotBeCalled;
        result = await executeWalkRelationGraph(state);
      });

      it('returns the current collected works on the right', () => {
        expect(result).toStrictEqual(E.right(crossrefWorks));
      });
    });

    describe('if there are currently 20 or fewer collected works', () => {
      describe('when there is one currently collected work', () => {
        const relatedWork = arbitraryPostedContentCrossrefWork();
        const previouslyKnownWork: CrossrefWork = {
          ...arbitraryPostedContentCrossrefWork(),
          relation: {
            'has-version': [
              {
                'id-type': 'doi',
                id: relatedWork.DOI,
              },
            ],
          },
        };

        describe('if one more related CrossrefWork is retrieved by looking up the queue', () => {
          const state: State = {
            queue: [relatedWork.DOI],
            collectedWorks: new Map([
              [previouslyKnownWork.DOI, previouslyKnownWork],
            ]),
          };

          beforeEach(async () => {
            queryCrossrefService = jest.fn(mockQueryCrossrefService(() => relatedWork));
            result = await executeWalkRelationGraph(state);
          });

          it('returns two collected works on the right', () => {
            expect(result).toStrictEqual(E.right([previouslyKnownWork, relatedWork]));
          });
        });
      });
    });

    describe('if the crossref graph is too big', () => {
      const state: State = {
        queue: [arbitraryExpressionDoi()],
        collectedWorks: new Map(),
      };

      beforeEach(async () => {
        const createWorkWithArbitraryRelation = (): CrossrefWork => ({
          ...arbitraryPostedContentCrossrefWork(),
          relation: {
            'has-version': [
              {
                'id-type': 'doi',
                id: arbitraryExpressionDoi(),
              },
            ],
          },
        });
        queryCrossrefService = jest.fn(mockQueryCrossrefService(createWorkWithArbitraryRelation));
        result = await executeWalkRelationGraph(state);
      });

      it('returns early', () => {
        expect(result).toStrictEqual(expect.anything());
      });
    });

    describe.skip('if the queue keeps getting populated with a discovered relation that cannot be fetched', () => {
      const state: State = {
        queue: ['10.1101/initialqueueitem'],
        collectedWorks: new Map(),
      };
      const arbitraryWorkWithArbitraryRelation: CrossrefWork = {
        ...arbitraryPostedContentCrossrefWork(),
        DOI: EDOI.fromValidatedString('10.1101/discoveredwork'),
        relation: {
          'has-version': [
            {
              'id-type': 'doi',
              id: EDOI.fromValidatedString('10.1101/discoveredrelation'),
            },
          ],
        },
      };

      beforeEach(async () => {
        queryCrossrefService = jest.fn(mockQueryCrossrefService(() => arbitraryWorkWithArbitraryRelation));
        result = await executeWalkRelationGraph(state);
      });

      it('returns early', () => {
        expect(result).toStrictEqual(expect.anything());
      });
    });
  });
});
