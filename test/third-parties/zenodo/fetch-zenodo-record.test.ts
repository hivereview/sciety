import { URL } from 'url';
import * as TE from 'fp-ts/TaskEither';
import * as E from 'fp-ts/Either';
import { pipe } from 'fp-ts/function';
import { fetchZenodoRecord } from '../../../src/third-parties/zenodo/fetch-zenodo-record';
import * as DE from '../../../src/types/data-error';
import { arbitraryHtmlFragment } from '../../helpers';
import { shouldNotBeCalled } from '../../should-not-be-called';

const notZenodoKey = '10.1234/zenodo/123';
const zenodoKey = '10.5281/zenodo.6386692';
const unexpectedSuffixZenodoKeyMostComplex = '10.5281/somethingelse.123';
const doiUrl = 'https://doi.org/10.5281/zenodo.6386692';

describe('fetch-zenodo-record', () => {
  describe('when the DOI is from Zenodo', () => {
    describe('and the DOI suffix has an unexpected format', () => {
      const queryExternalService = shouldNotBeCalled;
      let evaluation: E.Either<unknown, unknown>;

      beforeEach(async () => {
        evaluation = await fetchZenodoRecord(queryExternalService)(unexpectedSuffixZenodoKeyMostComplex)();
      });

      it('returns a left', () => {
        expect(evaluation).toStrictEqual(E.left(DE.unavailable));
      });
    });

    describe('when the request succeeds', () => {
      const description = arbitraryHtmlFragment();
      const queryExternalService = () => TE.right({
        metadata: {
          description,
        },
      });

      it('returns the metadata description as full text', async () => {
        const evaluation = await fetchZenodoRecord(queryExternalService)(zenodoKey)();

        expect(
          pipe(
            evaluation,
            E.map((ev) => ev.fullText),
          ),
        ).toStrictEqual(E.right(description));
      });

      it('returns the Doi.org url as url', async () => {
        const evaluation = await fetchZenodoRecord(queryExternalService)(zenodoKey)();

        expect(
          pipe(
            evaluation,
            E.map((ev) => ev.url),
          ),
        ).toStrictEqual(E.right(new URL(doiUrl)));
      });
    });

    describe('when the request fails', () => {
      const queryExternalService = () => TE.left(DE.unavailable);
      let evaluation: E.Either<unknown, unknown>;

      beforeEach(async () => {
        evaluation = await fetchZenodoRecord(queryExternalService)(zenodoKey)();
      });

      it('returns a left', () => {
        expect(evaluation).toStrictEqual(E.left(DE.unavailable));
      });
    });

    describe('when the returned JSON value is unexpected', () => {
      const wrongProperty = arbitraryHtmlFragment();
      const queryExternalService = () => TE.right({
        metadata: {
          wrongProperty,
        },
      });
      let evaluation: E.Either<unknown, unknown>;

      beforeEach(async () => {
        evaluation = await fetchZenodoRecord(queryExternalService)(zenodoKey)();
      });

      it('returns a left', () => {
        expect(evaluation).toStrictEqual(E.left(DE.unavailable));
      });
    });
  });

  describe('when the DOI is not from Zenodo', () => {
    let evaluation: E.Either<unknown, unknown>;
    const queryExternalService = shouldNotBeCalled;

    beforeEach(async () => {
      evaluation = await fetchZenodoRecord(queryExternalService)(notZenodoKey)();
    });

    it('returns a left', () => {
      expect(evaluation).toStrictEqual(E.left(DE.unavailable));
    });
  });
});
