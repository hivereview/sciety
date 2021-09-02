import * as O from 'fp-ts/Option';
import * as T from 'fp-ts/Task';
import { pipe } from 'fp-ts/function';
import { generateDocmapIndex } from '../../../src/docmaps/docmap-index/generate-docmap-index';
import { editorialCommunityReviewedArticle } from '../../../src/domain-events';
import * as GID from '../../../src/types/group-id';
import { arbitraryDoi } from '../../types/doi.helper';
import { arbitraryReviewId } from '../../types/review-id.helper';

describe('generate-docmap-index', () => {
  it('includes an absolute url for each docmap in the index', async () => {
    const doi = arbitraryDoi();
    const ncrcGroupId = GID.fromValidatedString('62f9b0d0-8d43-4766-a52a-ce02af61bc6a');
    const result = await pipe(
      { updatedAfter: O.none },
      generateDocmapIndex({
        getAllEvents: T.of([
          editorialCommunityReviewedArticle(ncrcGroupId, doi, arbitraryReviewId()),
        ]),
      }),
      T.map(({ articles }) => articles),
    )();

    expect(result).toStrictEqual(expect.arrayContaining([
      expect.objectContaining({
        docmap: expect.stringMatching(`https://.*${doi.value}\\.docmap\\.json`),
      }),
    ]));
  });

  describe('when no group identifier is supplied', () => {
    it('includes the doi of the hardcoded Review Commons docmap', async () => {
      const result = await pipe(
        { updatedAfter: O.none },
        generateDocmapIndex({
          getAllEvents: T.of([]),
        }),
        T.map(({ articles }) => articles),
      )();

      expect(result).toStrictEqual(expect.arrayContaining([
        expect.objectContaining({
          doi: '10.1101/2021.04.25.441302',
        }),
      ]));
    });

    it.todo('includes urls to the NCRC docmaps');
  });

  describe('when passed a group identifier for NCRC', () => {
    it.todo('only returns urls for NCRC docmaps');
  });

  describe('when passed a group identifier for Review Commons', () => {
    it.todo('only returns urls for Review Commons docmaps');
  });

  describe('when passed anything else as the group argument', () => {
    it.todo('returns an empty index');
  });
});
