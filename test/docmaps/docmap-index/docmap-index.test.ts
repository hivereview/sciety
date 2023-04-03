import { URL } from 'url';
import * as O from 'fp-ts/Option';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import * as TO from 'fp-ts/TaskOption';
import { StatusCodes } from 'http-status-codes';
import { docmapIndex } from '../../../src/docmaps/docmap-index';
import { evaluationRecorded, groupJoined } from '../../../src/domain-events';
import * as DE from '../../../src/types/data-error';
import * as GID from '../../../src/types/group-id';
import {
  arbitraryDate, arbitraryString, arbitraryUri, arbitraryWord,
} from '../../helpers';
import { shouldNotBeCalled } from '../../should-not-be-called';
import { arbitraryArticleId } from '../../types/article-id.helper';
import { arbitraryArticleServer } from '../../types/article-server.helper';
import { arbitraryGroup } from '../../types/group.helper';
import { arbitraryReviewId } from '../../types/review-id.helper';
import { arbitraryDescriptionPath } from '../../types/description-path.helper';
import { Ports } from '../../../src/docmaps/docmap-index/docmap-index';

describe('docmap-index', () => {
  const ncrcGroupId = GID.fromValidatedString('62f9b0d0-8d43-4766-a52a-ce02af61bc6a');
  type DocmapIndexBody = {
    articles?: ReadonlyArray<unknown>,
    error?: string,
  };

  describe('when all ports work', () => {
    describe('and there are no docmaps', () => {
      let response: { body: DocmapIndexBody, status: StatusCodes };

      beforeEach(async () => {
        const ports: Ports = {
          getAllEvents: T.of([]),
          fetchReview: shouldNotBeCalled,
          findVersionsForArticleDoi: shouldNotBeCalled,
          fetchArticle: shouldNotBeCalled,
          getGroup: shouldNotBeCalled,
        };
        response = await docmapIndex(ports)({})();
      });

      it('return an empty list in the articles field', async () => {
        expect(response.body).toStrictEqual({ articles: [] });
      });

      it('return a 200 status code', () => {
        expect(response.status).toStrictEqual(StatusCodes.OK);
      });
    });

    describe('when there are docmaps', () => {
      let response: { body: DocmapIndexBody, status: StatusCodes };

      beforeEach(async () => {
        const ports: Ports = {
          getGroup: () => O.some({
            ...arbitraryGroup(),
            id: ncrcGroupId,
          }),
          getAllEvents: T.of([
            groupJoined(
              ncrcGroupId,
              arbitraryString(),
              arbitraryWord(),
              arbitraryDescriptionPath(),
              arbitraryString(),
              arbitraryUri(),
              arbitraryWord(),
            ),
            evaluationRecorded(ncrcGroupId, arbitraryArticleId(), arbitraryReviewId()),
          ]),
          fetchReview: () => TE.right({ url: new URL(arbitraryUri()) }),
          findVersionsForArticleDoi: () => TO.some([
            {
              source: new URL(arbitraryUri()),
              publishedAt: arbitraryDate(),
              version: 1,
            },
          ]),
          fetchArticle: () => TE.right({ server: arbitraryArticleServer() }),
        };
        response = await docmapIndex(ports)({})();
      });

      it('return them as a list in the articles field', () => {
        expect(response.body.articles).toHaveLength(1);
      });

      it('return a 200 status code', () => {
        expect(response.status).toStrictEqual(StatusCodes.OK);
      });
    });
  });

  describe('when any docmap fails to generate', () => {
    let response: { body: DocmapIndexBody, status: StatusCodes };

    beforeEach(async () => {
      const ports: Ports = {
        getAllEvents: T.of([
          evaluationRecorded(ncrcGroupId, arbitraryArticleId(), arbitraryReviewId()),
        ]),
        fetchReview: () => TE.left(DE.unavailable),
        findVersionsForArticleDoi: () => TO.none,
        fetchArticle: () => TE.left(DE.unavailable),
        getGroup: () => O.none,
      };
      response = await docmapIndex(ports)({})();
    });

    it('returns a body containing an error object', () => {
      expect(response.body).toStrictEqual({
        error: 'Internal server error while generating Docmaps',
      });
    });

    it('returns a 500 status code', () => {
      expect(response.status).toStrictEqual(StatusCodes.INTERNAL_SERVER_ERROR);
    });
  });

  describe('when the query parameters are invalid', () => {
    let response: { body: DocmapIndexBody, status: StatusCodes };

    beforeEach(async () => {
      const ports: Ports = {
        getAllEvents: T.of([]),
        fetchReview: shouldNotBeCalled,
        findVersionsForArticleDoi: shouldNotBeCalled,
        getGroup: shouldNotBeCalled,
        fetchArticle: shouldNotBeCalled,
      };
      response = await docmapIndex(ports)({
        updatedAfter: 'not-a-date',
      })();
    });

    it('returns a body containing an error object', () => {
      expect(response.body.error).toMatch(/^Invalid value "not-a-date"/);
    });

    it('returns a 400 status code', () => {
      expect(response.status).toStrictEqual(StatusCodes.BAD_REQUEST);
    });
  });
});
