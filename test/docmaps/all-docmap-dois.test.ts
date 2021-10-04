import { allDocmapDois } from '../../src/docmaps/all-docmap-dois';
import { groupEvaluatedArticle } from '../../src/domain-events';
import * as GID from '../../src/types/group-id';
import { arbitraryDate } from '../helpers';
import { arbitraryDoi } from '../types/doi.helper';
import { arbitraryGroupId } from '../types/group-id.helper';
import { arbitraryReviewId } from '../types/review-id.helper';

describe('all-docmap-dois', () => {
  const ncrcGroupId = GID.fromValidatedString('62f9b0d0-8d43-4766-a52a-ce02af61bc6a');

  describe('when there are evaluated events by NCRC', () => {
    it('returns a list of all the evaluated article ids', () => {
      const articleId1 = arbitraryDoi();
      const articleId2 = arbitraryDoi();
      const date1 = arbitraryDate();
      const date2 = arbitraryDate();
      const events = [
        groupEvaluatedArticle(ncrcGroupId, articleId1, arbitraryReviewId(), date1),
        groupEvaluatedArticle(ncrcGroupId, articleId2, arbitraryReviewId(), date2),
      ];

      const dois = allDocmapDois(events);

      expect(dois).toStrictEqual([
        {
          articleId: articleId1,
          groupId: ncrcGroupId,
          updated: date1,
        },
        {
          articleId: articleId2,
          groupId: ncrcGroupId,
          updated: date2,
        },
      ]);
    });
  });

  describe('when there are evaluated events by NCRC and another group', () => {
    it('returns a list of article ids evaluated by NCRC', () => {
      const articleId1 = arbitraryDoi();
      const events = [
        groupEvaluatedArticle(ncrcGroupId, articleId1, arbitraryReviewId()),
        groupEvaluatedArticle(arbitraryGroupId(), arbitraryDoi(), arbitraryReviewId()),
      ];

      const dois = allDocmapDois(events);

      expect(dois).toStrictEqual([
        expect.objectContaining({
          articleId: articleId1,
        }),
      ]);
    });
  });
});
