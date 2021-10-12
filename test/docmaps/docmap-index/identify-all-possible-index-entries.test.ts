import { identifyAllPossibleIndexEntries } from '../../../src/docmaps/docmap-index/identify-all-possible-index-entries';
import { groupEvaluatedArticle } from '../../../src/domain-events';
import { arbitraryDoi } from '../../types/doi.helper';
import { arbitraryGroupId } from '../../types/group-id.helper';
import { arbitraryReviewId } from '../../types/review-id.helper';

describe('identify-all-possible-index-entries', () => {
  const supportedGroupIds = [arbitraryGroupId(), arbitraryGroupId()];

  describe('when there are evaluated events by a supported group', () => {
    it('returns a list of all the evaluated index entry models', () => {
      const articleId1 = arbitraryDoi();
      const articleId2 = arbitraryDoi();
      const earlierDate = new Date('1990');
      const laterDate = new Date('2000');
      const events = [
        groupEvaluatedArticle(supportedGroupIds[0], articleId1, arbitraryReviewId(), earlierDate),
        groupEvaluatedArticle(supportedGroupIds[0], articleId2, arbitraryReviewId(), laterDate),
      ];

      const dois = identifyAllPossibleIndexEntries(supportedGroupIds)(events);

      expect(dois).toStrictEqual([
        {
          articleId: articleId2,
          groupId: supportedGroupIds[0],
          updated: laterDate,
        },
        {
          articleId: articleId1,
          groupId: supportedGroupIds[0],
          updated: earlierDate,
        },
      ]);
    });
  });

  describe('when a supported group has evaluated an article multiple times', () => {
    const earlierDate = new Date('1990');
    const middleDate = new Date('2012');
    const latestDate = new Date('2021');
    const articleId = arbitraryDoi();
    const events = [
      groupEvaluatedArticle(supportedGroupIds[0], articleId, arbitraryReviewId(), earlierDate),
      groupEvaluatedArticle(supportedGroupIds[0], articleId, arbitraryReviewId(), latestDate),
      groupEvaluatedArticle(supportedGroupIds[0], articleId, arbitraryReviewId(), middleDate),
    ];

    const result = identifyAllPossibleIndexEntries(supportedGroupIds)(events);

    it('returns a single index entry model', () => {
      expect(result).toHaveLength(1);
    });

    it('returns the latest updated date', () => {
      expect(result).toStrictEqual([
        expect.objectContaining({
          updated: latestDate,
        }),
      ]);
    });
  });

  describe('when there are evaluated events by both supported and unsupported groups', () => {
    it('excludes articles evaluated by the unsupported group', () => {
      const articleId1 = arbitraryDoi();
      const articleId2 = arbitraryDoi();
      const events = [
        groupEvaluatedArticle(supportedGroupIds[0], articleId1, arbitraryReviewId()),
        groupEvaluatedArticle(supportedGroupIds[1], articleId2, arbitraryReviewId()),
        groupEvaluatedArticle(arbitraryGroupId(), arbitraryDoi(), arbitraryReviewId()),
      ];

      const dois = identifyAllPossibleIndexEntries(supportedGroupIds)(events);

      expect(dois).toHaveLength(2);
      expect(dois).toStrictEqual(expect.arrayContaining([
        expect.objectContaining({
          groupId: supportedGroupIds[0],
          articleId: articleId1,
        }),
        expect.objectContaining({
          groupId: supportedGroupIds[1],
          articleId: articleId2,
        }),
      ]));
    });
  });
});
