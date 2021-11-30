import * as E from 'fp-ts/Either';
import * as RA from 'fp-ts/ReadonlyArray';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { StatusCodes } from 'http-status-codes';
import {
  DocmapIndexEntryModel,
  identifyAllPossibleIndexEntries,
  Ports,
} from '../../../src/docmaps/docmap-index/identify-all-possible-index-entries';
import { publisherAccountId } from '../../../src/docmaps/docmap/publisher-account-id';
import { evaluationRecorded, groupCreated } from '../../../src/domain-events';
import { shouldNotBeCalled } from '../../should-not-be-called';
import { arbitraryDoi } from '../../types/doi.helper';
import { arbitraryGroupId } from '../../types/group-id.helper';
import { arbitraryGroup } from '../../types/group.helper';
import { arbitraryReviewId } from '../../types/review-id.helper';

describe('identify-all-possible-index-entries', () => {
  const supportedGroups = [arbitraryGroup(), arbitraryGroup()];
  const supportedGroupIds = supportedGroups.map((group) => group.id);
  const defaultPorts: Ports = {
    getAllEvents: pipe(
      supportedGroups,
      RA.map(groupCreated),
      T.of,
    ),
  };

  describe('when there are evaluated events by a supported group', () => {
    const articleId1 = arbitraryDoi();
    const articleId2 = arbitraryDoi();
    const earlierDate = new Date('1990');
    const laterDate = new Date('2000');
    const events = [
      evaluationRecorded(supportedGroupIds[0], articleId1, arbitraryReviewId(), earlierDate),
      evaluationRecorded(supportedGroupIds[0], articleId2, arbitraryReviewId(), laterDate),
    ];
    let result: ReadonlyArray<DocmapIndexEntryModel>;

    beforeEach(async () => {
      result = await pipe(
        events,
        identifyAllPossibleIndexEntries(supportedGroupIds, defaultPorts),
        TE.getOrElse(shouldNotBeCalled),
      )();
    });

    it('returns a list of all the evaluated index entry models', () => {
      expect(result).toStrictEqual([
        {
          articleId: articleId2,
          groupId: supportedGroupIds[0],
          updated: laterDate,
          publisherAccountId: publisherAccountId(supportedGroups[0]),
        },
        {
          articleId: articleId1,
          groupId: supportedGroupIds[0],
          updated: earlierDate,
          publisherAccountId: publisherAccountId(supportedGroups[0]),
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
      evaluationRecorded(supportedGroupIds[0], articleId, arbitraryReviewId(), earlierDate),
      evaluationRecorded(supportedGroupIds[0], articleId, arbitraryReviewId(), latestDate),
      evaluationRecorded(supportedGroupIds[0], articleId, arbitraryReviewId(), middleDate),
    ];

    let result: ReadonlyArray<DocmapIndexEntryModel>;

    beforeEach(async () => {
      result = await pipe(
        events,
        identifyAllPossibleIndexEntries(supportedGroupIds, defaultPorts),
        TE.getOrElse(shouldNotBeCalled),
      )();
    });

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
    const articleId1 = arbitraryDoi();
    const articleId2 = arbitraryDoi();
    const events = [
      evaluationRecorded(supportedGroupIds[0], articleId1, arbitraryReviewId()),
      evaluationRecorded(supportedGroupIds[1], articleId2, arbitraryReviewId()),
      evaluationRecorded(arbitraryGroupId(), arbitraryDoi(), arbitraryReviewId()),
    ];

    let result: ReadonlyArray<DocmapIndexEntryModel>;

    beforeEach(async () => {
      result = await pipe(
        events,
        identifyAllPossibleIndexEntries(supportedGroupIds, defaultPorts),
        TE.getOrElse(shouldNotBeCalled),
      )();
    });

    it('excludes articles evaluated by the unsupported group', () => {
      expect(result).toHaveLength(2);
      expect(result).toStrictEqual(expect.arrayContaining([
        expect.objectContaining({
          groupId: supportedGroupIds[0],
          articleId: articleId1,
          publisherAccountId: publisherAccountId(supportedGroups[0]),
        }),
        expect.objectContaining({
          groupId: supportedGroupIds[1],
          articleId: articleId2,
          publisherAccountId: publisherAccountId(supportedGroups[1]),
        }),
      ]));
    });
  });

  describe('when a supported group cannot be fetched', () => {
    const events = [
      evaluationRecorded(supportedGroupIds[0], arbitraryDoi(), arbitraryReviewId()),
    ];
    let result: unknown;

    beforeEach(async () => {
      result = await pipe(
        events,
        identifyAllPossibleIndexEntries(
          supportedGroupIds,
          {
            ...defaultPorts,
            getAllEvents: T.of([]),
          },
        ),
      )();
    });

    it('fails with an internal server error', () => {
      expect(result).toStrictEqual(E.left(expect.objectContaining({
        status: StatusCodes.INTERNAL_SERVER_ERROR,
      })));
    });
  });
});
