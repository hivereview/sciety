import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as T from 'fp-ts/Task';
import { pipe } from 'fp-ts/function';
import { JSDOM } from 'jsdom';
import {
  articleAddedToList,
  groupJoined, listCreated, userFollowedEditorialCommunity,
  userFoundReviewHelpful,
  userFoundReviewNotHelpful,
  userRevokedFindingReviewHelpful,
  userRevokedFindingReviewNotHelpful,
  userSavedArticle,
  userUnfollowedEditorialCommunity,
  userUnsavedArticle,
} from '../../src/domain-events';
import { Ports, scietyFeedPage } from '../../src/sciety-feed-page/sciety-feed-page';
import * as LOID from '../../src/types/list-owner-id';
import { dummyLogger } from '../dummy-logger';
import { arbitraryString, arbitraryUri, arbitraryWord } from '../helpers';
import { shouldNotBeCalled } from '../should-not-be-called';
import { arbitraryArticleId } from '../types/article-id.helper';
import { arbitraryGroupId } from '../types/group-id.helper';
import { arbitraryGroup } from '../types/group.helper';
import { arbitraryList } from '../types/list-helper';
import { arbitraryListId } from '../types/list-id.helper';
import { arbitraryListOwnerId } from '../types/list-owner-id.helper';
import { arbitraryReviewId } from '../types/review-id.helper';
import { arbitraryUserId } from '../types/user-id.helper';

describe('sciety-feed-page', () => {
  const getUser = () => O.some({
    handle: arbitraryWord(),
    avatarUrl: arbitraryUri(),
    id: arbitraryUserId(),
    displayName: arbitraryString(),
  });

  const group = arbitraryGroup();

  const defaultPorts: Ports = {
    getUser,
    getGroup: () => O.some(arbitraryGroup()),
    getList: () => O.some(arbitraryList()),
    logger: dummyLogger,
    getAllEvents: T.of([]),
  };

  it('renders a single article added to a list as a card', async () => {
    const listId = arbitraryListId();
    const ports = {
      ...defaultPorts,
      getAllEvents: T.of([
        listCreated(listId, arbitraryString(), arbitraryString(), arbitraryListOwnerId()),
        articleAddedToList(arbitraryArticleId(), listId),
      ]),
    };
    const renderedPage = await pipe(
      scietyFeedPage(ports)(20)({ page: 1 }),
      T.map(E.getOrElseW(shouldNotBeCalled)),
      T.map((page) => page.content),
    )();

    expect(renderedPage).toContain('added an article to a list');
  });

  it('renders a single user followed editorial community as a card', async () => {
    const ports = {
      ...defaultPorts,
      getAllEvents: T.of([
        groupJoined(
          group.id,
          group.name,
          group.avatarPath,
          group.descriptionPath,
          group.shortDescription,
          group.homepage,
          group.slug,
        ),
        userFollowedEditorialCommunity(arbitraryUserId(), group.id),
      ]),
    };
    const renderedPage = await pipe(
      scietyFeedPage(ports)(20)({ page: 1 }),
      T.map(E.getOrElseW(shouldNotBeCalled)),
      T.map((page) => page.content),
    )();

    expect(renderedPage).toContain('followed a group');
  });

  it('renders at most a page of cards at a time', async () => {
    const events = [
      groupJoined(
        group.id,
        group.name,
        group.avatarPath,
        group.descriptionPath,
        group.shortDescription,
        group.homepage,
        group.slug,
      ),
      userFollowedEditorialCommunity(arbitraryUserId(), group.id),
      userFollowedEditorialCommunity(arbitraryUserId(), group.id),
      userFollowedEditorialCommunity(arbitraryUserId(), group.id),
    ];
    const ports = {
      ...defaultPorts,
      getAllEvents: T.of(events),
    };
    const pageSize = events.length - 1;
    const renderedPage = await pipe(
      scietyFeedPage(ports)(pageSize)({ page: 1 }),
      T.map(E.getOrElseW(shouldNotBeCalled)),
      T.map((page) => page.content),
    )();
    const html = JSDOM.fragment(renderedPage);
    const itemCount = Array.from(html.querySelectorAll('.sciety-feed-card')).length;

    expect(itemCount).toStrictEqual(pageSize);
  });

  it('does not render uninteresting events', async () => {
    const listId = arbitraryListId();
    const ports = {
      ...defaultPorts,
      getAllEvents: T.of([
        groupJoined(
          group.id,
          group.name,
          group.avatarPath,
          group.descriptionPath,
          group.shortDescription,
          group.homepage,
          group.slug,
        ),
        listCreated(listId, arbitraryString(), arbitraryString(), LOID.fromGroupId(group.id)),
        articleAddedToList(arbitraryArticleId(), listId),
        userUnsavedArticle(arbitraryUserId(), arbitraryArticleId()),
        userUnfollowedEditorialCommunity(arbitraryUserId(), arbitraryGroupId()),
        userFoundReviewHelpful(arbitraryUserId(), arbitraryReviewId()),
        userFoundReviewNotHelpful(arbitraryUserId(), arbitraryReviewId()),
        userRevokedFindingReviewHelpful(arbitraryUserId(), arbitraryReviewId()),
        userRevokedFindingReviewNotHelpful(arbitraryUserId(), arbitraryReviewId()),
        userSavedArticle(arbitraryUserId(), arbitraryArticleId()),
      ]),
    };
    const renderedPage = await pipe(
      scietyFeedPage(ports)(10)({ page: 1 }),
      T.map(E.getOrElseW(shouldNotBeCalled)),
      T.map((page) => page.content),
    )();
    const html = JSDOM.fragment(renderedPage);
    const itemCount = Array.from(html.querySelectorAll('.sciety-feed-card')).length;

    expect(itemCount).toBe(1);
  });
});
