import { URL } from 'url';
import * as O from 'fp-ts/Option';
import * as RNEA from 'fp-ts/ReadonlyNonEmptyArray';
import * as E from 'fp-ts/Either';
import { ArticleAuthors } from '../../types/article-authors';
import { ArticleServer } from '../../types/article-server';
import { Doi } from '../../types/doi';
import { HtmlFragment } from '../../types/html-fragment';
import * as RI from '../../types/evaluation-locator';
import { SanitisedHtmlFragment } from '../../types/sanitised-html-fragment';
import { ListId } from '../../types/list-id';
import { ArticleViewModel } from '../../shared-components/article-card';

export type ResponseCounts = {
  helpfulCount: number,
  notHelpfulCount: number,
};

type Responses = {
  counts: ResponseCounts,
  current: O.Option<'helpful' | 'not-helpful'>,
};

export type ReviewFeedItem = {
  type: 'review',
  id: RI.EvaluationLocator,
  source: O.Option<URL>,
  publishedAt: Date,
  groupName: string,
  groupHref: string,
  groupAvatar: string,
  fullText: O.Option<SanitisedHtmlFragment>,
  responses: O.Option<Responses>,
};

export type ArticleVersionFeedItem = {
  type: 'article-version',
  source: URL,
  publishedAt: Date,
  version: number,
  server: ArticleServer,
};

type ArticleVersionErrorFeedItem = {
  type: 'article-version-error',
  server: ArticleServer,
};

export type FeedItem =
  | ReviewFeedItem
  | ArticleVersionFeedItem
  | ArticleVersionErrorFeedItem;

type ListSummary = {
  listName: string,
  listId: ListId,
};

type ArticleNotInAnyList = {
  lists: ReadonlyArray<ListSummary>,
};

type ArticleSavedToThisList = ListSummary;

// ts-unused-exports:disable-next-line
export type LoggedInUserListManagement = E.Either<ArticleNotInAnyList, ArticleSavedToThisList>;

export type ViewModel = {
  doi: Doi,
  title: string,
  titleLanguageCode: string,
  authors: ArticleAuthors,
  fullArticleUrl: string,
  abstract: HtmlFragment,
  evaluationCount: number,
  latestVersion: O.Option<Date>,
  latestActivity: O.Option<Date>,
  feedItemsByDateDescending: RNEA.ReadonlyNonEmptyArray<FeedItem>,
  userListManagement: O.Option<LoggedInUserListManagement>,
  listedIn: ReadonlyArray<{ listId: ListId, listName: string, listOwnerName: string }>,
  relatedArticles: O.Option<ReadonlyArray<ArticleViewModel>>,
};
