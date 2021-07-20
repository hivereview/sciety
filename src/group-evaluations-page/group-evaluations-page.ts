import { sequenceS } from 'fp-ts/Apply';
import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import * as TO from 'fp-ts/TaskOption';
import { constant, pipe } from 'fp-ts/function';
import * as t from 'io-ts';
import * as tt from 'io-ts-types';
import { evaluatedArticlesList, Ports as EvaluatedArticlesListPorts } from './evaluated-articles-list';
import { evaluatedArticles } from './evaluated-articles-list/evaluated-articles';
import { renderErrorPage, renderPage } from './render-page';
import { getEvaluatedArticlesListDetails } from '../group-page/get-evaluated-articles-list-details';
import { templateDate } from '../shared-components/date';
import { GroupIdFromString } from '../types/codecs/GroupIdFromString';
import * as DE from '../types/data-error';
import { DomainEvent } from '../types/domain-events';
import { Group } from '../types/group';
import { GroupId } from '../types/group-id';
import { toHtmlFragment } from '../types/html-fragment';
import { Page } from '../types/page';
import { RenderPageError } from '../types/render-page-error';

type FetchGroup = (groupId: GroupId) => TO.TaskOption<Group>;

type GetAllEvents = T.Task<ReadonlyArray<DomainEvent>>;

type Ports = EvaluatedArticlesListPorts & {
  getAllEvents: GetAllEvents,
  getGroup: FetchGroup,
};

export const paramsCodec = t.type({
  id: GroupIdFromString,
  page: tt.optionFromNullable(tt.NumberFromString),
});

type Params = t.TypeOf<typeof paramsCodec>;

type GroupEvaluationsPage = (params: Params) => TE.TaskEither<RenderPageError, Page>;

const notFoundResponse = () => ({
  type: DE.notFound,
  message: toHtmlFragment('No such group. Please check and try again.'),
} as const);

const renderArticleCount = (articleCount: number) => pipe(
  articleCount === 1,
  (singular) => `<span>${articleCount} ${singular ? 'article' : 'articles'}</span>`,
);

const renderLastUpdated = O.fold(
  () => '',
  (date: Date) => `<span>Last updated ${templateDate(date)}</span>`,
);

const renderPageNumbers = (page: O.Option<number>, articleCount: number, pageSize: number) => pipe(
  articleCount,
  O.fromPredicate(() => articleCount > 0),
  O.fold(
    constant(''),
    (count) => pipe(
      {
        currentPage: pipe(page, O.getOrElse(() => 1)),
        totalPages: Math.ceil(count / pageSize),
      },
      ({ currentPage, totalPages }) => `<p class="evaluated-articles__page_count">Showing page ${currentPage} of ${totalPages}<span class="visually-hidden"> pages of list content</span></p>`,
    ),
  ),
);

export const groupEvaluationsPage = (ports: Ports): GroupEvaluationsPage => ({ id, page }) => pipe(
  ports.getGroup(id),
  T.map(E.fromOption(notFoundResponse)),
  TE.chainTaskK((group) => pipe(
    ports.getAllEvents,
    T.map((events) => ({
      group,
      articles: evaluatedArticles(group.id)(events),
      ...getEvaluatedArticlesListDetails(group.id)(events),
      pageSize: 20,
    })),
  )),
  TE.chain(({
    group, articles, articleCount, lastUpdated, pageSize,
  }) => pipe(
    {
      header: pipe(
        `<header class="page-header page-header--group-evaluations">
          <h1>
            Evaluated Articles
          </h1>
          <p class="evaluated-articles__subheading">
            <img src="${group.avatarPath}" alt="" class="evaluated-articles__avatar">
            <span>A list by <a href="/groups/${group.id}">${group.name}</a></span>
          </p>
          <p class="evaluated-articles__description">Articles that have been evaluated by ${group.name}, most recently evaluated first.</p>
          <p class="evaluated-articles__meta"><span class="visually-hidden">This list contains </span>${renderArticleCount(articleCount)}${renderLastUpdated(lastUpdated)}</p>
        </header>`,
        toHtmlFragment,
        TE.right,
      ),
      pageNumbers: pipe(
        renderPageNumbers(page, articleCount, pageSize),
        toHtmlFragment,
        TE.right,
      ),
      evaluatedArticlesList: evaluatedArticlesList(ports)(articles, group, O.getOrElse(() => 1)(page), pageSize),
    },
    sequenceS(TE.ApplyPar),
    TE.bimap(renderErrorPage, renderPage(group)),
  )),
);
