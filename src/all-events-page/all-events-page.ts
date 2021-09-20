import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import * as TO from 'fp-ts/TaskOption';
import { pipe } from 'fp-ts/function';
import * as t from 'io-ts';
import * as tt from 'io-ts-types';
import {
  collapseCloseEvents,
  CollapsedEvent,
  isCollapsedGroupEvaluatedArticle,
  isCollapsedGroupEvaluatedMultipleArticles,
} from './collapse-close-events';
import { evaluatedArticleCard, FetchArticle } from './evaluated-article-card';
import { multipleArticlesCard } from './multiple-articles-card';
import { paginate } from './paginate';
import { DomainEvent, isGroupEvaluatedArticleEvent } from '../domain-events';
import { templateListItems } from '../shared-components/list-items';
import { paginationControls } from '../shared-components/pagination-controls';
import * as DE from '../types/data-error';
import { Group } from '../types/group';
import { GroupId } from '../types/group-id';
import { HtmlFragment, toHtmlFragment } from '../types/html-fragment';
import { Page } from '../types/page';
import { RenderPageError } from '../types/render-page-error';

type ViewModel = {
  cards: ReadonlyArray<HtmlFragment>,
  nextPage: O.Option<number>,
};

const renderContent = (viewModel: ViewModel) => toHtmlFragment(`
  <h1>All events</h1>
  <ol class="all-events-list">
    ${templateListItems(viewModel.cards, 'all-events-list__item')}
  </ol>
  ${pipe(
    viewModel.nextPage,
    O.fold(
      () => '',
      (page) => paginationControls(`/all-events?page=${page}`),
    ),
  )}
`);

export const allEventsCodec = t.type({
  page: tt.withFallback(tt.NumberFromString, 1),
});

type GetGroup = (id: GroupId) => TO.TaskOption<Group>;

type Ports = {
  fetchArticle: FetchArticle,
  getAllEvents: T.Task<ReadonlyArray<DomainEvent>>,
  getGroup: GetGroup,
};

type Params = t.TypeOf<typeof allEventsCodec> & {
  pageSize: number,
};

const renderGenericEvent = (event: DomainEvent | CollapsedEvent) => toHtmlFragment(`
  <article class="all-events-card">
    ${JSON.stringify(event, null, 2)}
  </article>
`);

const eventCard = (
  getGroup: GetGroup,
  fetchArticle: FetchArticle,
) => (
  event: DomainEvent | CollapsedEvent,
): TE.TaskEither<DE.DataError, HtmlFragment> => {
  if (isCollapsedGroupEvaluatedMultipleArticles(event)) {
    return pipe(
      event,
      multipleArticlesCard(getGroup),
    );
  }

  if (isCollapsedGroupEvaluatedArticle(event) || isGroupEvaluatedArticleEvent(event)) {
    return evaluatedArticleCard(getGroup, fetchArticle)(event);
  }

  return TE.right(renderGenericEvent(event));
};

export const allEventsPage = (ports: Ports) => (params: Params): TE.TaskEither<RenderPageError, Page> => pipe(
  ports.getAllEvents,
  T.map(RA.reverse),
  T.map(collapseCloseEvents),
  T.map(paginate(params.pageSize, params.page)),
  TE.chain(({ items, nextPage }) => pipe(
    items,
    TE.traverseArray(eventCard(ports.getGroup, ports.fetchArticle)),
    TE.map((cards) => ({ cards, nextPage })),
  )),
  TE.bimap(
    (e) => ({ type: e, message: toHtmlFragment('We couldn\'t find that information.') }),
    (viewModel) => ({
      title: 'All events',
      content: renderContent(viewModel),
    }),
  ),
);
