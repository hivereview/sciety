import { sequenceS } from 'fp-ts/Apply';
import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import * as TO from 'fp-ts/TaskOption';
import { pipe } from 'fp-ts/function';
import * as t from 'io-ts';
import * as tt from 'io-ts-types';
import { about, Ports as AboutPorts } from './about/about';
import { followers, Ports as FollowersPorts } from './followers/followers';
import { getEvaluatedArticlesListDetails } from './get-evaluated-articles-list-details';
import { renderEvaluatedArticlesListCard } from './render-evaluated-articles-list-card';
import { renderErrorPage, renderPage } from './render-page';
import { renderPageHeader } from './render-page-header';
import { DomainEvent } from '../domain-events';
import { renderFollowToggle } from '../follow/render-follow-toggle';
import { Tab, tabs } from '../shared-components/tabs';
import { UserIdFromString } from '../types/codecs/UserIdFromString';
import * as DE from '../types/data-error';
import { Group } from '../types/group';
import { GroupId } from '../types/group-id';
import { HtmlFragment, toHtmlFragment } from '../types/html-fragment';
import { Page } from '../types/page';
import { RenderPageError } from '../types/render-page-error';
import { UserId } from '../types/user-id';

type GetAllEvents = T.Task<ReadonlyArray<DomainEvent>>;

type Ports = AboutPorts & FollowersPorts & {
  getGroupBySlug: (slug: string) => TO.TaskOption<Group>,
  getAllEvents: GetAllEvents,
  follows: (userId: UserId, groupId: GroupId) => T.Task<boolean>,
};

type TabIndex = 0 | 1 | 2;

export const groupPageTabs: Record<string, TabIndex> = {
  lists: 0,
  about: 1,
  followers: 2,
};

export const paramsCodec = t.type({
  slug: t.string,
  user: tt.optionFromNullable(t.type({
    id: UserIdFromString,
  })),
  page: tt.withFallback(tt.NumberFromString, 1),
});

type Params = t.TypeOf<typeof paramsCodec>;

const notFoundResponse = () => ({
  type: DE.notFound,
  message: toHtmlFragment('No such group. Please check and try again.'),
} as const);

const renderLists = (evaluatedArticlesListCard: HtmlFragment) => toHtmlFragment(`
  <section class="group-page-lists">
    ${evaluatedArticlesListCard}
  </section>
`);

const listTabComponents = (ports: Ports) => (group: Group) => pipe(
  ports.getAllEvents,
  T.map(getEvaluatedArticlesListDetails(group.id)),
  T.map((details) => ({
    group,
    ...details,
  })),
  T.map(renderEvaluatedArticlesListCard),
  T.map(renderLists),
  TE.rightTask,
);

const contentRenderers = (
  ports: Ports,
) => (
  group: Group,
  pageNumber: number,
): Record<TabIndex, TE.TaskEither<DE.DataError, HtmlFragment>> => ({
  0: listTabComponents(ports)(group),
  1: about(ports)(group),
  2: followers(ports)(group, pageNumber),
});

const tabList = (groupId: GroupId): [Tab, Tab, Tab] => [
  {
    label: toHtmlFragment('Lists'),
    url: `/groups/${groupId}/lists`,
  },
  {
    label: toHtmlFragment('About'),
    url: `/groups/${groupId}/about`,
  },
  {
    label: toHtmlFragment('Followers'),
    url: `/groups/${groupId}/followers`,
  },
];

type GroupPage = (
  ports: Ports
) => (
  activeTabIndex: TabIndex
) => (
  params: Params
) => TE.TaskEither<RenderPageError, Page>;

export const groupPage: GroupPage = (ports) => (activeTabIndex) => ({ slug, user, page: pageNumber }) => pipe(
  ports.getGroupBySlug(slug),
  T.map(E.fromOption(notFoundResponse)),
  TE.chain((group) => pipe(
    {
      header: pipe(
        group,
        renderPageHeader,
        TE.right,
      ),
      followButton: pipe(
        user,
        O.fold(
          () => T.of(false),
          (u) => ports.follows(u.id, group.id),
        ),
        T.map(renderFollowToggle(group.id, group.name)),
        TE.rightTask,
      ),
      content: pipe(
        contentRenderers(ports)(group, pageNumber)[activeTabIndex],
        TE.map(tabs({
          tabList: tabList(group.id),
          activeTabIndex,
        })),
      ),
    },
    sequenceS(TE.ApplyPar),
    TE.bimap(renderErrorPage, renderPage(group)),
  )),
);
