import { sequenceS } from 'fp-ts/Apply';
import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import * as TO from 'fp-ts/TaskOption';
import { flow, pipe } from 'fp-ts/function';
import * as t from 'io-ts';
import * as tt from 'io-ts-types';
import { countFollowersOf } from './count-followers';
import { getEvaluatedArticlesListDetails } from './get-evaluated-articles-list-details';
import { FetchStaticFile, renderDescription } from './render-description';
import { renderEvaluatedArticlesListCard } from './render-evaluated-articles-list-card';
import { renderFollowers } from './render-followers';
import { renderErrorPage, renderPage } from './render-page';
import { renderPageHeader } from './render-page-header';
import { DomainEvent } from '../domain-events';
import { renderFollowToggle } from '../follow/render-follow-toggle';
import { GroupIdFromString } from '../types/codecs/GroupIdFromString';
import { UserIdFromString } from '../types/codecs/UserIdFromString';
import * as DE from '../types/data-error';
import { Group } from '../types/group';
import { GroupId } from '../types/group-id';
import { toHtmlFragment } from '../types/html-fragment';
import { Page } from '../types/page';
import { RenderPageError } from '../types/render-page-error';
import { UserId } from '../types/user-id';

type FetchGroup = (groupId: GroupId) => TO.TaskOption<Group>;

type GetAllEvents = T.Task<ReadonlyArray<DomainEvent>>;

type Ports = {
  fetchStaticFile: FetchStaticFile,
  getGroup: FetchGroup,
  getAllEvents: GetAllEvents,
  follows: (userId: UserId, groupId: GroupId) => T.Task<boolean>,
};

export const paramsCodec = t.type({
  id: GroupIdFromString,
  user: tt.optionFromNullable(t.type({
    id: UserIdFromString,
  })),
});

type Params = t.TypeOf<typeof paramsCodec>;

const notFoundResponse = () => ({
  type: DE.notFound,
  message: toHtmlFragment('No such group. Please check and try again.'),
} as const);

const aboutTabComponents = (ports: Ports) => (group: Group) => ({
  description: pipe(
    `groups/${group.descriptionPath}`,
    ports.fetchStaticFile,
    TE.map(renderDescription),
  ),
  followers: pipe(
    ports.getAllEvents,
    T.map(flow(
      countFollowersOf(group.id),
      renderFollowers,
      E.right,
    )),
  ),
});

const listTabComponents = (ports: Ports) => (group: Group) => ({
  evaluatedArticlesListCard: pipe(
    ports.getAllEvents,
    T.map(getEvaluatedArticlesListDetails(group.id)),
    T.map((details) => ({
      group,
      ...details,
    })),
    T.map(renderEvaluatedArticlesListCard),
    TE.rightTask,
  ),
});

type GroupPage = (params: Params) => TE.TaskEither<RenderPageError, Page>;

export const groupPage = (ports: Ports): GroupPage => ({ id, user }) => pipe(
  ports.getGroup(id),
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
      ...aboutTabComponents(ports)(group),
      ...listTabComponents(ports)(group),
    },
    sequenceS(TE.ApplyPar),
    TE.bimap(renderErrorPage, renderPage(group)),
  )),
);
