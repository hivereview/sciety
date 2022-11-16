import { sequenceS } from 'fp-ts/Apply';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { followList, Ports as FollowListPorts } from './follow-list/follow-list';
import { renderDescription } from './render-description';
import { renderErrorPage } from './render-error-page';
import { renderHeader } from './render-header';
import { renderPage } from './render-page';
import { tabList } from './tab-list';
import { userListCard } from './user-list-card';
import { tabs } from '../shared-components/tabs';
import { GetUserDetails } from '../shared-ports/get-user-details';
import { getGroupIdsFollowedBy } from '../shared-read-models/followings';
import * as DE from '../types/data-error';
import { toHtmlFragment } from '../types/html-fragment';
import * as LID from '../types/list-id';
import { Page } from '../types/page';
import { RenderPageError } from '../types/render-page-error';
import { UserId } from '../types/user-id';

type GetUserId = (handle: string) => TE.TaskEither<DE.DataError, UserId>;

type Ports = FollowListPorts & {
  getUserDetails: GetUserDetails,
  getUserId: GetUserId,
};

type Params = {
  handle: string,
};

type UserPage = (tab: string) => (params: Params) => TE.TaskEither<RenderPageError, Page>;

export const userPage = (ports: Ports): UserPage => (tab) => (params) => pipe(
  params.handle,
  ports.getUserId, // TODO: get the user details (extended to include the id) from Twitter here instead
  TE.chain((id) => pipe(
    {
      groupIds: pipe(
        ports.getAllEvents,
        T.map(getGroupIdsFollowedBy(id)),
        TE.rightTask,
      ),
      userDetails: ports.getUserDetails(id),
      activeTabIndex: TE.right(tab === 'lists' ? 0 as const : 1 as const),
      userId: TE.right(id),
      listId: TE.right(LID.fromValidatedString('abcd')),
    },
    sequenceS(TE.ApplyPar),
  )),
  TE.chainTaskK((inputs) => pipe(
    (inputs.activeTabIndex === 0)
      ? userListCard(ports.getAllEvents)(inputs.userDetails.handle, inputs.userId, inputs.listId)
      : followList(ports)(inputs.groupIds),
    T.map(tabs({
      tabList: tabList(inputs.userDetails.handle, inputs.groupIds.length),
      activeTabIndex: inputs.activeTabIndex,
    })),
    T.map((mainContent) => ({
      header: renderHeader(inputs.userDetails),
      userDisplayName: toHtmlFragment(inputs.userDetails.displayName),
      description: renderDescription(inputs.groupIds.length),
      mainContent,
    })),
  )),
  TE.bimap(renderErrorPage, renderPage),
);
