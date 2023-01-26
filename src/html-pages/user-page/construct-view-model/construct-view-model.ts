import { sequenceS } from 'fp-ts/Apply';
import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { followList, Ports as FollowListPorts } from './follow-list';
import { tabList } from './tab-list';
import { userListCard } from './user-list-card';
import { tabs } from '../../../shared-components/tabs';
import { GetUserViaHandle, SelectAllListsOwnedBy } from '../../../shared-ports';
import { getGroupIdsFollowedBy } from '../../../shared-read-models/followings';
import * as DE from '../../../types/data-error';
import * as LOID from '../../../types/list-owner-id';
import { FollowingTab, ListsTab, ViewModel } from '../view-model';
import { List } from '../../../types/list';
import { UserHandle } from '../../../types/user-handle';

const constructListsTab = (list: List): ListsTab => ({
  selector: 'lists',
  listId: list.id,
  articleCount: list.articleIds.length,
  lastUpdated: O.some(list.lastUpdated),
  title: list.name,
  description: list.description,
  articleCountLabel: 'This list contains',
});

const constructFollowingTab = (): FollowingTab => ({
  selector: 'followed-groups',
});

export type Ports = FollowListPorts & {
  getUserViaHandle: GetUserViaHandle,
  selectAllListsOwnedBy: SelectAllListsOwnedBy,
};

export type Params = {
  handle: UserHandle,
};

type ConstructViewModel = (tab: string, ports: Ports) => (params: Params) => TE.TaskEither<DE.DataError, ViewModel>;

export const constructViewModel: ConstructViewModel = (tab, ports) => (params) => pipe(
  params.handle,
  ports.getUserViaHandle,
  TE.fromOption(() => DE.notFound),
  TE.chainW((user) => pipe(
    {
      groupIds: pipe(
        ports.getAllEvents,
        T.map(getGroupIdsFollowedBy(user.id)),
        TE.rightTask,
      ),
      userDetails: TE.right(user),
      activeTabIndex: TE.right(tab === 'lists' ? 0 as const : 1 as const),
      list: pipe(
        user.id,
        LOID.fromUserId,
        ports.selectAllListsOwnedBy,
        RA.head,
        E.fromOption(() => DE.notFound),
        T.of,
      ),
    },
    sequenceS(TE.ApplyPar),
  )),
  TE.chainTaskK((inputs) => pipe(
    (inputs.activeTabIndex === 0)
      ? T.of(userListCard(inputs.list))
      : followList(ports)(inputs.groupIds),
    T.map(tabs({
      tabList: tabList(inputs.userDetails.handle, inputs.groupIds.length),
      activeTabIndex: inputs.activeTabIndex,
    })),
    T.map((mainContent) => ({
      user: inputs.userDetails,
      groupIds: inputs.groupIds,
      mainContent,
      activeTab: (tab === 'lists' ? constructListsTab(inputs.list) : constructFollowingTab()),
    })),
  )),
);
