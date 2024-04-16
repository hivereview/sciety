import * as O from 'fp-ts/Option';
import { pipe } from 'fp-ts/function';
import * as RA from 'fp-ts/ReadonlyArray';
import { Dependencies } from './dependencies';
import { Params } from './params';
import { Group } from '../../../../types/group';
import { ViewModel } from '../view-model';
import { calculateListCount } from '../../common-components/calculate-list-count';
import { GroupId } from '../../../../types/group-id';

const constructGroupListsPageHref = (group: Group, listCount: number): O.Option<string> => (
  listCount === 1
    ? O.none
    : O.some(`/groups/${group.slug}/lists`));

const checkFollowingStatus = (user: Params['user'], dependencies: Dependencies, groupId: GroupId) => pipe(
  user,
  O.fold(
    () => false,
    (u) => dependencies.isFollowing(groupId)(u.id),
  ),
);

export const constructHeader = (dependencies: Dependencies, user: Params['user']) => (group: Group): ViewModel['header'] => ({
  group,
  isFollowing: checkFollowingStatus(user, dependencies, group.id),
  followerCount: pipe(
    dependencies.getFollowers(group.id),
    RA.size,
  ),
  groupAboutPageHref: `/groups/${group.slug}/about`,
  groupListsPageHref: pipe(
    group.id,
    calculateListCount(dependencies),
    (listCount) => constructGroupListsPageHref(group, listCount),
  ),
  groupFollowersPageHref: `/groups/${group.slug}/followers`,
});
