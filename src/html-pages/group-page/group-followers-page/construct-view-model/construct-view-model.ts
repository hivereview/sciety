import { pipe } from 'fp-ts/function';
import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as TE from 'fp-ts/TaskEither';
import { renderLegacyPaginationControls } from '../../../../shared-components/pagination/index.js';
import { paginate } from './paginate.js';
import { augmentWithUserDetails } from './augment-with-user-details.js';
import * as DE from '../../../../types/data-error.js';
import { ViewModel } from '../view-model.js';
import { findFollowers } from './find-followers.js';
import { constructTabsViewModel } from '../../common-components/tabs-view-model.js';
import { GroupId } from '../../../../types/group-id.js';
import { Dependencies } from './dependencies.js';
import { Params } from './params.js';

const pageSize = 10;

const isFollowing = (dependencies: Dependencies) => (groupId: GroupId, user: Params['user']) => pipe(
  user,
  O.fold(
    () => false,
    (u) => dependencies.isFollowing(groupId)(u.id),
  ),
);

type ConstructViewModel = (dependencies: Dependencies) => (params: Params) => TE.TaskEither<DE.DataError, ViewModel>;

export const constructViewModel: ConstructViewModel = (dependencies) => (params) => pipe(
  dependencies.getGroupBySlug(params.slug),
  E.fromOption(() => DE.notFound),
  E.chain((group) => pipe(
    group.id,
    findFollowers(dependencies),
    paginate(params.page, pageSize),
    E.map((pageOfFollowers) => ({
      group,
      pageNumber: params.page,
      isFollowing: isFollowing(dependencies)(group.id, params.user),
      followerCount: pageOfFollowers.numberOfOriginalItems,
      followers: augmentWithUserDetails(dependencies)(pageOfFollowers.items),
      nextLink: renderLegacyPaginationControls({
        nextPageHref: pipe(
          pageOfFollowers.nextPage,
          O.map(
            (nextPage) => `/groups/${group.slug}/followers?page=${nextPage}`,
          ),
        ),
      }),
      tabs: constructTabsViewModel(dependencies, group),
    })),
  )),
  TE.fromEither,
);
