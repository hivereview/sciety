import * as O from 'fp-ts/Option';
import { pipe } from 'fp-ts/function';
import { renderListPageLinkHref } from '../../../shared-components/render-list-page-link-href.js';
import { List } from '../../../types/list.js';
import { Dependencies } from './dependencies.js';

type ListWithAddedOwnershipInformation = {
  name: string,
  description: string,
  ownerName: string,
  ownerAvatarUrl: string,
  linkUrl: string,
};

export const addListOwnershipInformation = (
  dependencies: Dependencies,
) => (
  list: List,
): ListWithAddedOwnershipInformation => {
  switch (list.ownerId.tag) {
    case 'group-id':
      return pipe(
        dependencies.getGroup(list.ownerId.value),
        O.match(
          () => {
            dependencies.logger('error', 'Could not find group that owns list', {
              listId: list.id,
              ownerId: list.ownerId,
            });
            return {
              ...list,
              ownerName: 'A group',
              ownerAvatarUrl: '/static/images/sciety-logo.jpg',
              linkUrl: renderListPageLinkHref(list.id),
            };
          },
          (group) => ({
            ...list,
            ownerName: group.name,
            ownerAvatarUrl: group.avatarPath,
            linkUrl: renderListPageLinkHref(list.id),
          }),
        ),
      );
    case 'user-id':
      return pipe(
        list.ownerId.value,
        dependencies.lookupUser,
        O.match(
          () => {
            dependencies.logger('error', 'Could not find user who owns list', {
              listId: list.id,
              ownerId: list.ownerId,
            });
            return {
              ...list,
              ownerName: 'A user',
              ownerAvatarUrl: '/static/images/sciety-logo.jpg',
              linkUrl: renderListPageLinkHref(list.id),
            };
          },
          (userDetails) => (
            {
              ...list,
              ownerName: userDetails.handle,
              ownerAvatarUrl: userDetails.avatarUrl,
              linkUrl: renderListPageLinkHref(list.id),
            }
          ),
        ),
      );
  }
};
