import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { ScietyFeedCard } from './sciety-feed-card';
import { ArticleAddedToListEvent } from '../../domain-events';
import { GetAllEvents } from '../../shared-ports';
import { getGroup } from '../../shared-read-models/groups';
import { getList, List } from '../../shared-read-models/lists';
import * as DE from '../../types/data-error';
import { toHtmlFragment } from '../../types/html-fragment';
import { UserId } from '../../types/user-id';

type GetUserDetails = (userId: UserId) => TE.TaskEither<DE.DataError, {
  handle: string,
  avatarUrl: string,
}>;

type Ports = {
  getAllEvents: GetAllEvents,
  getUserDetails: GetUserDetails,
};

const addListOwnerName = (ports: Ports) => (list: List) => {
  switch (list.ownerId.tag) {
    case 'group-id':
      return pipe(
        ports.getAllEvents,
        TE.rightTask,
        TE.chainEitherK(getGroup(list.ownerId.value)),
        TE.map((group) => ({
          ...list,
          ownerName: group.name,
          ownerAvatarUrl: group.avatarPath,
        })),
      );
    case 'user-id':
      return pipe(
        list.ownerId.value,
        ports.getUserDetails,
        TE.match(
          () => (
            {
              ...list,
              ownerName: 'A user',
              ownerAvatarUrl: '/static/images/sciety-logo.jpg',
            }
          ),
          (userDetails) => (
            {
              ...list,
              ownerName: userDetails.handle,
              ownerAvatarUrl: userDetails.avatarUrl,
            }
          ),
        ),
        TE.rightTask,
      );
  }
};

type ArticleAddedToListCard = (
  ports: Ports,
) => (event: ArticleAddedToListEvent) => TE.TaskEither<DE.DataError, ScietyFeedCard>;

export const articleAddedToListCard: ArticleAddedToListCard = (ports) => (event) => pipe(
  ports.getAllEvents,
  TE.rightTask,
  TE.chain(getList(event.listId)),
  TE.chain(addListOwnerName(ports)),
  TE.map((extendedListMetadata) => ({
    ownerName: extendedListMetadata.ownerName,
    ownerAvatarUrl: extendedListMetadata.ownerAvatarUrl,
    listName: extendedListMetadata.name,
    listDescription: extendedListMetadata.description,
  })),
  TE.map(
    (viewModel) => ({
      titleText: `${viewModel.ownerName} added an article to a list`,
      linkUrl: `/lists/${event.listId}`,
      avatarUrl: viewModel.ownerAvatarUrl,
      date: event.date,
      details: {
        title: toHtmlFragment(viewModel.listName),
        content: toHtmlFragment(viewModel.listDescription),
      },
    }),
  ),
);
