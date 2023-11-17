import * as O from 'fp-ts/Option';
import { pipe } from 'fp-ts/function';
import { addListOwnershipInformation } from './add-list-ownership-information.js';
import { ScietyFeedCard } from '../view-model.js';
import { toHtmlFragment } from '../../../types/html-fragment.js';
import { EventOfType } from '../../../domain-events/index.js';
import { Dependencies } from './dependencies.js';

type ArticleAddedToListCard = (
  dependencies: Dependencies,
) => (event: EventOfType<'ArticleAddedToList'>) => O.Option<ScietyFeedCard>;

export const articleAddedToListCard: ArticleAddedToListCard = (dependencies) => (event) => pipe(
  event.listId,
  dependencies.lookupList,
  O.map(addListOwnershipInformation(dependencies)),
  O.map((extendedListMetadata) => ({
    ownerName: extendedListMetadata.ownerName,
    ownerAvatarUrl: extendedListMetadata.ownerAvatarUrl,
    listName: extendedListMetadata.name,
    listDescription: extendedListMetadata.description,
    linkUrl: extendedListMetadata.linkUrl,
  })),
  O.map(
    (viewModel) => ({
      titleText: `${viewModel.ownerName} added an article to a list`,
      feedItemHref: viewModel.linkUrl,
      avatarUrl: viewModel.ownerAvatarUrl,
      date: event.date,
      details: {
        title: toHtmlFragment(viewModel.listName),
        content: toHtmlFragment(viewModel.listDescription),
      },
    }),
  ),
);
