import { toHtmlFragment } from '../../../types/html-fragment';

const staticMessage = (message: string) => toHtmlFragment(`<p class="articles-static-message">${message}</p>`);

export const noArticlesMessageForReader = staticMessage(
  'This list is currently empty. Try coming back later!',
);

export const noArticlesMessageForOwner = staticMessage(
  'This list is currently empty. <a href="/search">Search for evaluated preprints</a> to add them to your list.',
);

export const noArticlesCanBeFetchedMessage = staticMessage(
  'This information can\'t be fetched right now. Please try again later.',
);
