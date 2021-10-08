import * as E from 'fp-ts/Either';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { ScietyFeedCard } from './sciety-feed-card';
import { UserSavedArticleEvent } from '../../domain-events';
import * as DE from '../../types/data-error';
import { toHtmlFragment } from '../../types/html-fragment';
import { UserId } from '../../types/user-id';
import { defaultUserListDescription } from '../../user-page/static-messages';

type GetUserDetails = (userId: UserId) => TE.TaskEither<DE.DataError, {
  handle: string,
  avatarUrl: string,
}>;

export type Ports = {
  getUserDetails: GetUserDetails,
};

type UserSavedArticleToAListCard = (
  ports: Ports,
) => (event: UserSavedArticleEvent) => TE.TaskEither<DE.DataError, ScietyFeedCard>;

export const userSavedArticleToAListCard: UserSavedArticleToAListCard = (ports) => (event) => pipe(
  event.userId,
  ports.getUserDetails,
  TE.match(
    () => ({
      titleText: 'A user saved an article to a list',
      linkUrl: `/users/${event.userId}/lists/saved-articles`,
      avatarUrl: '/static/images/sciety-logo.jpg',
      date: event.date,
      details: {
        title: toHtmlFragment('Saved articles'),
        content: toHtmlFragment(`<p>${defaultUserListDescription('this user')}</p>`),
      },
    }),
    ({ handle, avatarUrl }) => ({
      titleText: `${handle} saved an article to a list`,
      linkUrl: `/users/${handle}/lists/saved-articles`,
      avatarUrl,
      date: event.date,
      details: {
        title: toHtmlFragment('Saved articles'),
        content: toHtmlFragment(`<p>${defaultUserListDescription(`@${handle}`)}.</p>`),
      },
    }),
  ),
  T.map(E.right),
);
