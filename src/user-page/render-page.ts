import { sequenceS } from 'fp-ts/Apply';
import * as O from 'fp-ts/Option';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { HtmlFragment, toHtmlFragment } from '../types/html-fragment';
import { RenderPageError } from '../types/render-page-error';
import { UserId } from '../types/user-id';

type Page = {
  title: string,
  content: HtmlFragment,
};

export type RenderPage = (userId: UserId, viewingUserId: O.Option<UserId>) => TE.TaskEither<RenderPageError, Page>;

type Component = (userId: UserId, viewingUserId: O.Option<UserId>) => TE.TaskEither<'not-found' | 'unavailable', HtmlFragment>;

const template = (
  components: {
    header: HtmlFragment,
    followList: HtmlFragment,
    userDisplayName: string,
    savedArticlesList: HtmlFragment,
  },
): Page => (
  {
    title: `${components.userDisplayName}`,
    content: toHtmlFragment(`
      <div class="sciety-grid sciety-grid--user">
        ${components.header}
        <div class="user-page-contents">
          ${components.followList}
          ${components.savedArticlesList}
        </div>
      </div>
    `),
  }
);

type GetUserDisplayName = (userId: UserId) => TE.TaskEither<'not-found' | 'unavailable', string>;

export const renderPage = (
  renderHeader: Component,
  renderFollowList: Component,
  getUserDisplayName: GetUserDisplayName,
  renderSavedArticles: Component,
): RenderPage => (userId, viewingUserId) => pipe(
  {
    header: renderHeader(userId, viewingUserId),
    followList: renderFollowList(userId, viewingUserId),
    userDisplayName: pipe(userId, getUserDisplayName, TE.map(toHtmlFragment)),
    savedArticlesList: renderSavedArticles(userId, viewingUserId),
  },
  sequenceS(TE.taskEither),
  TE.bimap(
    (e) => {
      if (e === 'not-found') {
        return {
          type: 'not-found',
          message: toHtmlFragment('User not found'),
        };
      }
      return {
        type: 'unavailable',
        message: toHtmlFragment('User information unavailable'),
      };
    },
    template,
  ),
);
