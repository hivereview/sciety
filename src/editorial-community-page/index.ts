import * as O from 'fp-ts/Option';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { constructFeedItem, GetArticle } from './construct-feed-item';
import { getDescription } from './get-description';
import { createGetMostRecentEvents, GetAllEvents } from './get-most-recent-events';
import { createProjectFollowerIds } from './project-follower-ids';
import { createRenderDescription } from './render-description';
import { createRenderFeed, RenderFeed } from './render-feed';
import { createRenderFollowToggle, Follows } from './render-follow-toggle';
import { createRenderFollowers } from './render-followers';
import { createRenderPage, RenderPage } from './render-page';
import { renderPageHeader } from './render-page-header';
import { renderSummaryFeedList } from '../shared-components/render-summary-feed-list';
import { EditorialCommunity } from '../types/editorial-community';
import { EditorialCommunityId } from '../types/editorial-community-id';
import { toHtmlFragment } from '../types/html-fragment';
import { User } from '../types/user';

type FetchStaticFile = (filename: string) => T.Task<string>;

type FetchEditorialCommunity = (editorialCommunityId: EditorialCommunityId) => T.Task<O.Option<EditorialCommunity>>;

type Ports = {
  fetchArticle: GetArticle,
  fetchStaticFile: FetchStaticFile,
  getEditorialCommunity: FetchEditorialCommunity,
  getAllEvents: GetAllEvents,
  follows: Follows,
};

const buildRenderFeed = (ports: Ports): RenderFeed => createRenderFeed(
  createGetMostRecentEvents(ports.getAllEvents, 20),
  constructFeedItem(ports.fetchArticle),
  renderSummaryFeedList,
  createRenderFollowToggle(ports.follows),
);

export type Params = {
  id?: string,
  user: O.Option<User>,
};

type EditorialCommunityPage = (params: Params) => ReturnType<RenderPage>;

export const editorialCommunityPage = (ports: Ports): EditorialCommunityPage => {
  const renderPage = createRenderPage(
    renderPageHeader,
    createRenderDescription(getDescription(ports.fetchStaticFile)),
    buildRenderFeed(ports),
    createRenderFollowers(createProjectFollowerIds(ports.getAllEvents)),
  );
  return (params) => {
    const editorialCommunityId = new EditorialCommunityId(params.id ?? '');
    const userId = pipe(
      params.user,
      O.map((user) => user.id),
    );

    return pipe(
      editorialCommunityId,
      ports.getEditorialCommunity,
      T.chain(O.fold(
        () => TE.left({
          type: 'not-found',
          message: toHtmlFragment(`Editorial community id '${editorialCommunityId.value}' not found`),
        } as const),
        (editorialCommunity) => renderPage(editorialCommunity, userId),
      )),
    );
  };
};
