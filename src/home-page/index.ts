import createGetMostRecentEvents, { FilterEvents } from './get-most-recent-events';
import createRenderEditorialCommunities, { GetAllEditorialCommunities } from './render-editorial-communities';
import createRenderFeed, { GetFollows } from './render-feed';
import createRenderFeedItem, { GetActor, GetArticle } from './render-feed-item';
import createRenderFindArticle from './render-find-article';
import createRenderFollowToggle from './render-follow-toggle';
import createRenderPage from './render-page';
import createRenderPageHeader from './render-page-header';
import EditorialCommunityRepository from '../types/editorial-community-repository';
import { FetchExternalArticle } from '../types/fetch-external-article';
import FollowList from '../types/follow-list';
import { User } from '../types/user';
import { UserId } from '../types/user-id';

interface Ports {
  fetchArticle: FetchExternalArticle;
  editorialCommunities: EditorialCommunityRepository;
  filterEvents: FilterEvents,
  getFollowList: (userId: UserId) => Promise<FollowList>,
}

interface Params {
  user: User,
}

type RenderPage = (params: Params) => Promise<string>;

export default (ports: Ports): RenderPage => {
  const editorialCommunitiesAdapter: GetAllEditorialCommunities = async () => ports.editorialCommunities.all();
  const getActorAdapter: GetActor = async (id) => {
    const editorialCommunity = (await ports.editorialCommunities.lookup(id)).unsafelyUnwrap();
    return {
      name: editorialCommunity.name,
      imageUrl: editorialCommunity.avatarUrl,
      url: `/editorial-communities/${id.value}`,
    };
  };
  const getArticleAdapter: GetArticle = async (id) => (
    (await ports.fetchArticle(id)).unsafelyUnwrap()
  );
  const getFollows: GetFollows = async (userId) => {
    const followList = await ports.getFollowList(userId);

    return (editorialCommunityId) => followList.follows(editorialCommunityId);
  };
  const getEventsAdapter = createGetMostRecentEvents(ports.filterEvents, 20);

  const renderPageHeader = createRenderPageHeader();
  const renderFollowToggle = createRenderFollowToggle(getFollows);
  const renderEditorialCommunities = createRenderEditorialCommunities(editorialCommunitiesAdapter, renderFollowToggle);
  const renderFindArticle = createRenderFindArticle();
  const renderFeedItem = createRenderFeedItem(getActorAdapter, getArticleAdapter);
  const renderFeed = createRenderFeed(
    getFollows,
    getEventsAdapter,
    renderFeedItem,
  );
  const renderPage = createRenderPage(
    renderPageHeader,
    renderEditorialCommunities,
    renderFindArticle,
    renderFeed,
  );

  return async (params) => (
    renderPage(params.user.id)
  );
};
