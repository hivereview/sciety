import * as O from 'fp-ts/lib/Option';
import * as T from 'fp-ts/lib/Task';
import { Maybe } from 'true-myth';
import createRenderFeed, { GetEvents } from '../../src/editorial-community-page/render-feed';
import { RenderFollowToggle } from '../../src/editorial-community-page/render-follow-toggle';
import { RenderSummaryFeedList } from '../../src/shared-components/render-summary-feed-list';
import EditorialCommunityId from '../../src/types/editorial-community-id';
import { toHtmlFragment } from '../../src/types/html-fragment';

describe('render feed', () => {
  const stubGetEvents: GetEvents<unknown> = () => T.of([]);
  const stubRenderFollowToggle: RenderFollowToggle = async () => toHtmlFragment('');
  const anEditorialCommunityId = new EditorialCommunityId('');
  const aUserId = Maybe.nothing<never>();

  describe('with community events', () => {
    it('returns a list of events', async () => {
      const renderSummaryFeedList: RenderSummaryFeedList<unknown> = async () => O.some(toHtmlFragment('a list'));

      const renderFeed = createRenderFeed(stubGetEvents, renderSummaryFeedList, stubRenderFollowToggle);

      const rendered = await renderFeed(anEditorialCommunityId, aUserId);

      expect(rendered).toContain('a list');
    });
  });

  describe('without community events', () => {
    it('returns fallback text', async () => {
      const renderSummaryFeedList: RenderSummaryFeedList<unknown> = async () => O.none;

      const renderFeed = createRenderFeed(stubGetEvents, renderSummaryFeedList, stubRenderFollowToggle);

      const rendered = await renderFeed(anEditorialCommunityId, aUserId);

      expect(rendered).toContain('community hasn’t evaluated');
    });
  });
});
