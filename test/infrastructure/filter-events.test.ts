import createFilterEvents from '../../src/infrastructure/filter-events';
import Doi from '../../src/types/doi';
import { DomainEvent, isEditorialCommunityEndorsedArticleEvent } from '../../src/types/domain-events';
import EditorialCommunityId from '../../src/types/editorial-community-id';

const anyEvent = (event: DomainEvent): event is DomainEvent => 'type' in event;

describe('filter-events', () => {
  const editorialCommunity1 = new EditorialCommunityId('a');
  const dummyEvent: DomainEvent = {
    type: 'EditorialCommunityEndorsedArticle',
    date: new Date('2020-07-08'),
    editorialCommunityId: editorialCommunity1,
    articleId: new Doi('10.1101/751099'),
  };

  it('sorts by date descending', async () => {
    const initial: ReadonlyArray<DomainEvent> = [
      {
        type: 'EditorialCommunityEndorsedArticle',
        date: new Date('2020-07-08'),
        editorialCommunityId: editorialCommunity1,
        articleId: new Doi('10.1101/751099'),
      },
      {
        type: 'EditorialCommunityReviewedArticle',
        date: new Date('2020-07-09'),
        editorialCommunityId: editorialCommunity1,
        articleId: new Doi('10.1101/2020.01.22.915660'),
        reviewId: new Doi('10.1234/5678'),
      },
    ];
    const filterEvents = createFilterEvents(initial);
    const sortedEvents = await filterEvents(anyEvent, 20);

    expect(sortedEvents[0]).toStrictEqual(initial[1]);
    expect(sortedEvents[1]).toStrictEqual(initial[0]);
  });

  it('only returns matching events', async () => {
    const initial: ReadonlyArray<DomainEvent> = [
      {
        type: 'EditorialCommunityEndorsedArticle',
        date: new Date('2020-07-08'),
        editorialCommunityId: new EditorialCommunityId('something else'),
        articleId: new Doi('10.1101/751099'),
      },
      {
        type: 'EditorialCommunityReviewedArticle',
        date: new Date('2020-07-09'),
        editorialCommunityId: editorialCommunity1,
        articleId: new Doi('10.1101/2020.01.22.915660'),
        reviewId: new Doi('10.1234/5678'),
      },
    ];
    const filterEvents = createFilterEvents(initial);
    const filtered = await filterEvents(isEditorialCommunityEndorsedArticleEvent, 20);

    expect(filtered).toHaveLength(1);
    expect(filtered[0]).toStrictEqual(initial[0]);
  });

  describe('when there are a small number of matching events', () => {
    it('returns exactly those', async () => {
      const dummyEvents: ReadonlyArray<DomainEvent> = [dummyEvent, dummyEvent, dummyEvent];
      const filterEvents = createFilterEvents(dummyEvents);
      const events = await filterEvents(anyEvent, 20);

      expect(events).toHaveLength(dummyEvents.length);
    });
  });

  describe('when there are more matching events than the specified maximum', () => {
    it('returns just the specified maximum number', async () => {
      const dummyEvents: ReadonlyArray<DomainEvent> = [dummyEvent, dummyEvent, dummyEvent];
      const filterEvents = createFilterEvents(dummyEvents);
      const events = await filterEvents(anyEvent, 2);

      expect(events).toHaveLength(2);
    });
  });
});
