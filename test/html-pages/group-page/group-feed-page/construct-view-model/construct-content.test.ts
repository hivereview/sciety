import { pipe } from 'fp-ts/function';
import * as O from 'fp-ts/Option';
import * as TE from 'fp-ts/TaskEither';
import * as RA from 'fp-ts/ReadonlyArray';
import { dummyLogger } from '../../../../dummy-logger';
import { OrderedArticleCards, ViewModel } from '../../../../../src/html-pages/group-page/group-feed-page/view-model';
import { createTestFramework, TestFramework } from '../../../../framework';
import { arbitraryGroup } from '../../../../types/group.helper';
import { shouldNotBeCalled } from '../../../../should-not-be-called';
import { constructContent } from '../../../../../src/html-pages/group-page/group-feed-page/construct-view-model/construct-content';
import { arbitraryArticleId } from '../../../../types/article-id.helper';
import { Dependencies } from '../../../../../src/html-pages/group-page/group-feed-page/construct-view-model/dependencies';
import { Doi } from '../../../../../src/types/doi';
import { ListId } from '../../../../../src/types/list-id';

describe('construct-content', () => {
  let framework: TestFramework;
  let dependencies: Dependencies;
  const group = arbitraryGroup();
  let groupEvaluatedArticlesList: ListId;
  const isOrderedArticleCards = (c: ViewModel['content']): c is OrderedArticleCards => c.tag === 'ordered-article-cards';
  const getContent = () => constructContent(
    dependencies,
    group,
    3,
    1,
  );
  const getContentAsRight = async () => pipe(
    getContent(),
    TE.getOrElse(shouldNotBeCalled),
  )();
  const getContentAsOrderedArticleCards = async () => pipe(
    getContent(),
    TE.filterOrElseW(isOrderedArticleCards, shouldNotBeCalled),
    TE.getOrElse(shouldNotBeCalled),
  )();

  const getArticleIdsFromContent = (orderedArticleCards: OrderedArticleCards) => pipe(
    orderedArticleCards.articleCards,
    RA.rights,
    RA.map((card) => card.articleId),
  );

  beforeEach(async () => {
    framework = createTestFramework();
    dependencies = {
      ...framework.queries,
      ...framework.happyPathThirdParties,
      logger: dummyLogger,
    };
    await framework.commandHelpers.createGroup(group);
    groupEvaluatedArticlesList = pipe(
      framework.queries.getEvaluatedArticlesListIdForGroup(group.id),
      O.getOrElseW(shouldNotBeCalled),
    );
  });

  describe('when the group\'s evaluated articles list contains two articles', () => {
    const article1 = arbitraryArticleId();
    const article2 = arbitraryArticleId();
    let articleIds: ReadonlyArray<Doi>;
    let nextPageHref: O.Option<string>;

    beforeEach(async () => {
      await framework.commandHelpers.addArticleToList(article1, groupEvaluatedArticlesList);
      await framework.commandHelpers.addArticleToList(article2, groupEvaluatedArticlesList);

      const orderedArticleCards = await getContentAsOrderedArticleCards();
      articleIds = getArticleIdsFromContent(orderedArticleCards);
      nextPageHref = orderedArticleCards.nextPageHref;
    });

    it('has the most recently added article as the first article card', () => {
      expect(articleIds).toStrictEqual([article2, article1]);
    });

    it('does not have a next page link', () => {
      expect(nextPageHref).toStrictEqual(O.none);
    });
  });

  describe('when the group\'s evaluated articles list contains more than one page of articles', () => {
    const article1 = arbitraryArticleId();
    const article2 = arbitraryArticleId();
    const article3 = arbitraryArticleId();
    const article4 = arbitraryArticleId();
    let articleIds: ReadonlyArray<Doi>;
    let nextPageHref: O.Option<string>;

    beforeEach(async () => {
      await framework.commandHelpers.addArticleToList(article1, groupEvaluatedArticlesList);
      await framework.commandHelpers.addArticleToList(article2, groupEvaluatedArticlesList);
      await framework.commandHelpers.addArticleToList(article3, groupEvaluatedArticlesList);
      await framework.commandHelpers.addArticleToList(article4, groupEvaluatedArticlesList);

      const orderedArticleCards = await getContentAsOrderedArticleCards();
      articleIds = getArticleIdsFromContent(orderedArticleCards);
      nextPageHref = orderedArticleCards.nextPageHref;
    });

    it('has the most recently added article as the first article card', () => {
      expect(articleIds).toStrictEqual([article4, article3, article2]);
    });

    it('does have a link to the next page', () => {
      expect(nextPageHref).toStrictEqual(O.some(`/groups/${group.slug}/feed?page=2`));
    });
  });

  describe('when the group\'s evaluated articles list is empty', () => {
    let content: ViewModel['content'];

    beforeEach(async () => {
      content = await getContentAsRight();
    });

    it('contains a no-activity-yet message', () => {
      expect(content.tag).toBe('no-activity-yet');
    });
  });
});
