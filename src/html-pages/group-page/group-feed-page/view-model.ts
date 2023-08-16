import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import { ArticleCardViewModel, ArticleErrorCardViewModel } from '../../../shared-components/article-card';
import { Group } from '../../../types/group';
import { PageHeaderViewModel } from '../common-components/page-header';
import { TabsViewModel } from '../common-components/tabs-view-model';

type NoActivity = { tag: 'no-activity-yet' };

export type OrderedArticleCards = {
  tag: 'ordered-article-cards',
  articleCards: ReadonlyArray<E.Either<ArticleErrorCardViewModel, ArticleCardViewModel>>,
  nextPageHref: O.Option<string>,
};

type Content = NoActivity | OrderedArticleCards;

export type ViewModel = PageHeaderViewModel & {
  group: Group,
  content: Content,
  tabs: TabsViewModel,
};
