import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { flow, pipe } from 'fp-ts/function';
import { ArticleItem, GroupItem, isArticleItem } from './data-types';
import { constructGroupCardViewModel } from '../../../shared-components/group-card';
import * as DE from '../../../types/data-error';
import { ArticlesCategoryViewModel, ItemCardViewModel, ViewModel } from '../view-model';
import {
  ArticleErrorCardViewModel,
  constructArticleCardViewModel,
} from '../../../shared-components/article-card';
import { Dependencies } from './dependencies';
import { Group } from '../../../types/group';
import { Doi } from '../../../types/doi';

const constructItemCardViewModel = (
  dependencies: Dependencies,
) => (item: ArticleItem | GroupItem): TE.TaskEither<DE.DataError | ArticleErrorCardViewModel, ItemCardViewModel> => (
  isArticleItem(item)
    ? pipe(item.articleId, constructArticleCardViewModel(dependencies))
    : pipe(item.id, constructGroupCardViewModel(dependencies), T.of));

type LimitedSetOfGroups = {
  query: string,
  evaluatedOnly: boolean,
  category: 'groups',
  availableArticleMatches: number,
  availableGroupMatches: number,
  itemsToDisplay: ReadonlyArray<GroupItem>,
  nextCursor: O.Option<string>,
  pageNumber: number,
  numberOfPages: number,
};

type LimitedSetOfArticles = {
  query: string,
  evaluatedOnly: boolean,
  category: 'articles',
  availableArticleMatches: number,
  availableGroupMatches: number,
  itemsToDisplay: ReadonlyArray<ArticleItem>,
  nextCursor: O.Option<string>,
  pageNumber: number,
  numberOfPages: number,
};

export type LimitedSet = LimitedSetOfGroups | LimitedSetOfArticles;

// eslint-disable-next-line @typescript-eslint/no-unused-vars
const constructRelatedGroups = (dependencies: Dependencies) => (articleIds: ReadonlyArray<Doi>): ArticlesCategoryViewModel['relatedGroups'] => {
  // step 1: get a list of evaluations for each article id
  // step 2: get the group id for each evaluation
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const groupIds = pipe(
    articleIds,
    RA.flatMap(dependencies.getEvaluationsForDoi),
    RA.map((recordedEvaluation) => recordedEvaluation.groupId),
  );
  // step 3: lookup a group from each group id
  const foundGroups: ReadonlyArray<Group> = [];
  if (foundGroups.length > 0) {
    return {
      tag: 'some-related-groups',
      items: pipe(
        foundGroups,
        RA.map((foundGroup) => ({
          groupPageHref: `/groups/${foundGroup.slug}`,
          groupName: foundGroup.name,
        })),
      ),
    };
  }
  return { tag: 'no-groups-evaluated-the-found-articles' as const };
};

const toFullPageViewModelForGroupsCategory = (
  state: LimitedSet,
) => (itemCardViewModels: ReadonlyArray<ItemCardViewModel>) => ({
  ...state,
  category: 'groups' as const,
  itemCardsToDisplay: itemCardViewModels,
  nextPageHref: pipe(
    {
      basePath: '',
      pageNumber: state.pageNumber + 1,
    },
    ({ basePath, pageNumber }) => O.some(`${basePath}page=${pageNumber}`),
  ),
});

const toFullPageViewModelForArticlesCategory = (
  dependencies: Dependencies,
  state: LimitedSetOfArticles,
) => (itemCardViewModels: ReadonlyArray<ItemCardViewModel>) => ({
  ...state,
  category: 'articles' as const,
  relatedGroups: pipe(
    state.itemsToDisplay,
    RA.map((itemToDisplay) => itemToDisplay.articleId),
    constructRelatedGroups(dependencies),
  ),
  itemCardsToDisplay: itemCardViewModels,
  nextPageHref: pipe(
    {
      basePath: '',
      pageNumber: state.pageNumber + 1,
    },
    ({ basePath, pageNumber }) => O.some(`${basePath}page=${pageNumber}`),
  ),
});

const toFullPageViewModel = (
  dependencies: Dependencies,
  state: LimitedSet,
) => (itemCardViewModels: ReadonlyArray<ItemCardViewModel>) => {
  if (state.category === 'articles') {
    return toFullPageViewModelForArticlesCategory(dependencies, state)(itemCardViewModels);
  }
  return toFullPageViewModelForGroupsCategory(state)(itemCardViewModels);
};

export const fetchExtraDetails = (dependencies: Dependencies) => (state: LimitedSet): T.Task<ViewModel> => pipe(
  state.itemsToDisplay,
  T.traverseArray(constructItemCardViewModel(dependencies)),
  T.map(flow(
    RA.rights,
    toFullPageViewModel(dependencies, state),
  )),
);
