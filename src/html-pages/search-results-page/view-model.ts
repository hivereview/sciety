import { PaginationViewModel } from './render-as-html/wrap-with-pagination-information';
import { GroupLinkWithLogoViewModel } from '../../read-side/html-pages/shared-components/group-link';
import { LegacyPaginationControlsViewModel } from '../../read-side/html-pages/shared-components/pagination';
import { PaperActivitySummaryCardViewModel } from '../../shared-components/paper-activity-summary-card';

type NoGroupsEvaluatedTheFoundArticles = {
  tag: 'no-groups-evaluated-the-found-articles',
};

export type SomeRelatedGroups = {
  tag: 'some-related-groups',
  items: ReadonlyArray<GroupLinkWithLogoViewModel>,
};

export type ViewModel = PaginationViewModel & LegacyPaginationControlsViewModel & {
  query: string,
  evaluatedOnly: boolean,
  paperActivitySummaryCards: ReadonlyArray<PaperActivitySummaryCardViewModel>,
  relatedGroups: NoGroupsEvaluatedTheFoundArticles | SomeRelatedGroups,
};
