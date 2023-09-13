import { pipe } from 'fp-ts/function';
import * as RA from 'fp-ts/ReadonlyArray';
import { HtmlFragment, toHtmlFragment } from '../../../types/html-fragment';
import { renderListItems } from '../../../shared-components/render-list-items';
import { GroupLinkWithLogoViewModel, renderGroupLinkWithLogo } from '../../../shared-components/group-link';

type GroupsViewModel = ReadonlyArray<GroupLinkWithLogoViewModel>;

export const renderGroups = (viewModel: GroupsViewModel): HtmlFragment => pipe(
  viewModel,
  RA.map(renderGroupLinkWithLogo('center')),
  RA.map(toHtmlFragment),
  renderListItems,
  (listContent) => `
<section class="home-page-groups">
  <h2 class="home-page-groups__title">Groups evaluating preprints on Sciety</h2>
  <ul class="home-page-groups-list">
  ${listContent}
  </ul>
  <div class="home-page-groups__button_wrapper">
    <a href="/groups" class="home-page-groups__button">See all evaluating groups</a>
  </div>
</section>
`,
  toHtmlFragment,
);
