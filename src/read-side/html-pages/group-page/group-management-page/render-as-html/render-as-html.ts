import { renderCurrentlyFeaturedLists } from './render-currently-featured-lists';
import { renderFeatureAList } from './render-feature-a-list';
import { toHtmlFragment } from '../../../../../types/html-fragment';
import { HtmlPage, toHtmlPage } from '../../../html-page';
import { ViewModel } from '../view-model';

export const renderAsHtml = (viewModel: ViewModel): HtmlPage => toHtmlPage({
  title: viewModel.pageHeading,
  content: toHtmlFragment(`
  <header class="page-header">
    <h1>${viewModel.pageHeading}</h1>
  </header>
  <p>
    From here you can manage your group pages on Sciety.
    You are able to see this page as you are a designated representative for a Sciety group.
  </p>
  <p>
    <a href="${viewModel.groupHomePageHref}">View public group page</a>
  </p>
  <section class="group-management-section">
    ${renderCurrentlyFeaturedLists(viewModel.currentlyFeaturedLists)}
  </section>
  <section class="group-management-section">
    ${renderFeatureAList(viewModel)}
  </section>
  `),
});
