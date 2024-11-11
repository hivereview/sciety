import { pipe } from 'fp-ts/function';
import { toHtmlFragment } from '../../../types/html-fragment';
import { PageLayout } from '../page-layout';
import { commonLayout } from '../shared-components/common-layout';

export const renderHomePageLayout: PageLayout = (viewModel) => (page) => pipe(
  `
  <main id="mainContent">
    ${page.content}
  </main>
  `,
  toHtmlFragment,
  commonLayout('home-page-container', viewModel.userDetails, 'dark'),
);
