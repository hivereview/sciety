import { HtmlFragment, toHtmlFragment } from '../types/html-fragment';
import { Page } from '../types/page';

type Components = {
  header: HtmlFragment,
  mainContent: HtmlFragment,
  userDisplayName: string,
  description: string,
};

export const renderPage = (components: Components): Page => ({
  title: components.userDisplayName,
  openGraph: {
    title: components.userDisplayName,
    description: components.description,
  },
  content: toHtmlFragment(`
    <article class="sciety-grid-two-columns">
      ${components.header}

      <div class="main-content main-content--user">
        ${components.mainContent}
      </div>

    </article>
  `),
});
