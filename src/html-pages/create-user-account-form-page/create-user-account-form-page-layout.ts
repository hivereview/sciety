import { pipe } from 'fp-ts/function';
import { siteFooter } from '../../shared-components/site-footer';
import { siteHeader } from '../../shared-components/site-header';
import { PageLayout } from '../page-layout';
import { wrapInHtmlDocument } from '../wrap-in-html-document';
import { toContentWrappedInLayout } from '../content-wrapped-in-layout';

export const createUserAccountFormPageLayout: PageLayout = (user) => (page) => pipe(
  `
  <div class="create-user-account-form-page__container">
    ${siteHeader(user)}

    <main id="mainContent" class="create-user-account-form-page__main">
      <div class="page-content">
        ${page.content}
      </div>
    </main>

    ${siteFooter(user)}
  </div>
  `,
  toContentWrappedInLayout,
  wrapInHtmlDocument(user, page),
);
