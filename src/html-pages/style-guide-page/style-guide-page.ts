import * as O from 'fp-ts/Option';
import { sanitise } from '../../types/sanitised-html-fragment';
import { renderArticleCard } from '../../shared-components/article-card/render-article-card';
import { renderPaginationControlsForFeed } from '../../shared-components/pagination/render-pagination-controls-for-feed';
import { toHtmlFragment } from '../../types/html-fragment';
import { Page } from '../../types/page';
import { Doi } from '../../types/doi';
import { renderArticleErrorCard } from '../../shared-components/article-card';
import * as LID from '../../types/list-id';
import * as DE from '../../types/data-error';
import {
  renderArticleCardWithControlsAndAnnotation,
} from '../../shared-components/article-card-with-controls-and-annotation';
import { renderPage } from '../create-annotation-form-page/render-page';

export const styleGuidePage: Page = {
  title: 'Style guide',
  content: toHtmlFragment(`
<style>
  ._style-guide-heading {
    font-family: monospace;
    margin-top: 3rem;
    margin-bottom: 3rem;
    margin-left: -3rem;
    background-color: wheat;
    color: teal;
    padding: 1.5rem 3rem;
  }
</style>
    <header class="page-header">
      <h1>Style guide</h1>
    </header>
    <div>
      <h2 class="_style-guide-heading">Pagination controls for feed</h2>
      <h3 class="_style-guide-heading">With a link only to older content</h3>
      ${renderPaginationControlsForFeed({
    prevPageHref: O.none, nextPageHref: O.some('/foo'), page: 1, pageCount: 42,
  })}
      <h3 class="_style-guide-heading">With a link only to newer content</h3>
      ${renderPaginationControlsForFeed({
    prevPageHref: O.some('/foo'), nextPageHref: O.none, page: 2, pageCount: 2,
  })}
      <h3 class="_style-guide-heading">With links to newer and older content</h3>
      ${renderPaginationControlsForFeed({
    prevPageHref: O.some('/foo'), nextPageHref: O.some('/foo'), page: 2, pageCount: 42,
  })}
      <h2 class="_style-guide-heading">Pagination controls [default]</h2>
      <h3 class="_style-guide-heading">With a link only to the next page</h3>
      ${renderPaginationControlsForFeed({
    prevPageHref: O.none, nextPageHref: O.some('/foo'), page: 1, pageCount: 42,
  })}
      <h2 class="_style-guide-heading">Article summary</h2>
      <h3 class="_style-guide-heading">With curation statement</h3>
      ${renderArticleCard({
    articleId: new Doi('10.1101/foo'),
    articleHref: '/articles/foo',
    title: sanitise(toHtmlFragment('Some title')),
    authors: O.some(['Doctor Smith']),
    latestVersionDate: O.some(new Date('2023-09-11')),
    latestActivityAt: O.some(new Date('2023-09-10')),
    evaluationCount: O.some(1),
    listMembershipCount: O.some(1),
    curationStatementsTeasers: [{
      groupPageHref: '/foo',
      groupName: 'Awesome group',
      quote: sanitise(toHtmlFragment(`<p><strong>elife assessment:</strong></p>
          <p>This small-sized clinical trial comparing nebulized dornase-alfa to best available care in patients hospitalized with COVID-19 pneumonia is valuable, but in its present form the paper is incomplete: the number of randomized participants is small, investigators describe also a contemporary cohort of controls and the study concludes about decrease of inflammation (reflected by CRP levels) after 7 days of treatment but no other statistically significant clinical benefit.</p>`)),
      quoteLanguageCode: O.some('en'),
    }],
    reviewingGroups: [],
  })}
  
      <h3 class="_style-guide-heading">With reviewing groups</h3>
      ${renderArticleCard({
    articleId: new Doi('10.1101/foo'),
    articleHref: '/articles/foo',
    title: sanitise(toHtmlFragment('Some title')),
    authors: O.some(['Doctor Smith']),
    latestVersionDate: O.some(new Date('2023-09-11')),
    latestActivityAt: O.some(new Date('2023-09-10')),
    evaluationCount: O.some(1),
    listMembershipCount: O.some(1),
    curationStatementsTeasers: [],
    reviewingGroups: [
      {
        href: '/anything',
        groupName: 'Awesome group',
      },
      {
        href: '/anything',
        groupName: 'Awesome society',
      },
    ],
  })}

      <h3 class="_style-guide-heading">With trashcan</h3>
      ${renderArticleCardWithControlsAndAnnotation({
    annotation: O.none,
    articleCard: {
      articleId: new Doi('10.1101/foo'),
      articleHref: '/articles/foo',
      title: sanitise(toHtmlFragment('Some title')),
      authors: O.some(['Doctor Smith']),
      latestVersionDate: O.some(new Date('2023-09-11')),
      latestActivityAt: O.some(new Date('2023-09-10')),
      evaluationCount: O.some(1),
      listMembershipCount: O.some(1),
      curationStatementsTeasers: [],
      reviewingGroups: [],
    },
    controls: O.some({
      listId: LID.fromValidatedString('ee7e738a-a1f1-465b-807c-132d273ca952'),
      articleId: new Doi('10.1101/foo'),
      createAnnotationFormHref: O.some('#'),
    }),
  })}

      <h3 class="_style-guide-heading">With annotation</h3>
      ${renderArticleCardWithControlsAndAnnotation({
    annotation: O.some({
      content: toHtmlFragment('There are few things I enjoy more than a comparative analysis of actin probes. Another of my all time favorites is this: https://www.tandfonline.com/doi/full/10.1080/19490992.2014.1047714'),
      author: 'AvasthiReading',
      authorAvatarPath: '/static/images/profile-dark.svg',
    }),
    articleCard: {
      articleId: new Doi('10.1101/foo'),
      articleHref: '/articles/foo',
      title: sanitise(toHtmlFragment('Some title')),
      authors: O.some(['Doctor Smith']),
      latestVersionDate: O.some(new Date('2023-09-11')),
      latestActivityAt: O.some(new Date('2023-09-10')),
      evaluationCount: O.some(1),
      listMembershipCount: O.some(1),
      curationStatementsTeasers: [],
      reviewingGroups: [],
    },
    controls: O.none,
  })}

      <h3 class="_style-guide-heading">With annotation and controls</h3>
      ${renderArticleCardWithControlsAndAnnotation({
    annotation: O.some({
      content: toHtmlFragment('There are few things I enjoy more than a comparative analysis of actin probes. Another of my all time favorites is this: https://www.tandfonline.com/doi/full/10.1080/19490992.2014.1047714'),
      author: 'AvasthiReading',
      authorAvatarPath: '/static/images/profile-dark.svg',
    }),
    articleCard: {
      articleId: new Doi('10.1101/foo'),
      articleHref: '/articles/foo',
      title: sanitise(toHtmlFragment('Some title')),
      authors: O.some(['Doctor Smith']),
      latestVersionDate: O.some(new Date('2023-09-11')),
      latestActivityAt: O.some(new Date('2023-09-10')),
      evaluationCount: O.some(1),
      listMembershipCount: O.some(1),
      curationStatementsTeasers: [],
      reviewingGroups: [],
    },
    controls: O.some({
      articleId: new Doi('10.1101/foo'),
      listId: LID.fromValidatedString('ee7e738a-a1f1-465b-807c-132d273ca952'),
      createAnnotationFormHref: O.none,
    }),
  })}
  
      <h3 class="_style-guide-heading">With error</h3>
      ${renderArticleErrorCard({
    evaluationCount: 1,
    href: '/articles/foo',
    latestActivityAt: O.some(new Date('2023-09-10')),
    error: DE.notFound,
    articleId: new Doi('10.1101/foo'),
  })}

      <h2 class="_style-guide-heading">Forms</h2>
      <h3 class="_style-guide-heading">Create annotation</h3>
      ${renderPage({
    articleId: new Doi('10.1101/1234'),
    listId: LID.fromValidatedString('foo'),
    articleTitle: sanitise(toHtmlFragment('New Article')),
    listName: 'Someone\'s saved articles',
  })}
    </div>
  `),
};
