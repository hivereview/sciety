import * as RA from 'fp-ts/ReadonlyArray';
import * as RS from 'fp-ts/ReadonlySet';
import * as T from 'fp-ts/Task';
import { flow, pipe } from 'fp-ts/function';
import * as S from 'fp-ts/string';
import { renderExampleSearches } from './render-example-searches';
import { DomainEvent, isUserSavedArticleEvent } from '../domain-events';
import { HtmlFragment, toHtmlFragment } from '../types/html-fragment';

const renderStatistics = (listCount: number) => toHtmlFragment(`
  <p class="visually-hidden">On Sciety you can find:</p>
  <ul role="list" class="home-page-hero__statistics">
    <li class="home-page-hero__statistic">
      <a href="https://blog.sciety.org/lists-on-sciety/" class="home-page-hero__statistic_link">
        <span class="home-page-hero__statistic_number">${listCount}</span><span class="home-page-hero__statistic_title"> user curated lists</span>
        <span class="visually-hidden">. Find out more about lists on Sciety.</span>
      </a>
    </li>
    <li class="home-page-hero__statistic">
      <a href="/groups" class="home-page-hero__statistic_link">
        <span class="home-page-hero__statistic_number">30k</span><span class="home-page-hero__statistic_title"> evaluations</span>
        <span class="visually-hidden">. Find out more about the groups producing them.</span>
      </a>
    </li>
    <li class="home-page-hero__statistic">
      <a href="/groups" class="home-page-hero__statistic_link">
        <span class="home-page-hero__statistic_number">20k</span><span class="home-page-hero__statistic_title"> evaluated articles</span>
        <span class="visually-hidden">. Find out more about the groups evaluating them.</span>
      </a>
    </li>
  </ul>
`);

const renderScietyFeedCTA = (): HtmlFragment => toHtmlFragment(
  '<a class="home-page-hero__sciety_feed_link" href="/sciety-feed">What\'s happening on Sciety?<a/>',
);

const renderHeroImage = () => `
  <picture class="home-page-hero__picture">
    <source srcset="/static/images/home-page-illustration.svg" media="(min-width: 60.25em)">
    <img src="data:" alt="" class="home-page-hero__image">
  </picture>
`;

const renderVideoCallToAction = () => `
  <div class="home-page-hero-with-video__video_call_to_action">
    <div class="home-page-hero-with-video__video_cta_text_wrapper">
      <p>Learn about Sciety.</p>
      <a href="">Play video</a></div>
    </div>
`;

type GetAllEvents = T.Task<ReadonlyArray<DomainEvent>>;

export type Ports = {
  getAllEvents: GetAllEvents,
};

type Hero = (ports: Ports) => T.Task<HtmlFragment>;

const renderHeroWithImage = (listCount: number) => `
<section class="home-page-hero">
<div class="home-page-hero__content">
  <h1 class="home-page-hero__content_title">
    Sciety: the home of public preprint evaluation
  </h1>
  <p class="home-page-hero__content_byline">
    Open evaluation and curation together in one place.
    <br>
    Let Sciety help you navigate the preprint landscape.
  </p>
  <p class="home-page-hero__content_byline">Follow the journey through <a href="/blog">our blog</a>.</p>
  ${renderStatistics(listCount)}
  ${renderScietyFeedCTA()}
  <form class="home-page-hero__search_form" action="/search" method="get">
    <input type="hidden" name="category" value="articles">
    <label for="searchText" class="visually-hidden">Search term</label>
    <input id="searchText" name="query" placeholder="Search for a topic of interest" class="home-page-hero__search_text">
    <input type="checkbox" name="evaluatedOnly" value="true" id="searchEvaluatedOnlyFilter">
    <label for="searchEvaluatedOnlyFilter" class="home-page-hero__search_form_label">Search only evaluated articles</label>
    <button type="submit" class="home-page-hero__search_button">Search</button>
    <button type="reset" class="visually-hidden">Reset</button>
  </form>
  ${renderExampleSearches()}
</div>
${process.env.EXPERIMENT_ENABLED === 'true' ? '' : renderHeroImage()}
</section>
`;

const renderHeroWithVideo = (listCount: number) => `
<section class="home-page-hero-with-video">
<div class="home-page-hero-with-video__content">
  <h1 class="home-page-hero-with-video__content_title">
    Sciety: the home of public preprint evaluation
  </h1>
  <p class="home-page-hero-with-video__content_byline">
    Open evaluation and curation together in one place.
    <br>
    Let Sciety help you navigate the preprint landscape.
  </p>
  <p class="home-page-hero-with-video__content_byline">Follow the journey through <a href="/blog">our blog</a>.</p>
  ${renderStatistics(listCount)}
  ${renderScietyFeedCTA()}
  <form class="home-page-hero-with-video__search_form" action="/search" method="get">
    <input type="hidden" name="category" value="articles">
    <label for="searchText" class="visually-hidden">Search term</label>
    <input id="searchText" name="query" placeholder="Search for a topic of interest" class="home-page-hero-with-video__search_text">
    <input type="checkbox" name="evaluatedOnly" value="true" id="searchEvaluatedOnlyFilter">
    <label for="searchEvaluatedOnlyFilter" class="home-page-hero-with-video__search_form_label">Search only evaluated articles</label>
    <button type="submit" class="home-page-hero-with-video__search_button">Search</button>
    <button type="reset" class="visually-hidden">Reset</button>
  </form>
  ${renderExampleSearches()}
  ${process.env.EXPERIMENT_ENABLED === 'true' ? renderVideoCallToAction() : ''}
</div>
</section>
`;

export const hero: Hero = (ports) => pipe(
  ports.getAllEvents,
  T.map(flow(
    RA.filter(isUserSavedArticleEvent),
    RA.map(({ userId }) => userId.toString()),
    RS.fromReadonlyArray(S.Eq),
    RS.size,
  )),
  T.map(flow(
    (listCount) => `
    ${process.env.EXPERIMENT_ENABLED === 'true' ? renderHeroWithVideo(listCount) : renderHeroWithImage(listCount)}
    
    `,
    toHtmlFragment,
  )),
);
