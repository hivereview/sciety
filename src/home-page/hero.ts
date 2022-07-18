import { HtmlFragment, toHtmlFragment } from '../types/html-fragment';

const renderVideoCallToAction = () => `
  <div class="home-page-hero__video_call_to_action">
    <div class="home-page-hero__video_cta_text_wrapper">
      <p>Learn about Sciety.</p>
      <a href="/learn-about">Play video<img src="/static/images/play-button.svg" alt=""/></a></div>
    </div>
`;

export const hero: HtmlFragment = toHtmlFragment(`
  <section class="home-page-hero">
    <div class="home-page-hero__content">
      <div class="home-page-hero__left_wrapper">
        <h1 class="home-page-hero__content_title">
          The home of public preprint evaluation
        </h1>
        <p class="home-page-hero__content_byline">
          Explore and curate evaluated preprints.
        </p>
        <h2>Find evaluated preprints</h2>
        <form class="home-page-hero__search_form" action="/search" method="get">
          <input type="hidden" name="category" value="articles">
          <label for="searchText" class="visually-hidden">Search term</label>
          <input id="searchText" name="query" placeholder="Search for a topic of interest" class="home-page-hero__search_text">
          <input type="hidden" name="evaluatedOnly" value="true">
          <button type="submit" class="home-page-hero__search_button">Search</button>
          <button type="reset" class="visually-hidden">Reset</button>
        </form>
      </div>

      <div class="home-page-hero__right_wrapper">
        ${renderVideoCallToAction()}
      </div>
    </div>
  </section>
`);
