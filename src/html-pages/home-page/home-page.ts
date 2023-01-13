import { pipe } from 'fp-ts/function';
import { cards, Ports as CardsPorts } from './cards';
import { hero } from './hero';
import { HtmlFragment, toHtmlFragment } from '../../types/html-fragment';
import { Page } from '../../types/page';

type Components = {
  hero: HtmlFragment,
  cards: HtmlFragment,
};

type Ports = CardsPorts;

const renderContent = (components: Components) => toHtmlFragment(`
  <div class="home-page">
    ${components.hero}
    ${components.cards}
  </div>
`);

export const homePage = (ports: Ports): Page => pipe(
  {
    hero,
    cards: cards(ports),
  },
  renderContent,
  (content) => ({
    title: 'Sciety: the home of public preprint evaluation',
    content,
  }),
);
