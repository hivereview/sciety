import { sequenceS } from 'fp-ts/Apply';
import * as T from 'fp-ts/Task';
import { pipe } from 'fp-ts/function';
import { callsToAction } from './calls-to-action';
import { cards, Ports as CardsPorts } from './cards';
import { hero, Ports as HeroPorts } from './hero';
import { personas } from './personas';
import { HtmlFragment, toHtmlFragment } from '../types/html-fragment';
import { Page } from '../types/page';

type Components = {
  hero: HtmlFragment,
  cards: HtmlFragment,
  personas: HtmlFragment,
  callsToAction: HtmlFragment,
};

type Ports = CardsPorts & HeroPorts;

const renderContent = (components: Components) => toHtmlFragment(`
  <div class="landing-page">
    ${components.hero}
    ${components.cards}
    ${components.personas}
    ${components.callsToAction}
  </div>
`);

export const landingPage = (ports: Ports): T.Task<Page> => pipe(
  {
    hero: hero(ports),
    cards: cards(ports),
    personas: T.of(personas),
    callsToAction: T.of(callsToAction),
  },
  sequenceS(T.ApplyPar),
  T.map(renderContent),
  T.map((content) => ({
    title: 'Sciety: the home of public preprint evaluation',
    content,
  })),
);
