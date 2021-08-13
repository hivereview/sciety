import { sequenceS } from 'fp-ts/Apply';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { callsToAction } from './calls-to-action';
import { hero } from './hero';
import { personas } from './personas';
import { recentlyEvaluated } from './recently-evaluated';
import { userListCard } from './user-list-card';
import { DomainEvent } from '../domain-events';
import * as DE from '../types/data-error';
import { HtmlFragment, toHtmlFragment } from '../types/html-fragment';
import { Page } from '../types/page';
import { toUserId, UserId } from '../types/user-id';

type GetAllEvents = T.Task<ReadonlyArray<DomainEvent>>;

const listCards = (ports: Ports) => pipe(
  {
    prachee: userListCard(ports)(
      toUserId('1412019815619911685'),
      'See what researchers at Prachee Avasthi’s lab are reading to discover some interesting new work',
    ),
    kenton: userListCard(ports)(
      toUserId('1417520401282854918'),
      'Some interesting preprints on ion channel proteins',
    ),
    marius: userListCard(ports)(
      toUserId('1223116442549145601'),
      'A list of papers on innate immunology curated by Ailís O’Carroll',
    ),
  },
  sequenceS(TE.ApplyPar),
);

type Components = {
  hero: HtmlFragment,
  cards: HtmlFragment,
  personas: HtmlFragment,
  callsToAction: HtmlFragment,
};

const renderContent = (components: Components) => toHtmlFragment(`
  <div class="landing-page">
    ${components.hero}
    ${components.cards}
    ${components.personas}
    ${components.callsToAction}
  </div>
`);

type GetUserDetails = (userId: UserId) => TE.TaskEither<DE.DataError, { avatarUrl: string, handle: string }>;

type Ports = {
  getAllEvents: GetAllEvents,
  getUserDetails: GetUserDetails,
};

export const landingPage = (ports: Ports): T.Task<Page> => pipe(
  {
    hero: T.of(hero),
    cards: pipe(
      listCards(ports),
      T.map(recentlyEvaluated),
    ),
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
