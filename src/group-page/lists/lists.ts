import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { DomainEvent } from '../../domain-events';
import { renderListCard } from '../../shared-components/list-card/render-list-card';
import { templateListItems } from '../../shared-components/list-items';
import { allLists, Ports as GroupListPorts } from '../../shared-read-models/all-lists';
import * as DE from '../../types/data-error';
import { Group } from '../../types/group';
import { HtmlFragment, toHtmlFragment } from '../../types/html-fragment';

type GetAllEvents = T.Task<ReadonlyArray<DomainEvent>>;

export type Ports = GroupListPorts & {
  getAllEvents: GetAllEvents,
};

const renderCards = (cards: ReadonlyArray<HtmlFragment>) => pipe(
  cards,
  (items) => templateListItems(items, 'group-page-followers-list__item'),
  (listContent) => `
    <section class="group-page-lists">
      <ul class="group-page-followers-list" role="list">
        ${listContent}
      </ul>
    </section>
  `,
  toHtmlFragment,
);

export const lists = (ports: Ports) => (group: Group): TE.TaskEither<DE.DataError, HtmlFragment> => pipe(
  ports.getAllEvents,
  TE.rightTask,
  TE.chain(allLists(ports, group.id)),
  TE.map((details) => ({
    ...details,
    href: `/groups/${group.slug}/evaluated-articles`,
    title: details.name,
    articleCountLabel: 'This group has evaluated',
  })),
  TE.map(renderListCard),
  TE.map((card) => [card]),
  TE.map(renderCards),
);
