import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { renderComponent } from './render-component';
import { DomainEvent } from '../../domain-events';
import { defaultGroupListDescription } from '../../group-page/messages';
import { groupList } from '../../shared-read-models/group-list';
import { Group } from '../../types/group';
import { HtmlFragment } from '../../types/html-fragment';

export type Ports = {
  getAllEvents: T.Task<ReadonlyArray<DomainEvent>>,
};

export const component = (
  ports: Ports,
  group: Group,
): TE.TaskEither<never, HtmlFragment> => pipe(
  ports.getAllEvents,
  T.map((events) => ({
    name: 'Evaluated Articles',
    description: defaultGroupListDescription(group.name),
    ownerName: group.name,
    ownerHref: `/groups/${group.slug}`,
    ownerAvatarPath: group.avatarPath,
    ...groupList(group.id)(events),
  })),
  T.map(renderComponent),
  TE.rightTask,
);
