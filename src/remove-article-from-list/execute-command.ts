import * as RA from 'fp-ts/ReadonlyArray';
import * as B from 'fp-ts/boolean';
import { pipe } from 'fp-ts/function';
import { articleRemovedFromList, DomainEvent } from '../domain-events';
import { ListResource } from '../shared-write-models/list-resource';
import { Doi } from '../types/doi';
import { ListId } from '../types/list-id';

type Command = {
  articleId: Doi,
  listId: ListId,
};

type ExecuteCommand = (
  command: { listId: ListId, articleId: Doi },
) => (
  listResource: ListResource,
) => ReadonlyArray<DomainEvent>;

const createAppropriateEvents = (command: Command) => (listResource: ListResource) => pipe(
  listResource.articleIds,
  RA.some((articleId) => articleId.value === command.articleId.value),
  B.fold(
    () => [],
    () => [articleRemovedFromList(command.articleId, command.listId)],
  ),
);

export const executeCommand: ExecuteCommand = (command) => (listResource) => pipe(
  listResource,
  createAppropriateEvents(command),
);
