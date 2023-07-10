import { pipe } from 'fp-ts/function';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import * as RA from 'fp-ts/ReadonlyArray';
import { dispatcher, Queries } from '../../src/shared-read-models';
import * as groupResource from '../../src/write-side/resources/group';
import { DomainEvent } from '../../src/domain-events';
import { GetAllEvents, CommitEvents } from '../../src/shared-ports';
import { CommandResult } from '../../src/types/command-result';
import { createUserAccountCommandHandler } from '../../src/write-side/create-user-account';
import { followCommandHandler } from '../../src/write-side/follow/follow-command-handler';
import {
  updateUserDetailsCommandHandler,
  recordEvaluationCommandHandler,
  createListCommandHandler,
  removeArticleFromListCommandHandler,
} from '../../src/write-side/command-handlers';
import { unfollowCommandHandler } from '../../src/write-side/follow/unfollow-command-handler';
import { CommandHandler } from '../../src/types/command-handler';
import { AddGroupCommand, UpdateGroupDetailsCommand } from '../../src/write-side/commands';
import { addArticleToListCommandHandler } from '../../src/write-side/command-handlers/add-article-to-list-command-handler';

const commitEvents = (
  inMemoryEvents: Array<DomainEvent>,
  dispatchToAllReadModels: (events: ReadonlyArray<DomainEvent>) => void,
): CommitEvents => (events) => pipe(
  events,
  RA.match(
    () => ('no-events-created' as CommandResult),
    (es) => {
      pipe(
        es,
        RA.map((event) => { inMemoryEvents.push(event); return event; }),
      );
      dispatchToAllReadModels(es);
      return 'events-created' as CommandResult;
    },
  ),
  T.of,
);

type EventStore = {
  getAllEvents: GetAllEvents,
  commitEvents: CommitEvents,
};

type CreateGroup = (adapters: EventStore) => CommandHandler<AddGroupCommand>;

const createGroup: CreateGroup = (adapters) => (command) => pipe(
  adapters.getAllEvents,
  T.map(groupResource.create(command)),
  TE.chainTaskK(adapters.commitEvents),
);

type UpdateGroupDetails = (adapters: EventStore) => CommandHandler<UpdateGroupDetailsCommand>;

const updateGroupDetails: UpdateGroupDetails = (adapters) => (command) => pipe(
  adapters.getAllEvents,
  T.map(groupResource.update(command)),
  TE.chainTaskK(adapters.commitEvents),
);

const instantiateCommandHandlers = (eventStore: EventStore, queries: Queries) => ({
  addArticleToList: addArticleToListCommandHandler(eventStore),
  createGroup: createGroup(eventStore),
  createList: createListCommandHandler(eventStore),
  createUserAccount: createUserAccountCommandHandler(eventStore),
  followGroup: followCommandHandler(eventStore),
  recordEvaluation: recordEvaluationCommandHandler({ ...eventStore, ...queries }),
  removeArticleFromList: removeArticleFromListCommandHandler(eventStore),
  unfollowGroup: unfollowCommandHandler(eventStore),
  updateGroupDetails: updateGroupDetails(eventStore),
  updateUserDetails: updateUserDetailsCommandHandler(eventStore),
});

export type ReadAndWriteSides = {
  commandHandlers: ReturnType<typeof instantiateCommandHandlers>,
  getAllEvents: GetAllEvents,
  queries: Queries,
};

export const createReadAndWriteSides = (): ReadAndWriteSides => {
  const allEvents: Array<DomainEvent> = [];
  const { dispatchToAllReadModels, queries } = dispatcher();
  const eventStore: EventStore = {
    getAllEvents: T.of(allEvents),
    commitEvents: commitEvents(allEvents, dispatchToAllReadModels),
  };
  const commandHandlers = instantiateCommandHandlers(eventStore, queries);
  return {
    commandHandlers,
    getAllEvents: eventStore.getAllEvents,
    queries,
  };
};
