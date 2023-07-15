import * as T from 'fp-ts/Task';
import { DomainEvent } from '../../src/domain-events';
import { GetAllEvents, CommitEvents, Logger } from '../../src/shared-ports';
import { CommandResult } from '../../src/types/command-result';
import { dummyLogger } from '../dummy-logger';

const commitEvents = (
  inMemoryEvents: Array<DomainEvent>,
  dispatchToAllReadModels: (events: ReadonlyArray<DomainEvent>) => void,
  logger: Logger,
): CommitEvents => (events) => {
  if (events.length === 0) {
    return T.of('no-events-created' as CommandResult);
  }
  inMemoryEvents.push(...events);
  dispatchToAllReadModels(events);
  logger('info', 'Events committed', { events });
  return T.of('events-created' as CommandResult);
};

type DispatchToAllReadModels = (events: ReadonlyArray<DomainEvent>) => void;

type EventStore = {
  getAllEvents: GetAllEvents,
  commitEvents: CommitEvents,
};

export const createInMemoryEventStore = (dispatchToAllReadModels: DispatchToAllReadModels): EventStore => {
  const allEvents: Array<DomainEvent> = [];
  return {
    getAllEvents: T.of(allEvents),
    commitEvents: commitEvents(allEvents, dispatchToAllReadModels, dummyLogger),
  };
};
