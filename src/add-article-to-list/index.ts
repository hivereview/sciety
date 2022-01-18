import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import { executeCommand } from './execute-command';
import { validateInputShape } from './validate-input-shape';
import { DomainEvent, RuntimeGeneratedEvent } from '../domain-events';
import { CommandResult } from '../types/command-result';

type CommitEvents = (event: ReadonlyArray<RuntimeGeneratedEvent>) => T.Task<CommandResult>;

type Ports = {
  getAllEvents: T.Task<ReadonlyArray<DomainEvent>>,
  commitEvents: CommitEvents,
};

type AddArticleToList = (ports: Ports) => (input: unknown) => TE.TaskEither<string, CommandResult>;

export const addArticleToList: AddArticleToList = (ports) => (input) => pipe(
  input,
  validateInputShape,
  TE.fromEither,
  TE.chainW((command) => pipe(
    ports.getAllEvents,
    T.map(executeCommand(command)),
  )),
  TE.chainTaskK(ports.commitEvents),
);
