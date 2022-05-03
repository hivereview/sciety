import * as E from 'fp-ts/Either';
import * as T from 'fp-ts/Task';
import * as TE from 'fp-ts/TaskEither';
import { pipe } from 'fp-ts/function';
import * as t from 'io-ts';
import { CreateAnnotationCommand, executeCreateAnnotationCommand } from './execute-create-annotation-command';
import { Logger } from '../infrastructure';
import { CommitEvents, GetAllEvents } from '../shared-ports';
import { DoiFromString } from '../types/codecs/DoiFromString';
import { CommandResult } from '../types/command-result';
import { Doi } from '../types/doi';
import { HtmlFragment, htmlFragmentCodec } from '../types/html-fragment';
import { fromValidatedString } from '../types/list-id';

type Body = {
  annotationContent: HtmlFragment,
  articleId: Doi,
};

const bodyCodec = t.type({
  annotationContent: htmlFragmentCodec,
  articleId: DoiFromString,
});

const transformToCommand = ({ annotationContent, articleId }: Body): CreateAnnotationCommand => ({
  content: annotationContent,
  target: {
    articleId,
    listId: fromValidatedString('1af5b971-162e-4cf3-abdf-57e3bbfcd0d7'),
  },
});

type Ports = {
  logger: Logger,
  getAllEvents: GetAllEvents,
  commitEvents: CommitEvents,
};

type HandleCreateAnnotationCommand = (ports: Ports) => (input: unknown) => TE.TaskEither<unknown, CommandResult>;

export const handleCreateAnnotationCommand: HandleCreateAnnotationCommand = (ports) => (input) => pipe(
  input,
  bodyCodec.decode,
  E.map(transformToCommand),
  TE.fromEither,
  TE.chainFirstTaskK(
    (command) => T.of(
      ports.logger('debug', 'Received CreateAnnotation command', { command }),
    ),
  ),
  TE.chainTaskK((command) => pipe(
    ports.getAllEvents,
    T.map(executeCreateAnnotationCommand(command)),
  )),
  TE.chainTaskK(ports.commitEvents),
  TE.map(() => 'no-events-created'),
);
