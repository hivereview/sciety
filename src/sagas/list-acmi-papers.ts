import * as TE from 'fp-ts/TaskEither';
import { CommandResult } from '../types/command-result';
import { ErrorMessage } from '../types/error-message';

export const listAcmiPapers = (): TE.TaskEither<ErrorMessage, CommandResult> => TE.right('no-events-created');
