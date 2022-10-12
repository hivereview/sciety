import * as E from 'fp-ts/Either';
import { pipe } from 'fp-ts/function';
import * as PR from 'io-ts/PathReporter';
import { Command } from './execute-command';
import { removeArticleFromListCommandCodec } from '../commands';

type ValidateInputShape = (input: unknown) => E.Either<string, Command>;

export const validateInputShape: ValidateInputShape = (input) => pipe(
  input,
  removeArticleFromListCommandCodec.decode,
  E.mapLeft((errors) => PR.failure(errors).join('\n')),
);
