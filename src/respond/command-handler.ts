import * as O from 'fp-ts/Option';
import * as T from 'fp-ts/Task';
import { flow, pipe } from 'fp-ts/function';
import * as t from 'io-ts';
import { isString } from 'is-what';
import { respondHelpful } from './respond-helpful-command';
import { respondNotHelpful } from './respond-not-helpful-command';
import { reviewResponse } from './review-response';
import { revokeResponse } from './revoke-response-command';
import {
  DomainEvent,
  UserFoundReviewHelpfulEvent,
  UserFoundReviewNotHelpfulEvent,
  UserRevokedFindingReviewHelpfulEvent,
  UserRevokedFindingReviewNotHelpfulEvent,
} from '../domain-events';
import { reviewIdCodec } from '../types/review-id';
import { User } from '../types/user';

export type GetAllEvents = T.Task<ReadonlyArray<DomainEvent>>;

const commands = {
  'respond-helpful': respondHelpful,
  'respond-not-helpful': respondNotHelpful,
  'revoke-response': revokeResponse,
};

type CommandType = keyof typeof commands;

const isCommand = (command: string): command is CommandType => command in commands;

export const toCommand = flow(
  O.of,
  O.filter(isString),
  O.filter(isCommand),
);

const commandCodec = t.type({
  reviewId: reviewIdCodec,
  type: t.union([
    t.literal('respond-helpful'),
    t.literal('respond-not-helpful'),
    t.literal('revoke-response'),
  ]),
});

type Command = t.TypeOf<typeof commandCodec>;

type GeneratedEvents = (
  UserFoundReviewHelpfulEvent |
  UserFoundReviewNotHelpfulEvent |
  UserRevokedFindingReviewHelpfulEvent |
  UserRevokedFindingReviewNotHelpfulEvent
);

type CommandHandler = (
  user: User,
  command: Command,
) => T.Task<ReadonlyArray<GeneratedEvents>>;

type Ports = {
  getAllEvents: GetAllEvents,
};

export const commandHandler = (
  ports: Ports,
): CommandHandler => (
  user,
  command,
) => pipe(
  ports.getAllEvents,
  T.map(reviewResponse(user.id, command.reviewId)),
  T.map((currentResponse) => commands[command.type](currentResponse, user.id, command.reviewId)),
);
