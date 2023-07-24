import * as E from 'fp-ts/Either';
import { pipe } from 'fp-ts/function';
import * as LID from '../../../types/list-id';
import * as LOID from '../../../types/list-owner-id';
import { AddGroupCommand } from '../../commands';
import { constructEvent } from '../../../domain-events';
import * as AG from '../all-groups';
import { ResourceAction } from '../resource-action';

export const create: ResourceAction<AddGroupCommand> = (command) => (events) => pipe(
  AG.replay(events),
  AG.check(command),
  E.map(LID.generate),
  E.map((listId) => [
    constructEvent('GroupJoined')({ ...command, date: command.issuedAt ?? new Date() }),
    constructEvent('ListCreated')({
      listId,
      name: 'Evaluated articles',
      description: `Articles that have been evaluated by ${command.name}`,
      ownerId: LOID.fromGroupId(command.groupId),
      date: command.issuedAt ?? new Date(),
    }),
    constructEvent('EvaluatedArticlesListSpecified')({ listId, groupId: command.groupId }),
  ]),
);
