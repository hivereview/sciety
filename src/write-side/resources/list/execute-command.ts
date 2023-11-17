import { EditListDetailsCommand } from '../../commands/index.js';
import { DomainEvent, constructEvent } from '../../../domain-events/index.js';
import { ListWriteModel } from './list-write-model.js';

type ExecuteCommand = (command: EditListDetailsCommand)
=> (listResource: ListWriteModel)
=> ReadonlyArray<DomainEvent>;

const handleEditingOfName = (listResource: ListWriteModel, command: EditListDetailsCommand) => (
  (listResource.name === command.name)
    ? []
    : [constructEvent('ListNameEdited')({ listId: command.listId, name: command.name })]
);

const handleEditingOfDescription = (listResource: ListWriteModel, command: EditListDetailsCommand) => (
  (listResource.description === command.description)
    ? []
    : [constructEvent('ListDescriptionEdited')({ listId: command.listId, description: command.description })]
);

export const executeCommand: ExecuteCommand = (command) => (listResource) => [
  ...handleEditingOfName(listResource, command),
  ...handleEditingOfDescription(listResource, command),
];
