import * as E from 'fp-ts/Either';
import { pipe } from 'fp-ts/function';
import { listCreated } from '../../../src/domain-events/list-created-event';
import { executeCommand } from '../../../src/write-side/edit-list-details/execute-command';
import { replayListResource } from '../../../src/write-side/resources/replay-list-resource';
import { arbitraryString } from '../../helpers';
import { shouldNotBeCalled } from '../../should-not-be-called';
import { arbitraryListId } from '../../types/list-id.helper';
import { arbitraryListOwnerId } from '../../types/list-owner-id.helper';

describe('execute-command', () => {
  const listId = arbitraryListId();
  const listName = arbitraryString();
  const listDescription = arbitraryString();

  describe('when the new name is different from the current name', () => {
    const newName = arbitraryString();
    const command = {
      name: newName,
      description: listDescription,
      listId,
    };

    const raisedEvents = pipe(
      [
        listCreated(listId, arbitraryString(), listDescription, arbitraryListOwnerId()),
      ],
      replayListResource(listId),
      E.map(executeCommand(command)),
      E.getOrElseW(shouldNotBeCalled),
    );

    it('raises an event with the new name', () => {
      expect(raisedEvents).toStrictEqual([expect.objectContaining({ name: newName, type: 'ListNameEdited' })]);
    });
  });

  describe('when the new description is different from the current description', () => {
    const newDescription = arbitraryString();
    const command = {
      name: listName,
      description: newDescription,
      listId,
    };

    const raisedEvents = pipe(
      [
        listCreated(listId, listName, arbitraryString(), arbitraryListOwnerId()),
      ],
      replayListResource(listId),
      E.map(executeCommand(command)),
      E.getOrElseW(shouldNotBeCalled),
    );

    it('raises an event with the new description', () => {
      expect(raisedEvents).toStrictEqual([expect.objectContaining({ description: newDescription, type: 'ListDescriptionEdited' })]);
    });
  });

  describe('when the new name and description are the same as the current details', () => {
    it('raises no events', () => {
      const command = {
        name: listName,
        description: listDescription,
        listId,
      };
      const eventsToBeRaised = pipe(
        [
          listCreated(listId, listName, listDescription, arbitraryListOwnerId()),
        ],
        replayListResource(listId),
        E.map(executeCommand(command)),
        E.getOrElseW(shouldNotBeCalled),
      );

      expect(eventsToBeRaised).toStrictEqual([]);
    });
  });

  describe('when both name and description are different from the current details', () => {
    const newName = arbitraryString();
    const newDescription = arbitraryString();
    const command = {
      name: newName,
      description: newDescription,
      listId,
    };

    const raisedEvents = pipe(
      [
        listCreated(listId, arbitraryString(), arbitraryString(), arbitraryListOwnerId()),
      ],
      replayListResource(listId),
      E.map(executeCommand(command)),
      E.getOrElseW(shouldNotBeCalled),
    );

    it('raises two events with the new details', () => {
      expect(raisedEvents).toStrictEqual([
        expect.objectContaining({ name: newName, type: 'ListNameEdited' }),
        expect.objectContaining({ description: newDescription, type: 'ListDescriptionEdited' }),
      ]);
    });
  });
});
