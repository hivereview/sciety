import * as E from 'fp-ts/Either';
import { pipe } from 'fp-ts/function';
import { create } from '../../../../src/write-side/resources/group/create';
import { constructEvent, ListCreatedEvent } from '../../../../src/domain-events';
import { arbitraryString, arbitraryWord } from '../../../helpers';
import { shouldNotBeCalled } from '../../../should-not-be-called';
import { arbitraryGroupId } from '../../../types/group-id.helper';
import * as LOID from '../../../../src/types/list-owner-id';
import { arbitraryGroup } from '../../../types/group.helper';

describe('create', () => {
  const newGroup = arbitraryGroup();
  const addGroupCommand = {
    groupId: newGroup.id,
    name: newGroup.name,
    shortDescription: newGroup.shortDescription,
    homepage: newGroup.homepage,
    avatarPath: newGroup.avatarPath,
    descriptionPath: newGroup.descriptionPath,
    slug: newGroup.slug,
  };

  describe('when the group does not exist', () => {
    const result = pipe(
      [],
      create(addGroupCommand),
      E.getOrElseW(shouldNotBeCalled),
    );

    it('creates the group', () => {
      expect(result[0]).toStrictEqual(expect.objectContaining({
        type: 'GroupJoined',
        groupId: newGroup.id,
        name: newGroup.name,
        shortDescription: newGroup.shortDescription,
        homepage: newGroup.homepage,
        avatarPath: newGroup.avatarPath,
        descriptionPath: newGroup.descriptionPath,
        slug: newGroup.slug,
      }));
    });

    it('creates a list owned by the group', () => {
      expect(result[1]).toStrictEqual(expect.objectContaining({
        type: 'ListCreated',
        ownerId: LOID.fromGroupId(newGroup.id),
        description: expect.stringContaining(newGroup.name),
      }));
    });

    it('identifies the list as the target for ingestion', () => {
      expect(result[2]).toStrictEqual(expect.objectContaining({
        type: 'EvaluatedArticlesListSpecified',
        groupId: newGroup.id,
        listId: (result[1] as ListCreatedEvent).listId,
      }));
    });
  });

  describe('when passed a value taken by another group', () => {
    const otherGroup = arbitraryGroup();

    describe('name', () => {
      describe('and the other group\'s details have never been updated', () => {
        const name = arbitraryString();
        const result = pipe(
          [
            constructEvent('GroupJoined')({
              groupId: otherGroup.id,
              name,
              avatarPath: otherGroup.avatarPath,
              descriptionPath: otherGroup.descriptionPath,
              shortDescription: otherGroup.shortDescription,
              homepage: otherGroup.homepage,
              slug: otherGroup.slug,
            }),
          ],
          create({ ...addGroupCommand, name }),
        );

        it('fails with no events raised', () => {
          expect(E.isLeft(result)).toBe(true);
        });
      });

      describe('and the other group\'s name has previously been updated', () => {
        const name = arbitraryString();
        const result = pipe(
          [
            constructEvent('GroupJoined')({
              groupId: otherGroup.id,
              name: otherGroup.name,
              avatarPath: otherGroup.avatarPath,
              descriptionPath: otherGroup.descriptionPath,
              shortDescription: otherGroup.shortDescription,
              homepage: otherGroup.homepage,
              slug: otherGroup.slug,
            }),
            constructEvent('GroupDetailsUpdated')({
              groupId: otherGroup.id,
              name,
              avatarPath: undefined,
              descriptionPath: undefined,
              shortDescription: undefined,
              homepage: undefined,
              slug: undefined,
            }),
          ],
          create({ ...addGroupCommand, name }),
        );

        it('fails with no events raised', () => {
          expect(E.isLeft(result)).toBe(true);
        });
      });
    });

    describe('slug', () => {
      describe('and the other group\'s details have never been updated', () => {
        const slug = arbitraryWord();
        const result = pipe(
          [
            constructEvent('GroupJoined')({
              groupId: otherGroup.id,
              name: otherGroup.name,
              avatarPath: otherGroup.avatarPath,
              descriptionPath: otherGroup.descriptionPath,
              shortDescription: otherGroup.shortDescription,
              homepage: otherGroup.homepage,
              slug,
            }),
          ],
          create({ ...addGroupCommand, slug }),
        );

        it('fails with no events raised', () => {
          expect(E.isLeft(result)).toBe(true);
        });
      });

      describe('and the other group\'s slug has previously been updated', () => {
        it.todo('fails with no events raised');
      });
    });
  });

  describe('when the group already exists', () => {
    const groupId = arbitraryGroupId();
    const result = pipe(
      [
        constructEvent('GroupJoined')({
          groupId,
          name: newGroup.name,
          avatarPath: newGroup.avatarPath,
          descriptionPath: newGroup.descriptionPath,
          shortDescription: newGroup.shortDescription,
          homepage: newGroup.homepage,
          slug: newGroup.slug,
        }),
      ],
      create({ ...arbitraryGroup(), groupId }),
    );

    it('fails with no events raised', () => {
      expect(E.isLeft(result)).toBe(true);
    });
  });
});
