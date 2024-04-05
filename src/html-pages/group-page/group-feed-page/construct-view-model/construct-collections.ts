import * as O from 'fp-ts/Option';
import * as RA from 'fp-ts/ReadonlyArray';
import { pipe } from 'fp-ts/function';
import { Dependencies } from './dependencies';
import * as LID from '../../../../types/list-id';
import { constructListCardViewModelWithCurator } from '../../../../shared-components/list-card';
import { GroupId } from '../../../../types/group-id';
import * as GID from '../../../../types/group-id';
import { ViewModel } from '../view-model';

const constructListCardForACollection = (dependencies: Dependencies) => (listId: LID.ListId) => pipe(
  listId,
  dependencies.lookupList,
  O.map(constructListCardViewModelWithCurator(dependencies)),
  O.map((viewModel) => ({
    ...viewModel,
    imageUrl: dependencies.lookupHardcodedListImage(listId),
  })),
);

const hardcoded = new Map<GroupId, LID.ListId>([
  [GID.fromValidatedString('4bbf0c12-629b-4bb8-91d6-974f4df8efb2'), LID.fromValidatedString('454ba80f-e0bc-47ed-ba76-c8f872c303d2')],
  [GID.fromValidatedString('f7a7aec3-8b1c-4b81-b098-f3f2e4eefe58'), LID.fromValidatedString('729cab51-b47d-4ab5-bf2f-8282f1de445e')],
]);

export const constructCollections = (
  dependencies: Dependencies,
  groupId: GroupId,
): ViewModel['collections'] => {
  const hardcodedListId = hardcoded.get(groupId);
  if (hardcodedListId) {
    return pipe(
      hardcodedListId,
      constructListCardForACollection(dependencies),
      RA.fromOption,
    );
  }
  return [];
};
