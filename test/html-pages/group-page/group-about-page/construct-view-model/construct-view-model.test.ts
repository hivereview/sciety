import { pipe } from 'fp-ts/function';
import * as O from 'fp-ts/Option';
import * as TE from 'fp-ts/TaskEither';
import { createTestFramework, TestFramework } from '../../../../framework';
import { arbitraryGroup } from '../../../../types/group.helper';
import { arbitraryList } from '../../../../types/list-helper';
import * as LOID from '../../../../../src/types/list-owner-id';
import { ViewModel } from '../../../../../src/html-pages/group-page/group-about-page/view-model';
import { constructViewModel, Ports } from '../../../../../src/html-pages/group-page/group-about-page/construct-view-model/construct-view-model';
import { shouldNotBeCalled } from '../../../../should-not-be-called';

describe('construct-view-model', () => {
  let framework: TestFramework;
  const group = arbitraryGroup();

  beforeEach(() => {
    framework = createTestFramework();
  });

  describe('when the group has more than one list', () => {
    const firstList = arbitraryList(LOID.fromGroupId(group.id));
    const secondList = arbitraryList(LOID.fromGroupId(group.id));
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    let viewmodel: ViewModel;

    beforeEach(async () => {
      await framework.commandHelpers.createList(firstList);
      await framework.commandHelpers.createList(secondList);
      const adapters: Ports = {
        ...framework.queries,
        fetchStaticFile: () => TE.right(''),
      };
      viewmodel = await pipe(
        {
          slug: group.slug,
          user: O.none,
        },
        constructViewModel(adapters),
        TE.getOrElse(shouldNotBeCalled),
      )();
    });

    it.todo('returns lists in descending order of updated date');
  });
});
