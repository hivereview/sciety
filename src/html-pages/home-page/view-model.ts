import * as O from 'fp-ts/Option';
import { HtmlFragment } from '../../types/html-fragment';
import { GroupId } from '../../types/group-id';
import { GroupLinkWithLogoViewModel } from '../../shared-components/group-link';

type CurationTeaser = {
  articleLink: string,
  groupId: GroupId,
  quote: HtmlFragment,
  caption: string,
  articleTitle: HtmlFragment,
};

export type ViewModel = {
  groups: O.Option<ReadonlyArray<GroupLinkWithLogoViewModel>>,
  curationTeasers: ReadonlyArray<CurationTeaser>,
};
