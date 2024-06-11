import { List } from '../../../../read-models/lists';
import { GroupId } from '../../../../types/group-id';
import { ListId } from '../../../../types/list-id';

export type CurrentlyFeaturedLists = ReadonlyArray<{
  listName: string,
  listId: ListId,
  forGroup: GroupId,
  successRedirectPath: string,
}>;

export type ViewModel = {
  pageHeading: string,
  groupId: GroupId,
  successRedirectPath: string,
  groupHomePageHref: string,
  currentlyFeaturedLists: CurrentlyFeaturedLists,
  listsThatCanBeFeatured: ReadonlyArray<List>,
};
