import { GetAllGroups, getAllGroups } from './get-all-groups';
import { GetGroup, getGroup } from './get-group';
import { GetGroupBySlug, getGroupBySlug } from './get-group-by-slug';
import { ReadModel } from './handle-event';

export type Queries = {
  getAllGroups: GetAllGroups,
  getGroup: GetGroup,
  getGroupBySlug: GetGroupBySlug,
};

export const queries = (instance: ReadModel): Queries => ({
  getAllGroups: getAllGroups(instance),
  getGroup: getGroup(instance),
  getGroupBySlug: getGroupBySlug(instance),
});
