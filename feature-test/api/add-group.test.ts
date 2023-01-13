import {
  $, click, closeBrowser, goto, openBrowser, text, within,
} from 'taiko';
import { AddGroupCommand } from '../../src/commands';
import { arbitraryGroup } from '../../test/types/group.helper';
import { callApi } from '../call-api.helper';

describe('add-group', () => {
  const newGroup = arbitraryGroup();
  const addGroupCommand: AddGroupCommand = {
    groupId: newGroup.id,
    name: newGroup.name,
    shortDescription: newGroup.shortDescription,
    homepage: newGroup.homepage,
    avatarPath: newGroup.avatarPath,
    descriptionPath: newGroup.descriptionPath,
    slug: newGroup.slug,
  };

  beforeAll(async () => {
    await openBrowser();
    await callApi('api/add-group', addGroupCommand);
  });

  afterAll(async () => {
    await closeBrowser();
  });

  it('the group appears in the list on the groups page', async () => {
    await goto('localhost:8080/groups');
    const groupCardExists = await text(newGroup.name, within($('.group-list'))).exists();

    expect(groupCardExists).toBe(true);
  });

  it('the group now has its own page', async () => {
    await goto('localhost:8080/groups');
    const link = $(`[href="/groups/${newGroup.slug}"].group-card__link`);
    await click(link);
    const title = await $('h1').text();

    expect(title).toContain(newGroup.name);
  });

  it('the group can be searched for', async () => {
    const encodedGroupName = encodeURIComponent(newGroup.name);
    await goto(`localhost:8080/search?query=${encodedGroupName}&category=groups`);
    const groupWasFound = await text(newGroup.name, within($('.search-results-list'))).exists();

    expect(groupWasFound).toBe(true);
  });
});
