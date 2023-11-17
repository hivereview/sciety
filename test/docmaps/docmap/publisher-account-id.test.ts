import { publisherAccountId } from '../../../src/docmaps/docmap/publisher-account-id.js';
import { arbitraryGroup } from '../../types/group.helper.js';

describe('publisher-account-id', () => {
  const group = arbitraryGroup();
  const id = publisherAccountId(group);

  it('points to an existing page on Sciety', () => {
    expect(id).toMatch(/^https:\/\/sciety.org\//);
  });

  it('contains a readable identifier for the group', () => {
    expect(id).toContain(group.slug);
  });
});
