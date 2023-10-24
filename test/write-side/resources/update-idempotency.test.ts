import * as UI from '../../../src/write-side/resources/update-idempotency';

describe('update-idempotency', () => {
  describe('isEmpty', () => {
    it.each([
      [{}, true],
      [{ foo: 'bar' }, false],
    ])('given %s returns %s', (input, expected) => {
      expect(UI.isEmpty(input)).toBe(expected);
    });
  });

  describe('changedFields', () => {
    it.each([
      // trivial
      [{}, {}, {}],
      [{ a: 1 }, { a: 1 }, { a: undefined }],
      // involving objects
      [{ a: ['1'] }, { a: ['1'] }, { a: undefined }],
      [{ a: ['2'] }, { a: ['1'] }, { a: ['2'] }],
      // combinations of changed and unchanged
      [{ a: 1, b: 2 }, { a: 1, b: 1 }, { a: undefined, b: 2 }],
      // combinations of changed and undefined
      [{ a: undefined, b: 2 }, { a: 1, b: 1 }, { a: undefined, b: 2 }],
      [{ a: undefined, b: 2 }, { a: undefined, b: 1 }, { a: undefined, b: 2 }],
      [{ a: 1, b: 2 }, { a: undefined, b: 1 }, { a: 1, b: 2 }],
    ])('input: %s and state: %s returns %s', (input, state, expected) => {
      expect(UI.changedFields(input)(state)).toStrictEqual(expected);
    });
  });
});
