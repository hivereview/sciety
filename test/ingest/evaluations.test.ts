import * as Es from '../../src/ingest/evaluations';

describe('evaluations', () => {
  describe('uniq', () => {
    it('removes duplicate entries', () => {
      const evaluation = {
        date: new Date('2021-07-08'),
        articleDoi: '10.1101/111111',
        evaluationLocator: 'ncrc:1234',
        authors: [],
      };

      expect(Es.uniq([evaluation, evaluation])).toStrictEqual(Es.uniq([evaluation]));
    });
  });
});
