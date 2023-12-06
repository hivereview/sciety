import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import { pipe } from 'fp-ts/function';
import { v4 } from 'uuid';
import { PaperId } from '../../src/third-parties';
import { arbitraryArticleId } from '../types/article-id.helper';
import { shouldNotBeCalled } from '../should-not-be-called';
import { arbitraryString } from '../helpers';

describe('paper-id', () => {
  describe('when successfully decoding a UUID', () => {
    const input = v4();
    const decoded = pipe(
      input,
      PaperId.paperIdCodec.decode,
      E.getOrElseW(shouldNotBeCalled),
    );

    it('isDoi detects that the paper id is not a doi', () => {
      expect(PaperId.isDoi(decoded)).toBe(false);
    });

    it('isUuid detects that the paper id is a uuid', () => {
      expect(PaperId.isUuid(decoded)).toBe(true);
    });

    describe('encoding', () => {
      it.todo('returns the original value');
    });
  });

  describe('when successfully decoding a DOI', () => {
    const input = arbitraryArticleId().value;
    const decoded = pipe(
      input,
      PaperId.paperIdCodec.decode,
      E.getOrElseW(shouldNotBeCalled),
    );

    it('isDoi detects that the paper id is a doi', () => {
      expect(PaperId.isDoi(decoded)).toBe(true);
    });

    it('isUuid detects that the paper id is not a uuid', () => {
      expect(PaperId.isUuid(decoded)).toBe(false);
    });

    describe('encoding', () => {
      it.todo('returns the original value');
    });

    describe('getDoiPortion', () => {
      const result = pipe(
        decoded,
        O.fromPredicate(PaperId.isDoi),
        O.getOrElseW(shouldNotBeCalled),
        PaperId.getDoiPortion,
      );

      it('results in the original value', () => {
        expect(result).toBe(input);
      });
    });
  });

  describe('when decoding a value that is neither a uuid nor a doi', () => {
    const decoded = PaperId.paperIdCodec.decode(arbitraryString());

    it('returns on the left', () => {
      expect(E.isLeft(decoded)).toBe(true);
    });
  });
});
