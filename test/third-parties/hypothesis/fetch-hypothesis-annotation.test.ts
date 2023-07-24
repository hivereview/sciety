import { URL } from 'url';
import * as E from 'fp-ts/Either';
import { pipe } from 'fp-ts/function';
import * as TE from 'fp-ts/TaskEither';
import { fetchHypothesisAnnotation, insertSelectedText } from '../../../src/third-parties/hypothesis/fetch-hypothesis-annotation';
import { toHtmlFragment } from '../../../src/types/html-fragment';
import { dummyLogger } from '../../dummy-logger';
import { arbitraryWord } from '../../helpers';
import { HypothesisAnnotation } from '../../../src/third-parties/hypothesis/HypothesisAnnotation';

const date = '2019-09-12T09:55:46.146050+00:00';
const key = arbitraryWord();

describe('fetch-hypothesis-annotation', () => {
  it('returns the evaluation', async () => {
    const queryExternalService = () => () => TE.right({
      created: date,
      text: '<p>Very good</p>',
      target: [],
      links: {
        incontext: 'https://www.example.com',
      },
    });
    const evaluation = await fetchHypothesisAnnotation(queryExternalService, dummyLogger)(key)();

    const expected = {
      fullText: pipe('<p>Very good</p>', toHtmlFragment),
      url: new URL('https://www.example.com'),
    };

    expect(evaluation).toStrictEqual(E.right(expected));
  });

  it.each([
    ['basic Markdown', '# Very good', '<h1>Very good</h1>'],
    ['(linkify) GitHub Flavored Markdown', 'www.example.com', '<a href="http://www.example.com">www.example.com</a>'],
    ['bold italics', '***bold/italics** italics*', '<p><em><strong>bold/italics</strong> italics</em></p>'],
  ])('converts %s to HTML', async (_, input: string, expected: string) => {
    const queryExternalService = () => () => TE.right({
      created: date,
      text: input,
      tags: [],
      target: [],
      links: {
        incontext: 'https://www.example.com',
      },
    });
    const evaluation = await fetchHypothesisAnnotation(queryExternalService, dummyLogger)(key)();

    expect(evaluation).toStrictEqual(E.right(expect.objectContaining({
      fullText: expect.stringContaining(expected),
    })));
  });

  it.todo('test the 404 response when hypothesis group has removed an annotation');

  it.todo('test the 500 response when hypothesis is unreachable');
});

describe('insertSelectedText', () => {
  describe('when there is a text quote selector', () => {
    it('returns the text quote in markdown format in addition to the response text', () => {
      const input: HypothesisAnnotation = {
        text: 'some text',
        links: {
          incontext: '',
        },
        target: [{
          selector: [{
            type: 'TextQuoteSelector',
            exact: 'lorem ipsum',
          }],
        }],
      };
      const result = insertSelectedText(input);

      const expectedResult = `> lorem ipsum

some text`;

      expect(result).toBe(expectedResult);
    });
  });

  describe('when there is a selector with multiple text quotes', () => {
    it.todo('unspecified behaviour');
  });

  describe('when there is no selector', () => {
    it('returns the response text', () => {
      const input: HypothesisAnnotation = {
        text: 'some text',
        links: {
          incontext: '',
        },
        target: [{
          selector: [{
            type: 'TextPositionSelector',
          }],
        }],
      };
      const result = insertSelectedText(input);

      expect(result).toBe('some text');
    });
  });
});
