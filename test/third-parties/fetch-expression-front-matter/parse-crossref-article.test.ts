import { DOMParser } from '@xmldom/xmldom';
import * as O from 'fp-ts/Option';
import {
  getAbstract, getAuthors, getTitle,
} from '../../../src/third-parties/fetch-expression-front-matter/parse-crossref-article';

const crossrefResponseWith = (content: string): string => `
  <?xml version="1.0" encoding="UTF-8"?>
  <doi_records>
    <doi_record>
      <crossref>
        <posted_content>
          ${content}
        </posted_content>
      </crossref>
    </doi_record>
  </doi_records>
`;

describe('parse-crossref-article', () => {
  const parser = new DOMParser({
    errorHandler: (_, msg) => {
      throw msg;
    },
  });

  describe('parsing the abstract', () => {
    it('extracts the abstract text from the XML response', async () => {
      const response = crossrefResponseWith(`
        <abstract>
          Some random nonsense.
        </abstract>`);
      const doc = parser.parseFromString(response, 'text/xml');
      const abstract = getAbstract(doc, response);

      expect(abstract).toStrictEqual(O.some(expect.stringContaining('Some random nonsense.')));
    });

    it('removes the <abstract> element', async () => {
      const response = crossrefResponseWith(`
        <abstract class="something">
          Some random nonsense.
        </abstract>`);
      const doc = parser.parseFromString(response, 'text/xml');
      const abstract = getAbstract(doc, response);

      expect(abstract).toStrictEqual(O.some(expect.not.stringContaining('<abstract>')));
      expect(abstract).toStrictEqual(O.some(expect.not.stringContaining('</abstract>')));
    });

    it('removes the first <title> if present', async () => {
      const response = crossrefResponseWith(`
        <abstract>
          <title class="something">Abstract</title>
          Some random nonsense.
        </abstract>`);
      const doc = parser.parseFromString(response, 'text/xml');
      const abstract = getAbstract(doc, response);

      expect(abstract).toStrictEqual(O.some(expect.not.stringContaining('Abstract')));
    });

    it('replaces remaining <title>s with HTML <h3>s', async () => {
      const response = crossrefResponseWith(`
        <abstract>
          <title class="something">expected to be removed</title>
          <p>Lorem ipsum</p>
          <title class="something">should be an h3</title>
          <p>Lorem ipsum</p>
          <title class="something">should also be an h3</title>
          </sec>
        </abstract>`);
      const doc = parser.parseFromString(response, 'text/xml');
      const abstract = getAbstract(doc, response);

      expect(abstract).toStrictEqual(O.some(expect.stringContaining('<h3>should be an h3</h3>')));
      expect(abstract).toStrictEqual(O.some(expect.stringContaining('<h3>should also be an h3</h3>')));
      expect(abstract).toStrictEqual(O.some(expect.not.stringContaining('<title>')));
      expect(abstract).toStrictEqual(O.some(expect.not.stringContaining('</title>')));
    });

    it('renders italic if present', async () => {
      const response = crossrefResponseWith(`
        <abstract>
          <title>Abstract</title>
          <p>
            The spread of antimicrobial resistance continues to be a priority health concern worldwide, necessitating exploration of alternative therapies.
            <italic class="something">Cannabis sativa</italic>
            has long been known to contain antibacterial cannabinoids, but their potential to address antibiotic resistance has only been superficially investigated. Here, we show that cannabinoids exhibit antibacterial activity against MRSA, inhibit its ability to form biofilms and eradicate pre-formed biofilms and stationary phase cells persistent to antibiotics. We show that the mechanism of action of cannabigerol is through targeting the cytoplasmic membrane of Gram-positive bacteria and demonstrate
            <italic class="something">in vivo</italic>
            efficacy of cannabigerol in a murine systemic infection model caused by MRSA. We also show that cannabinoids are effective against Gram-negative organisms whose outer membrane is permeabilized, where cannabigerol acts on the inner membrane. Finally, we demonstrate that cannabinoids work in combination with polymyxin B against multi-drug resistant Gram-negative pathogens, revealing the broad-spectrum therapeutic potential for cannabinoids.
          </p>
        </abstract>`);
      const doc = parser.parseFromString(response, 'text/xml');
      const abstract = getAbstract(doc, response);

      expect(abstract).toStrictEqual(O.some(expect.stringContaining('<i>Cannabis sativa</i>')));
      expect(abstract).toStrictEqual(O.some(expect.stringContaining('<i>in vivo</i>')));
    });

    it('replaces <list> unordered list with HTML <ul>', async () => {
      const response = crossrefResponseWith(`
        <abstract>
          <list class="something" list-type="bullet" id="id-1">
            <list-item class="something">
              <p>Transcriptional regulation of ESRP2.</p>
            </list-item>
            <list-item class="something">
              <p>Both ESRP1 and ESRP2 are highly expressed in prostate cancer tissue.</p>
            </list-item>
          </list>
        </abstract>`);
      const doc = parser.parseFromString(response, 'text/xml');
      const abstract = getAbstract(doc, response);

      expect(abstract).toStrictEqual(O.some(expect.stringContaining('<ul>')));
      expect(abstract).toStrictEqual(O.some(expect.stringContaining('</ul>')));
      expect(abstract).toStrictEqual(O.some(expect.stringContaining('<li>')));
      expect(abstract).toStrictEqual(O.some(expect.stringContaining('</li>')));
    });

    it('replaces <sec> with HTML <section>', () => {
      const response = crossrefResponseWith(`
        <abstract>
          <sec class="something">
            <p>Lorem ipsum</p>
          </sec>
        </abstract>`);
      const doc = parser.parseFromString(response, 'text/xml');
      const abstract = getAbstract(doc, response);

      expect(abstract).toStrictEqual(O.some(expect.stringContaining('<section>')));
      expect(abstract).toStrictEqual(O.some(expect.stringContaining('</section>')));
      expect(abstract).toStrictEqual(O.some(expect.not.stringContaining('<sec>')));
      expect(abstract).toStrictEqual(O.some(expect.not.stringContaining('</sec>')));
    });

    it('strips <title> named Graphical abstract', () => {
      const response = crossrefResponseWith(`
        <abstract>
          <title>First title</title>
          <sec>
            <title>Graphical abstract</title>
            <fig id="ufig1" position="float" fig-type="figure" orientation="portrait">
              <graphic href="222794v2_ufig1" position="float" orientation="portrait" />
            </fig>
          </sec>
        </abstract>`);
      const doc = parser.parseFromString(response, 'text/xml');
      const abstract = getAbstract(doc, response);

      expect(abstract).toStrictEqual(O.some(''));
    });

    it('strips <section> elements that are empty or only contain whitespace', () => {
      const response = crossrefResponseWith(`
        <abstract>
          <sec>
            <fig id="ufig1" position="float" fig-type="figure" orientation="portrait">
              <graphic href="222794v2_ufig1" position="float" orientation="portrait" />
            </fig>
          </sec>
        </abstract>`);

      const doc = parser.parseFromString(response, 'text/xml');
      const abstract = getAbstract(doc, response);

      expect(abstract).toStrictEqual(O.some(''));
    });

    it('doesn\'t strip <section> elements that are not empty', () => {
      const response = crossrefResponseWith(`
        <abstract>
          <sec>
            Lorem ipsum
          </sec>
        </abstract>`);

      const doc = parser.parseFromString(response, 'text/xml');
      const abstract = getAbstract(doc, response);

      expect(abstract).toStrictEqual(O.some(expect.stringContaining('<section>')));
      expect(abstract).toStrictEqual(O.some(expect.stringContaining('Lorem ipsum')));
      expect(abstract).toStrictEqual(O.some(expect.stringContaining('</section>')));
    });
  });

  describe('parsing the authors', () => {
    describe('when there are no contributors', () => {
      it('returns none', async () => {
        const response = crossrefResponseWith('');
        const doc = parser.parseFromString(response, 'text/xml');
        const authors = getAuthors(doc, response);

        expect(authors).toStrictEqual(O.none);
      });
    });

    describe('when there are person contributors with a given name and a surname', () => {
      it('extracts authors from the XML response', async () => {
        const response = crossrefResponseWith(`
        <contributors>
          <person_name contributor_role="author" sequence="first">
            <given_name>Eesha</given_name>
            <surname>Ross</surname>
          </person_name>
          <person_name contributor_role="author" sequence="additional">
            <given_name>Fergus</given_name>
            <surname>Fountain</surname>
          </person_name>
        </contributors>`);
        const doc = parser.parseFromString(response, 'text/xml');
        const authors = getAuthors(doc, response);

        expect(authors).toStrictEqual(O.some(['Eesha Ross', 'Fergus Fountain']));
      });

      describe('when any part of the author name contains XML', () => {
        it('strips XML from the author names', async () => {
          const response = crossrefResponseWith(`
        <contributors>
          <person_name contributor_role="author" sequence="first">
            <given_name><scp>Fergus</scp></given_name>
            <surname>Fo<scp>untain</scp></surname>
          </person_name>
        </contributors>`);
          const doc = parser.parseFromString(response, 'text/xml');
          const authors = getAuthors(doc, response);

          expect(authors).toStrictEqual(O.some(['Fergus Fountain']));
        });
      });
    });

    describe('when there is a person author without a given name', () => {
      it('uses the surname', async () => {
        const response = crossrefResponseWith(`
        <contributors>
          <person_name contributor_role="author" sequence="first">
            <surname>Ross</surname>
          </person_name>
        </contributors>`);
        const doc = parser.parseFromString(response, 'text/xml');
        const authors = getAuthors(doc, response);

        expect(authors).toStrictEqual(O.some(['Ross']));
      });
    });

    describe('when there are both author and non-author contributors', () => {
      it('only includes authors', async () => {
        const response = crossrefResponseWith(`
        <contributors>
          <person_name contributor_role="author" sequence="first">
            <given_name>Eesha</given_name>
            <surname>Ross</surname>
          </person_name>
          <person_name contributor_role="reviewer" sequence="additional">
            <given_name>Fergus</given_name>
            <surname>Fountain</surname>
          </person_name>
        </contributors>`);
        const doc = parser.parseFromString(response, 'text/xml');
        const authors = getAuthors(doc, response);

        expect(authors).toStrictEqual(O.some(['Eesha Ross']));
      });
    });

    describe('when there is an organisational author', () => {
      it('uses the organisation\'s name', () => {
        const response = crossrefResponseWith(`
        <contributors>
          <organization contributor_role="author" sequence="first">SEQC2 Oncopanel Sequencing Working Group</organization>
          <person_name contributor_role="author" sequence="additional">
            <given_name>Yifan</given_name>
            <surname>Zhang</surname>
            <ORCID>http://orcid.org/0000-0002-3677-6973</ORCID>
          </person_name>
        </contributors>
      `);
        const doc = parser.parseFromString(response, 'text/xml');
        const authors = getAuthors(doc, response);

        expect(authors).toStrictEqual(O.some(['SEQC2 Oncopanel Sequencing Working Group', 'Yifan Zhang']));
      });
    });

    describe('when the mandatory surname is missing', () => {
      it('return O.none from getAuthors', () => {
        const response = crossrefResponseWith(`
        <contributors>
          <person_name contributor_role="author" sequence="additional">
            <given_name>Yifan</given_name>
          </person_name>
        </contributors>
      `);
        const doc = parser.parseFromString(response, 'text/xml');
        const authors = getAuthors(doc, response);

        expect(O.isSome(authors)).toBeFalsy();
      });
    });
  });

  // TODO: these assertions appear to expect a string, but should expect a SanitisedHtmlFragment
  // There is coupling between these expectations and the way the SanitisedHtmlFragment type
  // has been defined.
  describe('parsing the title', () => {
    it('extracts a title from the XML response', async () => {
      const response = crossrefResponseWith(`
        <titles>
          <title>An article title</title>
        </titles>`);
      const doc = parser.parseFromString(response, 'text/xml');
      const title = getTitle(doc);

      expect(title).toStrictEqual(O.some('An article title'));
    });

    it('trims leading and trailing whitespace', () => {
      const response = crossrefResponseWith(`
        <titles>
          <title>
            An article title
          </title>
        </titles>`);
      const doc = parser.parseFromString(response, 'text/xml');
      const title = getTitle(doc);

      expect(title).toStrictEqual(O.some('An article title'));
    });

    it('returns `Unknown title` when no title present', async () => {
      const response = crossrefResponseWith('');
      const doc = parser.parseFromString(response, 'text/xml');
      const title = getTitle(doc);

      expect(title).toStrictEqual(O.none);
    });

    it('extracts a title containing inline HTML tags from the XML response', async () => {
      const response = crossrefResponseWith(`
        <titles>
          <title>An article title for <i>C. elegans</i></title>
        </titles>`);
      const doc = parser.parseFromString(response, 'text/xml');
      const title = getTitle(doc);

      expect(title).toStrictEqual(O.some('An article title for <i>C. elegans</i>'));
    });

    it('strips non html tags from the title', async () => {
      const response = crossrefResponseWith(`
        <titles>
          <title>An article title for <scp>C. elegans</scp></title>
        </titles>`);
      const doc = parser.parseFromString(response, 'text/xml');
      const title = getTitle(doc);

      expect(title).toStrictEqual(O.some('An article title for C. elegans'));
    });
  });
});
