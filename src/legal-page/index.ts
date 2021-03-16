import { toHtmlFragment } from '../types/html-fragment';
import { Page } from '../types/page';

export const legalPage: Page = {
  title: 'Legal',
  content: toHtmlFragment(`
    <div class="sciety-grid sciety-grid--simple">
      <header class="page-header">
        <h1>Legal</h1>
      </header>

      <h2>Terms and conditions</h2>
      <p>
        This website is operated by eLife Sciences Publications, Ltd under their terms and conditions as
        set out at <a href="https://elifesciences.org/terms">elifesciences.org/terms</a> unless otherwise stated through amendments below.
      </p>
      <h3>
        Exceptions to Ownership
      </h3>
      <p>
        Unless otherwise indicated the Editorial Community names, logos and trademarks are owned by
        their respective owners and not by eLife or its licensors.
        The article content is created by the authors stated on the article page and is not owned by
        eLife or its licensors, and is subject to the licence terms shown against it.
        Evaluation content is created by the Editorial Community named alongside the evaluation content
        and is not owned by eLife or its licensors, and is subject to the licence terms shown on the Editorial Community's page.
      </p>

      <h2>Privacy notice</h2>

      <p>
        This Privacy Notice relates to data held and processed by eLife Sciences Publications, Ltd
        who operate this site. For all queries relating to personal data and privacy, please contact
        us at <a href="mailto:data@elifesciences.org">data@elifesciences.org</a>.
      </p>

      <p>
        Full details of the Privacy Notice can be found at
        <a href="https://elifesciences.org/privacy">elifesciences.org/privacy</a>.
      </p>
      <h3>
        What additional personal information does this site hold?
      </h3>
      <p>
        This site adds the following to the information specified in the privacy notice linked above.
      </p>

      <p>
        When you log in to your Twitter account through our site (for example to store your feed preferences) that interaction is directly with Twitter Inc. We only receive from Twitter a user name and user identifier. Your Twitter user identifier is associated with items that you follow on the site (communities, people, papers and evaluation events) and used to publicly display events that you follow on your user page. In addition, your user name is displayed on the community page of any community that you follow.
      </p>

      <h3>
        Privacy notice changes
      </h3>
      <h4>
        Change log
      </h4>
      <p>
        Although most changes are likely to be minor, eLife may change its Privacy Notice from time to time, and at our sole discretion. We encourage visitors to check this page frequently for any changes to its Privacy Notice. First published October 19, 2020.
      </p>
    </div>
  `),
};
