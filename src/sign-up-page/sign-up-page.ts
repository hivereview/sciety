import { toHtmlFragment } from '../types/html-fragment';
import { Page } from '../types/page';

export const signUpPage: Page = {
  title: 'Sign Up for Sciety',
  content: toHtmlFragment(`
    <header class="page-header">
      <h1>Sign Up for Sciety</h1>
    </header>
    <div class="sign-up-page-content">
      <p><a href="/sign-up-call-to-action" class="sign-up-page-call-to-action">Sign Up With Your Twitter Account</a></p>
  
      <h2>Don't Have a Twitter Account?</h2>
      <p>You will need to create a twitter account before you can sign up for Sciety.</p>
      <p><a href="https://twitter.com/">Create a Twitter Account</a></p>
  
      <h3>Don't Want to Create a Twitter Account?</h3>
      <p>We will be adding other sign-in methods soon. <a href="/signup">Subscribe to our newsletter</a> to be notified when we do.</p>
  
      <h2>Why Should I Make a Sciety Account?</h2>
      <p>You don't need an account to use Sciety, but having an account unlocks new features.</p>
      <ul>
        <li><b>Save Your Favourite Preprints</b>: Never lose the preprints that interest you most.</li>
        <li><b>Follow Trusted Groups</b>: Stay updated on what your favourite groups are evaluating.</li>
        <li><b>Track Recent Activity</b>: Always see new evaluations on saved preprints.</li>
      </ul>
  
      <p><a href="/about">Learn more about Sciety</a></p>
    </div>

    <aside class="sign-up-page-supplementary">
      <h2>What We Do With Your Data</h2>
      <h3>Twitter Data</h3>
      <p>We only use your user name. We never post on your behalf.</p>
      <h3>Sciety Data</h3>
      <p>We use website data to inform our improvements.</p>
      <p>Read more on our <a href="/legal">legal information page</a>.</p>
    </aside>

  `),
};
