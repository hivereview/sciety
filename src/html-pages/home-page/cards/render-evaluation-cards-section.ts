import { HtmlFragment, toHtmlFragment } from '../../../types/html-fragment';

type EvaluationCardViewModel = {
  articleLink: string,
  quote: HtmlFragment,
  caption: string,
  articleTitle: HtmlFragment,
};

const evaluationCard1 = {
  articleLink: '/articles/activity/10.1101/2022.06.22.497259',
  quote: toHtmlFragment('The preprint by Yang et al. asks how the shape of the membrane influences the localization of mechanosensitive Piezo channels. The authors use a creative approach involving methods that distort the plasma membrane by generating blebs and artificial filopodia. They convincingly show that curvature of the lipid environment influences Piezo1 localization, such that increased curvature causes channel depletion, and that application of the chemical modulator Yoda1 is sufficient to allow channels to enter filopodia. The study provides support for a provocative “flattening model” of Yoda1 action, and should inspire future studies by researchers interested in mechanosensitive channels and membrane curvature.'),
  caption: 'Curated by Biophysics Colab',
  articleTitle: toHtmlFragment('Membrane curvature governs the distribution of Piezo1 in live cells'),
};

const evaluationCard2 = {
  articleLink: '/articles/activity/10.21203/rs.3.rs-2407778/v1',
  quote: toHtmlFragment('This valuable study revisits the effects of substitution model selection on phylogenetics by comparing reversible and non-reversible DNA substitution models. The authors provide evidence that 1) non time-reversible models sometimes perform better than general time-reversible models when inferring phylogenetic trees out of simulated viral genome sequence data sets, and that 2) non time-reversible models can fit the real data better than the reversible substitution models commonly used in phylogenetics, a finding consistent with previous work. However, the methods are incomplete in supporting the main conclusion of the manuscript, that is that non time-reversible models should be incorporated in the model selection process for these data sets.'),
  caption: 'Curated by eLife',
  articleTitle: toHtmlFragment('Viral genome sequence datasets display pervasive evidence of strand-specific substitution biases that are best described using non-reversible nucleotide substitution models'),
};

const evaluationCard3 = {
  articleLink: '/articles/activity/10.1101/2023.01.02.522517',
  quote: toHtmlFragment('This study presents important findings on the decision-making capacities of honey bees in controlled conditions. The evidence supporting the study is solid, however, the explanation of the methods, importance, and novelty of the study requires further clarification. With a deeper development of the relevance of this study, the reader will have a clear idea of how this study contributes to the field.'),
  caption: 'Curated by eLife',
  articleTitle: toHtmlFragment('How honey bees make fast and accurate decisions'),
};

const renderEvaluationCard = (viewModel: EvaluationCardViewModel) => `
  <article class="curation-teaser">
    <figure>
      <blockquote class="curation-teaser__quote" cite="${viewModel.articleLink}">
        ${viewModel.quote}
      </blockquote>
      <figcaption>
        <p>${viewModel.caption}</p>
        <cite><a href="${viewModel.articleLink}">${viewModel.articleTitle}</a></cite>
      </figcaption>
    </figure>
  </article>
`;

const evaluationCards = () => toHtmlFragment(`
  <h2 class="home-page-cards__title">Most recent evaluations</h2>
  <ul class="home-page-cards__cards">
    <li>
      ${renderEvaluationCard(evaluationCard1)}
    </li>
    <li>
      ${renderEvaluationCard(evaluationCard2)}
    </li>
    <li>
      ${renderEvaluationCard(evaluationCard3)}
    </li>
  </ul>
`);

export const renderEvaluationCardsSection = (): HtmlFragment => toHtmlFragment(`
  <section class="home-page-cards">
    ${evaluationCards()}
  </section>
`);
