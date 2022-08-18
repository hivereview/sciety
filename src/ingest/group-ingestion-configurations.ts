import { fetchNcrcEvaluations } from './fetch-ncrc-evaluations';
import { fetchPciEvaluations } from './fetch-pci-evaluations';
import { fetchPrereviewEvaluations } from './fetch-prereview-evaluations';
import { fetchRapidReviews } from './fetch-rapid-reviews';
import { fetchReviewsFromCrossrefViaBiorxiv } from './fetch-reviews-from-crossref-via-biorxiv';
import { fetchReviewsFromHypothesisGroup } from './fetch-reviews-from-hypothesis-group';
import { fetchReviewsFromHypothesisUser } from './fetch-reviews-from-hypothesis-user';
import { GroupIngestionConfiguration } from './update-all';
import { fetchPrelightsEvaluations } from '../third-parties/prelights/fetch-prelights-evaluations';

// addArticleToEvaluatedArticlesList policy needs to be updated BEFORE adding a new group to this configuration
export const groupIngestionConfigurations: Array<GroupIngestionConfiguration> = [
  {
    id: 'bc1f956b-12e8-4f5c-aadc-70f91347bd18',
    name: 'Arcadia Science',
    fetchFeed: fetchReviewsFromHypothesisGroup('VzenYeD8'),
  },
  {
    id: 'b560187e-f2fb-4ff9-a861-a204f3fc0fb0',
    name: 'eLife',
    fetchFeed: fetchReviewsFromHypothesisGroup('q5X6RWJ6'),
  },
  {
    id: '62f9b0d0-8d43-4766-a52a-ce02af61bc6a',
    name: 'NCRC',
    fetchFeed: fetchNcrcEvaluations(),
  },
  {
    id: '74fd66e9-3b90-4b5a-a4ab-5be83db4c5de',
    name: 'PCI Zoology',
    fetchFeed: fetchPciEvaluations('https://zool.peercommunityin.org/rss/rss4elife'),
  },
  {
    id: '19b7464a-edbe-42e8-b7cc-04d1eb1f7332',
    name: 'PCI Evolutionary Biology',
    fetchFeed: fetchPciEvaluations('https://evolbiol.peercommunityin.org/rss/rss4elife'),
  },
  {
    id: '32025f28-0506-480e-84a0-b47ef1e92ec5',
    name: 'PCI Ecology',
    fetchFeed: fetchPciEvaluations('https://ecology.peercommunityin.org/rss/rss4elife'),
  },
  {
    id: '4eebcec9-a4bb-44e1-bde3-2ae11e65daaa',
    name: 'PCI Animal Science',
    fetchFeed: fetchPciEvaluations('https://animsci.peercommunityin.org/rss/rss4elife'),
  },
  {
    id: 'b90854bf-795c-42ba-8664-8257b9c68b0c',
    name: 'PCI Archaeology',
    fetchFeed: fetchPciEvaluations('https://archaeo.peercommunityin.org/rss/rss4elife'),
  },
  {
    id: '7a9e97d1-c1fe-4ac2-9572-4ecfe28f9f84',
    name: 'PCI Paleontology',
    fetchFeed: fetchPciEvaluations('https://paleo.peercommunityin.org/rss/rss4elife'),
  },
  {
    id: 'af792cd3-1600-465c-89e5-250c48f793aa',
    name: 'PCI Neuroscience',
    fetchFeed: fetchPciEvaluations('https://neuro.peercommunityin.org/rss/rss4elife'),
  },
  {
    id: '53ed5364-a016-11ea-bb37-0242ac130002',
    name: 'PeerJ',
    fetchFeed: fetchReviewsFromCrossrefViaBiorxiv('10.7717', '10.7287'),
  },
  {
    id: 'f97bd177-5cb6-4296-8573-078318755bf2',
    name: 'preLights',
    fetchFeed: fetchPrelightsEvaluations(),
  },
  {
    id: '10360d97-bf52-4aef-b2fa-2f60d319edd7',
    name: 'PREreview',
    fetchFeed: fetchPrereviewEvaluations(),
  },
  {
    id: '5142a5bc-6b18-42b1-9a8d-7342d7d17e94',
    name: 'Rapid Reviews COVID-19',
    fetchFeed: fetchRapidReviews(),
  },
  {
    id: '316db7d9-88cc-4c26-b386-f067e0f56334',
    name: 'Review Commons',
    fetchFeed: fetchReviewsFromHypothesisGroup('NEGQVabn'),
  },
  {
    id: '8ccea9c2-e6c8-4dd7-bf1d-37c3fa86ff65',
    name: 'ScreenIT',
    fetchFeed: fetchReviewsFromHypothesisUser('sciscore'),
  },
  {
    id: '4bbf0c12-629b-4bb8-91d6-974f4df8efb2',
    name: 'Biophysics Colab',
    fetchFeed: fetchReviewsFromHypothesisUser('biophysics_sciencecolab'),
  },
  {
    id: '50401e46-b764-47b7-8557-6bb35444b7c8',
    name: 'ASAPbio crowd review',
    fetchFeed: fetchReviewsFromHypothesisUser('ASAPbio_crowd_preprint_review'),
  },
  {
    id: 'b5f31635-d32b-4df9-92a5-0325a1524343',
    name: 'PeerRef',
    fetchFeed: fetchReviewsFromHypothesisGroup('LN28Q33j', 150),
  },
  {
    id: 'd6e1a913-76f8-40dc-9074-8eac033e1bc8',
    name: 'GigaScience',
    fetchFeed: fetchReviewsFromHypothesisGroup('x6WjqQay', 365),
  },
  {
    id: 'f7a7aec3-8b1c-4b81-b098-f3f2e4eefe58',
    name: 'GigaByte',
    fetchFeed: fetchReviewsFromHypothesisGroup('z4oRbDWY', 365),
  },
];
