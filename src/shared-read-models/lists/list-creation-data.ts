import { listCreated, ListCreatedEvent } from '../../domain-events/list-created-event';
import * as GID from '../../types/group-id';

export const listCreationEvents: ReadonlyArray<ListCreatedEvent> = [
  listCreated(
    'ee7e738a-a1f1-465b-807c-132d273ca952',
    'Evaluated articles',
    'Articles that have been evaluated by Biophysics Colab.',
    GID.fromValidatedString('4bbf0c12-629b-4bb8-91d6-974f4df8efb2'),
    new Date('2021-06-29T08:13:17Z'),
  ),
  listCreated(
    'dc83aa3b-1691-4356-b697-4257d31a27dc',
    'Evaluated articles',
    'Articles that have been evaluated by ASAPbio crowd review.',
    GID.fromValidatedString('50401e46-b764-47b7-8557-6bb35444b7c8'),
    new Date('2021-06-29T08:13:17Z'),
  ),
  listCreated(
    '4654fd6e-cb00-458f-967b-348b41804927',
    'Evaluated articles',
    'Articles that have been evaluated by NCRC.',
    GID.fromValidatedString('62f9b0d0-8d43-4766-a52a-ce02af61bc6a'),
    new Date('2021-02-18T10:28:54Z'),
  ),
  listCreated(
    '49e589f1-531d-4447-92b6-e60b6d1c705e',
    'Evaluated articles',
    'Articles that have been evaluated by Rapid Reviews COVID-19.',
    GID.fromValidatedString('5142a5bc-6b18-42b1-9a8d-7342d7d17e94'),
    new Date('2021-05-13T14:33:28Z'),
  ),
  listCreated(
    'f1561c0f-d247-4e03-934d-52ad9e0aed2f',
    'Evaluated articles',
    'Articles that have been evaluated by eLife.',
    GID.fromValidatedString('b560187e-f2fb-4ff9-a861-a204f3fc0fb0'),
    new Date('2020-08-12T13:59:32Z'),
  ),
  listCreated(
    'e9606e0e-8fdb-4336-a24a-cc6547d7d950',
    'Evaluated articles',
    'Articles that have been evaluated by ScreenIT.',
    GID.fromValidatedString('8ccea9c2-e6c8-4dd7-bf1d-37c3fa86ff65'),
    new Date('2021-05-13T10:39:43Z'),
  ),
  listCreated(
    'f4b96b8b-db49-4b41-9c5b-28d66a83cd70',
    'Evaluated articles',
    'Articles that have been evaluated by preLights.',
    GID.fromValidatedString('f97bd177-5cb6-4296-8573-078318755bf2'),
    new Date('2021-01-05T11:43:08Z'),
  ),
  listCreated(
    '5c2e4b99-f5f0-4145-8c87-cadd7a41a1b1',
    'Evaluated articles',
    'Articles that have been evaluated by PREreview.',
    GID.fromValidatedString('10360d97-bf52-4aef-b2fa-2f60d319edd7'),
    new Date('2020-08-12T13:59:32Z'),
  ),
  listCreated(
    'f981342c-bf38-4dc8-9569-acda5878c07b',
    'Evaluated articles',
    'Articles that have been evaluated by PeerJ.',
    GID.fromValidatedString('53ed5364-a016-11ea-bb37-0242ac130002'),
    new Date('2020-08-12T13:53:55Z'),
  ),
  listCreated(
    'f3dbc188-e891-4586-b267-c99cf3b3808e',
    'Evaluated articles',
    'Articles that have been evaluated by Review Commons.',
    GID.fromValidatedString('316db7d9-88cc-4c26-b386-f067e0f56334'),
    new Date('2020-08-12T13:59:32Z'),
  ),
  listCreated(
    'a4d57b30-b41c-4c9d-81f0-dccd4cd1d099',
    'Evaluated articles',
    'Articles that have been evaluated by Peer Community In Zoology.',
    GID.fromValidatedString('74fd66e9-3b90-4b5a-a4ab-5be83db4c5de'),
    new Date('2020-08-12T13:59:32Z'),
  ),
  listCreated(
    '3d69f9e5-6fd2-4266-9cf8-c069bca79617',
    'Evaluated articles',
    'Articles that have been evaluated by Peer Community in Evolutionary Biology.',
    GID.fromValidatedString('19b7464a-edbe-42e8-b7cc-04d1eb1f7332'),
    new Date('2020-08-17T13:07:09Z'),
  ),
  listCreated(
    '65f661e6-73f9-43e9-9ae6-a84635afb79a',
    'Evaluated articles',
    'Articles that have been evaluated by Peer Community in Ecology.',
    GID.fromValidatedString('32025f28-0506-480e-84a0-b47ef1e92ec5'),
    new Date('2020-08-17T13:56:41Z'),
  ),
  listCreated(
    'e764d90c-ffea-4b0e-a63e-d2b5236aa1ed',
    'Evaluated articles',
    'Articles that have been evaluated by Peer Community in Animal Science.',
    GID.fromValidatedString('4eebcec9-a4bb-44e1-bde3-2ae11e65daaa'),
    new Date('2020-08-17T13:56:41Z'),
  ),
  listCreated(
    '24a60cf9-5f45-43f2-beaf-04139e6f0a0e',
    'Evaluated articles',
    'Articles that have been evaluated by Peer Community in Archaeology.',
    GID.fromValidatedString('b90854bf-795c-42ba-8664-8257b9c68b0c'),
    new Date('2021-06-29T08:13:17Z'),
  ),
  listCreated(
    'dd9d166f-6d25-432c-a60f-6df33ca86897',
    'Evaluated articles',
    'Articles that have been evaluated by Peer Community in Paleontology.',
    GID.fromValidatedString('7a9e97d1-c1fe-4ac2-9572-4ecfe28f9f84'),
    new Date('2020-08-17T13:56:41Z'),
  ),
  listCreated(
    'cbd478fe-3ff7-4125-ac9f-c94ff52ae0f7',
    'High interest articles',
    'Articles that have been identified as high interest by NCRC editors.',
    // this group id is wrong, but if it were right it would trigger a bug
    // on the selection of the list from its Read Model
    GID.fromValidatedString('62f9b0d0-8d43-4766-a52a-ce02af61bc6a.jpg'),
    new Date('2021-11-24'),
  ),
  listCreated(
    '5ac3a439-e5c6-4b15-b109-92928a740812',
    'Endorsed articles',
    'Articles that have been endorsed by Biophysics Colab.',
    // this group id is wrong, but if it were right it would trigger a bug
    // on the selection of the list from its Read Model
    GID.fromValidatedString('4bbf0c12-629b-4bb8-91d6-974f4df8efb2.png'),
    new Date('2021-11-22T15:09:00Z'),
  ),
  listCreated(
    'f2ce72ba-a982-4156-ab34-4a536bd86cb7',
    'Evaluated articles',
    'Articles that have been evaluated by Peer Community in Neuroscience.',
    GID.fromValidatedString('af792cd3-1600-465c-89e5-250c48f793aa'),
    new Date('2021-12-02T10:28:00Z'),
  ),
];
