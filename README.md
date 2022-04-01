Sciety
======

The repo for [sciety.org].

Developed and maintained by Sciety's remote first [ensemble programming] team.

Development
-----------

<details>

<summary>Requirements</summary>

- [Docker]
- [Docker Compose]
- [GNU Make]
- [Node.js]
- [Git LFS]
- Unix-like operating system

</details>

### Running the app

To build and run the app for development, execute:

```shell
make dev
```

You can now access the app at <http://localhost:8080>.

Certain parts of the application require you to have set up credentials for external services on your machine.

Most content will be missing as the database will be empty, see the Operations section below on how to populate it.

Containers restart automatically on most code changes. If they don't, `ctrl-c` and rerun `make dev`. An example of when this is needed, is changes to `package.json`.

#### Configuring environment variables and credentials

Environment variables control certain behaviour.

For the application to be able to interact with external services, credentials need to be provided via the `.env` file as well as dedicated credential files.

Running `make dev` creates a `.env` file based on `.env.example`. This includes instructions on how to populate and use it.

You'll need to re-run `make dev` after modifying the `.env` file.

If you've been added to the appropriate `GCP` organisation, you can run `make .gcp-ncrc-key.json` to create the necessary credential file (used for the content of the NCRC group). 

### Running the tests

#### Fast tests

These tests live in `test/` and use [Jest]. You can run them by executing:

```shell
make test
```

#### Slow tests

Browser-based tests live in `feature-test/` and use Taiko. We use them for user journeys.

```shell
make taiko
```

To run just one test file, execute:

```shell
make taiko TEST=[file-name].ts
```

Visual regression tests are defined in `backstop.json`. They rely on approved screenshots in `backstop_data/` stored with Git LFS.

```shell
make backstop-test
make backstop-approve
```

### Linting

The following target runs all static code checks:

```shell
make lint
```

You can fix problems, where possible, by executing:

```shell
make lint:fix
```

The above is quite heavy weight and can take a while.
To increase feedback speed you can gain partial coverage:

```shell
make watch:typescript
```

In our team we also rely on eslint feedback from our IDEs.

## Operations

<details>

<summary>Requirements</summary>

- [logcli]
- [kubectl]
- [aws-cli]

</details>

The application is deployed on a Kubernetes cluster via an Helm chart.

A [staging environment] is updated with every new commit on `main` that passes tests.

A [production environment] is [updated][production deployments] manually by pushing a tag.

### Releasing to production

Ensure your current reference is [green in CI][build].

Run `make release`.

### Looking at logs

Logs of all Pods are streamed to [AWS CloudWatch][AWS CloudWatch logs] for persistence and searchability.

A [CloudWatch dashboard] graphs log lines representing errors and shows the state of the alarm.

An [monitoring SNS topic] triggers a [lambda function that notifies the Slack #sciety-general channel][monitoring
 lambda].

A [CloudWatch user journey by IP] query is available to track a single client across multiple requests (adjust timeframe and IP).

### Dump all data

Run `make prod-sql`.

At the prompt, execute this command:

```sql
\copy (SELECT date, type, payload FROM events ORDER BY date) TO STDOUT WITH CSV;
```

License
-------

We released this software under the [MIT license][License]. Copyright © 2020 [eLife Sciences Publications, Ltd][eLife].

[Architecture sketch]: https://miro.com/app/board/o9J_ksK0wlg=/
[aws-cli]: https://aws.amazon.com/cli/
[AWS CloudWatch logs]: https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logs-insights:queryDetail=~(end~0~start~-900~timeType~'RELATIVE~unit~'seconds~editorString~'fields*20*40timestamp*2c*20*40message*0a*7c*20filter*20*60kubernetes.labels.app_kubernetes_io*2finstance*60*3d*22prc--prod*22*0a*7c*20sort*20*40timestamp*20desc*0a*7c*20limit*2020~isLiveTail~false~queryId~'89133ab9-5bb4-4770-b3e9-96052e8300ef~source~(~'*2faws*2fcontainerinsights*2flibero-eks--franklin*2fapplication));tab=logs
[Build]: https://github.com/sciety/sciety/actions?query=workflow%3ACI
[CloudWatch dashboard]: https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#dashboards:name=PRCMetrics
[CloudWatch user journey by IP]: https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:logs-insights$3FqueryDetail$3D$257E$2528end$257E0$257Estart$257E-1800$257EtimeType$257E$2527RELATIVE$257Eunit$257E$2527seconds$257EeditorString$257E$2527fields*20*40timestamp*2c*20app_request*0a*7c*20filter*20*60kubernetes.labels.app_kubernetes_io*2finstance*60*20*3d*3d*20*27ingress-nginx*27*20and*20app_remote_addr*20*3d*3d*20*2778.105.99.80*27*20and*20app_request*20not*20like*20*2fstatic*2f*0a*7c*20filter*20app_ingress_name*20*3d*3d*20*27sciety--prod--frontend*27*0a*7c*20sort*20*40timestamp*20asc*0a*7c*20limit*20200$257EisLiveTail$257Efalse$257EqueryId$257E$2527e3086054-9d14-4384-bca5-a9c12b181c87$257Esource$257E$2528$257E$2527*2faws*2fcontainerinsights*2flibero-eks--franklin*2fapplication$2529$2529
[Docker]: https://www.docker.com/
[Docker Compose]: https://docs.docker.com/compose/
[eLife]: https://elifesciences.org/
[ensemble programming]: https://en.wikipedia.org/w/index.php?title=Ensemble_programming&redirect=no
[Export from CloudWatch]: https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:log-groups/log-group/$252Faws$252Fcontainerinsights$252Flibero-eks--franklin$252Fapplication
[ESLint]: https://eslint.org/
[Git LFS]: https://git-lfs.github.com/
[GNU Make]: https://www.gnu.org/software/make/
[Jest]: https://jestjs.io/
[kubectl]: https://kubernetes.io/docs/tasks/tools/
[License]: LICENSE.md
[logcli]: https://github.com/grafana/loki/releases
[Makefile]: Makefile
[Monitoring SNS topic]: https://console.aws.amazon.com/sns/v3/home?region=us-east-1#/topic/arn:aws:sns:us-east-1:540790251273:prc-logging
[Monitoring lambda]: https://console.aws.amazon.com/lambda/home?region=us-east-1#/functions/notifySlackFromSnsTopicError
[Node.js]: https://nodejs.org/
[Production deployments]: https://github.com/sciety/sciety/actions?query=workflow%3AProduction
[Production environment]: https://sciety.org
[Staging environment]: https://staging.sciety.org
[sciety.org]: https://sciety.org
