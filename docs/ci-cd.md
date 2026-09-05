# OrderFlow CI/CD

GitHub Actions validates changes, deploys the Databricks Asset Bundle, and leaves runtime
orchestration to Airflow and Lakeflow. CI answers whether a commit is safe to merge. CD publishes
an accepted commit to the development Databricks workspace.

## Workflows and triggers

| Workflow | Trigger | Result |
| --- | --- | --- |
| `CI` | Pull request targeting `main` | Validates Python, the development bundle, and Airflow; it never deploys or runs the Lakeflow job. |
| `CI` | Push to `main` or manual dispatch | Repeats the same validation for the selected commit. |
| `Deploy development` | Push to `main` | Validates and deploys the `dev` bundle target, then prints the bundle summary. |
| `Deploy development` | Manual dispatch | Performs the same deployment and can optionally run the complete deployed job. |

CI cancels an older run for the same branch when a newer commit arrives. Development deployments
share one concurrency group and are never cancelled halfway through.

### CI checks

`Python validation` installs the project with its declared `dev` extra on Python 3.12, checks
Black formatting and Ruff linting, runs `tests/quality`, runs the complete pytest suite, builds the
wheel, and uploads that wheel as a short-lived workflow artifact.

`Databricks bundle validation` installs the official Databricks CLI and runs
`databricks bundle validate -t dev`. It is an authenticated check because the bundle needs a real
workspace and cluster policy. The job is skipped for pull requests whose head repository is a
fork, so the `development` environment secret is not exposed to untrusted fork code. Python and
Airflow validation still run for those pull requests.

`Airflow validation` uses Astro and Docker to install `airflow/requirements.txt`, parse the real
`orderflow_pipeline` DAG, execute Astro's DAG integrity test, and run the tests under
`airflow/tests`. CI supplies only `ORDERFLOW_JOB_ID=1`; parsing does not create an Airflow
connection, authenticate to Databricks, or execute the operator.

### Development deployment and optional run

CD checks out `github.sha`, sets up Python 3.12 and the official Databricks CLI, and executes:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle summary -t dev
```

The bundle's existing `orderflow_wheel` artifact configuration builds the wheel during deployment.
A normal push to `main` stops after the summary. The full pipeline is deliberately not run for
every pull request or merge because it consumes Databricks compute, writes development data, and
is an integration operation rather than a source validation check.

To request a controlled integration run, open **Actions > Deploy development > Run workflow**,
select the commit or branch, enable `run_pipeline`, and run the workflow. Optionally set
`batch_id` to an ISO date (`YYYY-MM-DD`); leaving it blank preserves the bundle's job-start-date
default. After a successful deployment, GitHub Actions runs the exact bundle resource key
`orderflow_pipeline` and forwards the value with the Databricks CLI job-parameter syntax
`-- --batch_id <value>`.

## Required GitHub configuration

Create a GitHub Environment named exactly `development`. Both authenticated bundle validation and
CD read the following values from it:

| Type | Name | Purpose |
| --- | --- | --- |
| Environment variable | `DATABRICKS_HOST` | Azure Databricks development workspace URL. |
| Environment variable | `DATABRICKS_CLUSTER_POLICY_ID` | Value passed to bundle variable `cluster_policy_id` as `BUNDLE_VAR_cluster_policy_id`. |
| Environment secret | `DATABRICKS_TOKEN` | Credential used to validate, deploy, summarize, and optionally run the development bundle. |

The token must belong to an identity authorized for the development workspace, the configured
cluster policy, and the bundle resources. A PAT is acceptable for this development portfolio
environment. A production system should replace it with workload identity federation or a
service principal and short-lived credentials.

In the GitHub UI:

1. Open **Settings > Environments > New environment**, enter `development`, and create it.
2. Under that environment, add `DATABRICKS_HOST` and `DATABRICKS_CLUSTER_POLICY_ID` as variables.
3. Add `DATABRICKS_TOKEN` under **Environment secrets**. Never add its value to a variable or file.
4. Run `CI` once so its check names become available to repository rules.
5. Protect `main` under **Settings > Rules > Rulesets** (or **Branches**) and require pull requests
   plus `CI / Python validation`, `CI / Databricks bundle validation`, and
   `CI / Airflow validation` before merging.

Environment approval or deployment-branch rules can add protection, but they also apply to the
authenticated bundle-validation job because it uses the same environment. Configure those rules
only if the resulting approval behavior is intentional.

## Runtime responsibility and failures

GitHub Actions validates and deploys code. Local Airflow triggers the one deployed Lakeflow job;
it is validated here but is not deployed by GitHub Actions. Lakeflow executes the Databricks task
graph, and Databricks performs the PySpark transformations and data-quality checks.

At runtime, a critical data-quality failure raises `DataQualityFailure` in
`gold_quality_checks`. That fails the final Lakeflow task and therefore the complete job. The
Airflow `DatabricksRunNowOperator` waits for the terminal job result, so the failed Lakeflow result
propagates naturally to the Airflow task and DAG run.

## Local equivalents

From the repository root, using Python 3.12:

```bash
python -m pip install -e ".[dev]"
black --check src tests scripts
ruff check src tests scripts
python -m pytest tests/quality -q
python -m pytest -q
python -m build --wheel
databricks bundle validate -t dev
```

Bundle validation requires a configured workspace credential and either the existing
`cluster_policy_id` lookup to succeed or `BUNDLE_VAR_cluster_policy_id` to be set explicitly.
Deploy only as a deliberate development operation:

```bash
databricks bundle deploy -t dev
databricks bundle summary -t dev
```

From `airflow/`, with Docker running:

```bash
printf '%s\n' 'ORDERFLOW_JOB_ID=1' > .env
astro dev parse
astro dev pytest
```

The local `.env` file is ignored by Git. These commands parse and test the DAG; they do not deploy
Airflow or call Databricks.
