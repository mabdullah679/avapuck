AVAPUCK, IN PLAIN WORDS

What it is
Avapuck is a small data platform that shows how private information can move safely through a company. It takes a hundred rows from a public Google BigQuery dataset every four hours, or any spreadsheet (CSV file) you upload through a web console. It locks the private columns with strong encryption, passes the data through a message system (Kafka), hides sensitive values with policy rules (Apache Ranger) in a Hive table, loads the result into a Postgres database, writes one PDF report, and finally publishes one digitally signed yes-or-no decision, called the verdict, that other teams read to decide whether to go ahead.

It is a proof of concept. The file TRUST-BOUNDARY.md lists, honestly, what is faked or unproven. It is not meant for real credit card data.

What is not on your machine
Live BigQuery needs a Google Cloud key file that you supply, saved as secrets/gcp-sa.json. Without it the pipeline uses built-in sample data.

What you need
To run the whole thing you need Docker with Compose. It starts about fifteen containers, so allow several gigabytes of memory and a long first download. Nothing else has to be installed. To run only the tests you need Python 3.12. No Docker, no cloud account, no passwords.

How to run it
1. Run ./setup.sh. It creates a settings file called .env.local with development passwords, makes certificates, starts everything, sets up the Kafka topics, and runs one cycle. It is safe to run again.
2. Open http://localhost:8447 in a browser. On a development machine you are signed in automatically. Upload a CSV and watch the Runs tab.
3. To make a test file, run: python3 scripts/make_test_csv.py --rows 1000 > small.csv
4. Stop with ./setup.sh --down. Adding --clean also deletes the stored data.

If something will not start
Check docs/RUNBOOK.md, section Common failures. The Docker project is named avapuck, so a second copy of the repo on the same computer would share the first copy's storage.

What it does
A scheduled run happens every four hours and takes at most 100 BigQuery rows. This limit is enforced in the query and checked again afterwards, because BigQuery costs money.
An uploaded CSV makes one run with no row limit unless you set UPLOAD_MAX_ROWS. It is processed in chunks so a huge file and a tiny file take the same path.
Private columns are encrypted before they are saved anywhere. Two checks in the code, assert_no_plaintext and _assert_no_plaintext_published, refuse to continue if readable private data shows up.
Hiding of sensitive values follows Ranger policy files in config/ranger. For a new dataset, a policy file is generated once for a person to review.
Every run, even a failed one, ends by publishing a signed verdict. Other teams read only its proceed field.
Re-running the same time slot replaces that slot's data and never loads it twice into Postgres.
If a later file has different columns than the first one of that dataset, the run stops at the first step and lists the differences.
A watchdog service reports a problem if a verdict does not arrive on time.

Services and who may call them
Console on port 8447: login, upload, start runs, view runs, Kafka log, Spark status, traffic. You must be logged in through the identity service, except for login and the health check.
Identity service on 8443: hands out short-lived tokens and signs verdicts.
Crypto service on 8444: encrypt and decrypt, only with a valid token that has the crypto permission.
Watchdog on 8446 and the Kafka tap: status pages with no login in the code.
Scaler on 8448: sets the number of Spark workers. It has no login in the code and it controls Docker, which is a known risk described in TRUST-BOUNDARY.md section 4.8.

Tests
Create a virtual environment with Python 3.12, install requirements-ci.txt, then run make ci. It first runs the Python tests and checks the example messages against their schemas, then checks that each message schema still works with the version on main. On 2026-11-18 the result was 318 passed and 4 skipped. The skipped ones need the Google BigQuery library, an extract secret, or a service-account key. The tests need no outside services and do not start the Docker stack. The same two steps run automatically in GitHub Actions together with a secret scan, and in a local Jenkins.

Where things are
pipeline: the processing steps, with stages.py as the one list of steps and verdict.py deciding the verdict.
dags: the two Airflow workflows.
services: identity, crypto, console, Kafka tap, watchdog, scaler.
contracts: message schemas and examples.
config: dataset contracts, Ranger policies, BigQuery queries, traffic limits.
docker and docker-compose.yml: container images and the stack.
scripts: contract checker, git hooks, certificate and test-file generators.
tests: the test suite.
docs: architecture, decisions, runbook, how to consume the verdict, execution flow.
AGENTS.md and CLAUDE.md: working rules for anyone changing the code.

Why it is built this way
Encryption happens before data is stored, not inside the warehouse, because otherwise readable data would sit in Kafka and files. Masking is a policy file rather than code, so security can review it. The verdict is a plain true or false produced by one function, so other teams do not depend on log wording. CI runs in both Jenkins and GitHub Actions, but both call the same make commands, so they cannot drift apart.
