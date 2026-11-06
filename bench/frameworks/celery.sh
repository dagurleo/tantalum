# Celery's integration tests, with Redis as both broker and result backend, as
# its tox environment `integration-redis` runs them, except that a failure
# doesn't end the run (no -x).
venv() { . /cache/celery/bin/activate; }

setup() {
  python -m venv /cache/celery
  venv
  work_on celery
  pip install -q --upgrade pip
  pip install -q . -r requirements/test.txt -r requirements/test-integration.txt
}

tests() {
  venv
  work_on celery
  start_server
  export TEST_BROKER=redis:// TEST_BACKEND=redis:// WORKER_LOGLEVEL=INFO PYTHONUNBUFFERED=1
  # shellcheck disable=SC2086
  timeout 7200 python -m pytest -vv -p no:cacheprovider --timeout=600 --junitxml=/out/celery.xml \
    ${ONLY:-t/integration} >/out/celery.txt 2>&1
  echo "pytest: exit $?"
  tail -5 /out/celery.txt | cut -c1-200
}
