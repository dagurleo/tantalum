# Kombu's Redis integration tests: the library under Celery, as its tox runs them.
venv() { . /cache/kombu/bin/activate; }

setup() {
  python -m venv /cache/kombu
  venv
  work_on kombu
  pip install -q --upgrade pip
  pip install -q ".[redis]" -r requirements/test.txt -r requirements/test-ci.txt pytest-rerunfailures
}

tests() {
  venv
  work_on kombu
  start_server
  timeout 900 python -m pytest -v -p no:cacheprovider -E redis --junitxml=/out/kombu.xml \
    t/integration/test_redis.py >/out/kombu.txt 2>&1
  echo "pytest: exit $?"
  tail -5 /out/kombu.txt | cut -c1-200
}
