# redis-py's own tests, sync and asyncio, on one server: no cluster, modules,
# TLS, replicas or the cloud's.
venv() { . /cache/redis-py/bin/activate; }

setup() {
  python -m venv /cache/redis-py
  venv
  work_on redis-py
  pip install -q --upgrade pip
  pip install -q . -r dev_requirements.txt pytest-timeout
}

tests() {
  venv
  work_on redis-py
  start_server
  timeout 3000 python -m pytest tests -p no:cacheprovider --timeout=120 \
    -m 'not onlycluster and not redismod and not ssl and not replica and not cp_integration' \
    --ignore=tests/test_scenario --ignore=tests/maint_notifications --ignore=tests/test_multidb \
    --ignore=tests/test_http --ignore=tests/test_auth --ignore=tests/test_cluster.py \
    --ignore=tests/test_cluster_transaction.py --ignore=tests/test_asyncio/test_cluster.py \
    --ignore=tests/test_asyncio/test_scenario --ignore=tests/test_asyncio/test_multidb \
    --junitxml=/out/redis-py.xml >/out/redis-py.txt 2>&1
  echo "pytest: exit $?"
  tail -2 /out/redis-py.txt | cut -c1-200
}
