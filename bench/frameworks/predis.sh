# Predis's own tests: its commands, each against a real server, but for the groups its
# configuration leaves out (cluster, sentinel, the Stack's modules...).
export COMPOSER_HOME=/cache/composer COMPOSER_ALLOW_SUPERUSER=1

setup() {
  cd /fw/predis || exit 1
  composer install --prefer-dist --no-interaction --no-progress --ignore-platform-req='ext-*' 2>&1 | tail -1
}

tests() {
  work_on predis
  start_server
  timeout 1800 vendor/bin/phpunit --do-not-cache-result --log-junit /out/predis.xml \
    >/out/predis.log 2>&1
  echo "phpunit: exit $?"
  tail -2 /out/predis.log | cut -c1-200
}
