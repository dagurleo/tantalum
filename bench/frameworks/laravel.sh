# Every test file in Laravel's framework that names Redis, once with phpredis and
# once with Predis, each file in a PHPUnit of its own. Its tests use databases 5
# and 6; DATABASE, if set, moves them all to that one.
export COMPOSER_HOME=/cache/composer COMPOSER_ALLOW_SUPERUSER=1

setup() {
  cd /fw/framework || exit 1
  # Extensions only for other drivers, such as FTP, aren't needed for these tests.
  composer update --prefer-dist --no-interaction --no-progress --no-audit --ignore-platform-req='ext-*' 2>&1 | tail -3
}

tests() {
  work_on framework
  [ -z "${DATABASE:-}" ] || sed -i "s/'database' => [56],/'database' => $DATABASE,/" \
    src/Illuminate/Foundation/Testing/Concerns/InteractsWithRedis.php
  start_server
  export REDIS_HOST=127.0.0.1 REDIS_PORT=6379
  for client in phpredis predis; do
    mkdir -p "/out/$client"
    grep -rl 'InteractsWithRedis\|Redis' tests --include='*Test.php' | sort | while read -r file; do
      name=$(echo "$file" | tr '/' '-')
      REDIS_CLIENT=$client timeout 300 vendor/bin/phpunit --no-progress --do-not-cache-result \
        --log-junit "/out/$client/$name.xml" "$file" >"/out/$client/$name.log" 2>&1
      echo "$client $file: exit $?"
    done
  done
}
