# The parts of Rails' own tests that talk to Redis: the cache store, Action
# Cable's adapter, and Active Job with Sidekiq and Resque, whose workers Rails
# starts for real. Each at the database its tests name, unless they take a URL.
export BUNDLE_PATH=/cache/rails BUNDLE_JOBS=4 BUNDLE_SILENCE_ROOT_WARNING=1

setup() {
  work_on rails
  # Resque's workers can't read their jobs with json 3: MultiJson 1.21 passes it
  # an argument it no longer takes, whichever server is behind them.
  sed -i 's/^gem "json".*/gem "json", "< 3"/' Gemfile
  bundle install --quiet
  cp Gemfile /cache/rails.gemfile
  cp Gemfile.lock /cache/rails.lock
}

part() {
  local name=$1 dir=$2
  shift 2
  (cd "$dir" && timeout 900 "$@" >"/out/$name.txt" 2>&1; echo "$name: exit $?")
  tail -3 "/out/$name.txt" | cut -c1-200
}

tests() {
  work_on rails
  cp /cache/rails.gemfile Gemfile
  cp /cache/rails.lock Gemfile.lock
  start_server
  part cache activesupport bin/test test/cache/stores/redis_cache_store_test.rb -v
  part cable actioncable bin/test test/subscription_adapter/redis_test.rb -v
  part job-sidekiq activejob bundle exec rake test:integration:sidekiq TESTOPTS=-v
  part job-resque activejob bundle exec rake test:integration:resque TESTOPTS=-v
}
