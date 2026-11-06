# Sidekiq's own tests: minitest, and the server is at its default redis://localhost:6379/0.
export BUNDLE_PATH=/cache/sidekiq BUNDLE_JOBS=4 BUNDLE_SILENCE_ROOT_WARNING=1

setup() {
  work_on sidekiq
  bundle install --quiet
  cp Gemfile.lock /cache/sidekiq.lock
}

tests() {
  work_on sidekiq
  cp /cache/sidekiq.lock Gemfile.lock
  start_server
  timeout 900 bundle exec rake test TESTOPTS=-v >/out/sidekiq.txt 2>&1
  echo "rake test: exit $?"
  tail -5 /out/sidekiq.txt
}
