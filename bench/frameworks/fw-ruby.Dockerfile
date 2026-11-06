FROM ruby:3.4-bookworm
RUN apt-get update -qq && apt-get install -y -qq socat >/dev/null
