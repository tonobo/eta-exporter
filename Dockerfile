FROM ruby:alpine

RUN mkdir -p /app
COPY Gemfile Gemfile.lock /app
WORKDIR /app
RUN apk add --no-cache ruby-dev build-base curl bash
RUN bundle install
COPY config.ru /app
CMD ["bundle", "exec", "rackup", "-o", "0.0.0.0"]
