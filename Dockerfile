FROM ruby:alpine

COPY . /app
WORKDIR /app
RUN apk add --no-cache ruby-dev build-base curl
RUN bundle install --path .bundle --deployment
CMD ["bundle", "exec", "rackup", "-o", "0.0.0.0"]
