FROM ruby:alpine

COPY . /app
WORKDIR /app
RUN apk add --no-cache ruby-dev build-base curl
RUN gem install -N rack rackup webrick ox prometheus-client typhoeus
CMD ["rackup", "-o", "0.0.0.0"]
