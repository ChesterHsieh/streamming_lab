FROM ruby:3.1-alpine
RUN apk add --no-cache curl
RUN gem install sinatra
RUN gem install rackup
COPY app.rb .
ENTRYPOINT ["ruby", "app.rb"]