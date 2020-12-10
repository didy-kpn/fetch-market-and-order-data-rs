FROM rust:latest
MAINTAINER Didy KUPANHY <d.kupanhy@gmail.com>

ENV APP_ROOT=/usr/src/fetch-market-and-order-data-rs
RUN git clone https://github.com/didy-kpn/fetch-market-and-order-data-rs $APP_ROOT
WORKDIR $APP_ROOT

RUN git checkout develop
RUN cargo install --path .

RUN mkdir -p /var/log/fetch-market-and-order-data
RUN touch /var/log/fetch-market-and-order-data/output.log

VOLUME /var/log/fetch-market-and-order-data

CMD /usr/local/cargo/bin/fetch-market-and-order-data -o /var/log/fetch-market-and-order-data >> output.log 2>&1
