FROM rust:latest
MAINTAINER Didy KUPANHY <d.kupanhy@gmail.com>

ENV APP_ROOT=/usr/src/fetch-market-and-order-data-rs
RUN git clone https://github.com/didy-kpn/fetch-market-and-order-data-rs $APP_ROOT
WORKDIR $APP_ROOT

RUN git checkout main
RUN cargo install --path .

ENV LOG_DIR=/var/log/fetch-market-and-order-data
ENV OUTPUT_LOG=$LOG_DIR/output.log

RUN mkdir -p $LOG_DIR
VOLUME $LOG_DIR
RUN touch $OUTPUT_LOG

CMD /usr/local/cargo/bin/fetch-market-and-order-data -o $LOG_DIR >> $OUTPUT_LOG 2>&1
