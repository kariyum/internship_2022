FROM openjdk:8-jre

COPY ./target/pack/bin ./bin
COPY ./target/pack/lib ./lib

EXPOSE 80
ENV HOSTPORT "5050"
ENV HOSTIP "api"

CMD ["./bin/main"]
