FROM jfrog.fkinternal.com/indradhanush/openjdk8-builder:1619315215 as builder
ARG AppName
COPY . /build/

FROM jfrog.fkinternal.com/runtime/debian-10/java-jre:8u202 as baseruntime
ARG AppName
RUN apt-get update && apt-get install -y procps ngrep netcat curl iputils-ping
COPY --from=builder [ "/build/target/hbase-compactor-*.jar", "/var/lib/${AppName}/" ]
COPY --from=builder [ "/build/target/lib/*", "/var/lib/${AppName}/lib/" ]
COPY --from=builder [ "/entrypoint", "/entrypoint" ]

RUN sed -i "s/__PACKAGE__/$AppName/g" "/entrypoint"
RUN chmod +x "/entrypoint"
ENTRYPOINT ["/entrypoint"]

CMD ["com.flipkart.yak.HCompactor"]