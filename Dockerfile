FROM jfrog.fkinternal.com/indradhanush/openjdk8-builder:1619315215 as builder
ARG AppName
COPY ./target/lib/* /build/lib/
RUN mv /build/lib/hbase-compactor-*.jar /build

FROM jfrog.fkinternal.com/runtime/debian-10/java-jre:8u202 as baseruntime
ARG AppName
RUN addgroup --gid 1011 ejxipedt
RUN useradd --create-home --uid 1011 --gid 1011 --shell /bin/bash --system ejxipedt
COPY --from=builder [ "/build/hbase-compactor-*.jar", "/var/lib/${AppName}/" ]
COPY --from=builder [ "/build/lib/*", "/var/lib/${AppName}/lib/" ]
COPY --from=builder [ "/entrypoint", "/entrypoint" ]

RUN sed -i "s/__PACKAGE__/$AppName/g" "/entrypoint"
RUN chmod +x "/entrypoint"
