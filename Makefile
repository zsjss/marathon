
marathon-runnable.jar: jar = $(wildcard target/marathon-*-dependencies.jar)
marathon-runnable.jar: run = bin/marathon-framework
marathon-runnable.jar: $(run) $(jar)
	cat $(run) $(jar) > $@

.PHONY: proto
proto:
	protoc -I ~/sandbox/mesos/include/mesos/:src/main/proto/ \
	       --java_out src/main/java src/main/proto/marathon.proto

