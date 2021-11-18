#!/bin/bash

TAGS="-needs:repl -needs:debug -needs:stralgo -needs:rand -needs:bitfield -large-memory -needs:config -needs:config-maxmemory -needs:stream -needs:dump -needs:client -needs:encoding -needs:resp3 -needs:blmpop -needs:resp3 -needs:aof -needs:info -needs:sort -cluster:skip"
./runtest --host 127.0.0.1 --port 6379 --single unit/type/string --tags "$TAGS" --ignore-encoding
sleep 2
./runtest --host 127.0.0.1 --port 6379 --single unit/type/incr --tags "$TAGS" --ignore-encoding
sleep 2
./runtest --host 127.0.0.1 --port 6379 --single unit/type/hash --tags "$TAGS" --ignore-encoding
sleep 2
./runtest --host 127.0.0.1 --port 6379 --single unit/type/set --tags "$TAGS" --ignore-encoding
sleep 2
./runtest --host 127.0.0.1 --port 6379 --single unit/type/zset --tags "$TAGS" --ignore-encoding
sleep 2
./runtest --host 127.0.0.1 --port 6379 --single unit/type/list --tags "$TAGS" --ignore-encoding
sleep 2
./runtest --host 127.0.0.1 --port 6379 --single unit/type/list-2 --tags "$TAGS" --ignore-encoding
sleep 2
./runtest --host 127.0.0.1 --port 6379 --single unit/type/list-3 --tags "$TAGS" --ignore-encoding
sleep 2
./runtest --host 10.0.5.137 --port 16379 --single unit/bitops --tags "$TAGS" --ignore-encoding
sleep 2
./runtest --host 127.0.0.1 --port 6379 --single unit/scan --tags "$TAGS" --ignore-encoding
sleep 2
./runtest --host 127.0.0.1 --port 6379 --single unit/multi --tags "$TAGS" --ignore-encoding
sleep 2
./runtest --host 127.0.0.1 --port 6379 --single unit/expire --tags "$TAGS" --ignore-encoding
sleep 2
./runtest --host 127.0.0.1 --port 6379 --single unit/scripting --tags "$TAGS" --ignore-encoding
