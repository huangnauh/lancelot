#!/bin/bash
./runtest --host 127.0.0.1 --port 6379 --single unit/type/string --tags "-needs:repl -needs:debug" --ignore-encoding
./runtest --host 127.0.0.1 --port 6379 --single unit/type/incr --tags "-needs:repl -needs:debug" --ignore-encoding
./runtest --host 127.0.0.1 --port 6379 --single unit/type/hash --tags "-needs:repl -needs:debug" --ignore-encoding
./runtest --host 127.0.0.1 --port 6379 --single unit/type/set --tags "-needs:repl -needs:debug" --ignore-encoding
./runtest --host 127.0.0.1 --port 6379 --single unit/type/zset --tags "-needs:repl -needs:debug" --ignore-encoding
./runtest --host 127.0.0.1 --port 6379 --single unit/type/list --tags "-needs:repl -needs:debug" --ignore-encoding
./runtest --host 127.0.0.1 --port 6379 --single unit/type/list-2 --tags "-needs:repl -needs:debug" --ignore-encoding
./runtest --host 127.0.0.1 --port 6379 --single unit/type/list-3 --tags "-needs:repl -needs:debug" --ignore-encoding
./runtest --host 127.0.0.1 --port 6379 --single unit/bitops --tags "-needs:repl -needs:debug" --ignore-encoding