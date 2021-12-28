# We need a value larger than list-max-ziplist-value to make sure
# the list has the right encoding when it is swapped in again.
array set largevalue {}
set largevalue(ziplist) "hello"
set largevalue(linkedlist) [string repeat "hello" 4]

proc create_list {key entries} {
        r del $key
        foreach entry $entries { r rpush $key $entry }
        assert_encoding quicklist $key
}