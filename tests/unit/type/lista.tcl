start_server {
    tags {"list"}
} {
    source "tests/unit/type/list-common.tcl"

    test {LPUSH, RPUSH, LLENGTH, LINDEX, LPOP - ziplist} {
        # first lpush then rpush
        assert_equal 1 [r lpush myziplist1 aa]
        assert_equal 1 [r rpush myziplist1 bb]
        assert_equal 1 [r rpush myziplist1 cc]
        assert_equal 3 [r llen myziplist1]
        assert_equal aa [r lindex myziplist1 0]
        assert_equal bb [r lindex myziplist1 1]
        assert_equal cc [r lindex myziplist1 2]
        assert_equal {} [r lindex myziplist2 3]
        assert_equal cc [r rpop myziplist1]
        assert_equal aa [r lpop myziplist1]
        assert_encoding quicklist myziplist1

        # first rpush then lpush
        assert_equal 1 [r rpush myziplist2 a]
        assert_equal 1 [r lpush myziplist2 b]
        assert_equal 1 [r lpush myziplist2 c]
        assert_equal 3 [r llen myziplist2]
        assert_equal c [r lindex myziplist2 0]
        assert_equal b [r lindex myziplist2 1]
        assert_equal a [r lindex myziplist2 2]
        assert_equal {} [r lindex myziplist2 3]
        assert_equal a [r rpop myziplist2]
        assert_equal c [r lpop myziplist2]
        assert_encoding quicklist myziplist2
    }

    test {LPUSH, RPUSH, LLENGTH, LINDEX, LPOP - regular list} {
        # first lpush then rpush
        assert_equal 1 [r lpush mylist1 $largevalue(linkedlist)]
        assert_encoding quicklist mylist1
        assert_equal 1 [r rpush mylist1 b]
        assert_equal 1 [r rpush mylist1 c]
        assert_equal 3 [r llen mylist1]
        assert_equal $largevalue(linkedlist) [r lindex mylist1 0]
        assert_equal b [r lindex mylist1 1]
        assert_equal c [r lindex mylist1 2]
        assert_equal {} [r lindex mylist1 3]
        assert_equal c [r rpop mylist1]
        assert_equal $largevalue(linkedlist) [r lpop mylist1]

        # first rpush then lpush
        assert_equal 1 [r rpush mylist2 $largevalue(linkedlist)]
        assert_encoding quicklist mylist2
        assert_equal 1 [r lpush mylist2 b]
        assert_equal 1 [r lpush mylist2 c]
        assert_equal 3 [r llen mylist2]
        assert_equal c [r lindex mylist2 0]
        assert_equal b [r lindex mylist2 1]
        assert_equal $largevalue(linkedlist) [r lindex mylist2 2]
        assert_equal {} [r lindex mylist2 3]
        assert_equal $largevalue(linkedlist) [r rpop mylist2]
        assert_equal c [r lpop mylist2]
    }

    test {Variadic RPUSH/LPUSH} {
        r del mylist
        assert_equal 4 [r lpush mylist a b c d]
        assert_equal 4 [r rpush mylist 0 1 2 3]
        assert_equal {d c b a 0 1 2 3} [r lrange mylist 0 -1]
    }

    foreach {type large} [array get largevalue] {
        test "LPUSHX, RPUSHX - $type" {
            create_list xlist "$large c"
            assert_equal 1 [r rpushx xlist d]
            assert_equal 1 [r lpushx xlist a]
            assert_equal 1 [r rpushx xlist 42 x]
            assert_equal 1 [r lpushx xlist y3 y2 y1]
            assert_equal "y1 y2 y3 a $large c d 42 x" [r lrange xlist 0 -1]
        }

        test "LINSERT - $type" {
            create_list xlist "a $large c d"
            assert_equal 1 [r linsert xlist before c zz] "before c"
            assert_equal "a $large zz c d" [r lrange xlist 0 10] "lrangeA"
            assert_equal 1 [r linsert xlist after c yy] "after c"
            assert_equal "a $large zz c yy d" [r lrange xlist 0 10] "lrangeB"
            assert_equal 1 [r linsert xlist after d dd] "after d"
            assert_equal -1 [r linsert xlist after bad ddd] "after bad"
            assert_equal "a $large zz c yy d dd" [r lrange xlist 0 10] "lrangeC"
            assert_equal 1 [r linsert xlist before a aa] "before a"
            assert_equal -1 [r linsert xlist before bad aaa] "before bad"
            assert_equal "aa a $large zz c yy d dd" [r lrange xlist 0 10] "lrangeD"

            # check inserting integer encoded value
            assert_equal 1 [r linsert xlist before aa 42] "before aa"
            assert_equal 42 [r lrange xlist 0 0] "lrangeE"
        }
    }
}
