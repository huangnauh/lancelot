start_server {
    tags {"list"}
} {
    source "tests/unit/type/list-common.tcl"

    foreach {type large} [array get largevalue] {
        tags {"slow"} {
            test "LTRIM stress testing - $type" {
                set mylist {}
                set startlen 32
                r del mylist

                # Start with the large value to ensure the
                # right encoding is used.
                r rpush mylist $large
                lappend mylist $large

                for {set i 0} {$i < $startlen} {incr i} {
                    set str [randomInt 9223372036854775807]
                    r rpush mylist $str
                    lappend mylist $str
                }

                for {set i 0} {$i < 100} {incr i} {
                    set min [expr {int(rand()*$startlen)}]
                    set max [expr {$min+int(rand()*$startlen)}]
                    set before_len [llength $mylist]
                    set before_len_r [r llen mylist]
                    assert_equal $before_len $before_len_r
                    set mylist [lrange $mylist $min $max]
                    set myinfo1 [r linfo mylist]
                    r ltrim mylist $min $max
                    set myinfo2 [r linfo mylist]
                    assert_equal $mylist [r lrange mylist 0 -1] "failed $i trim $min $max $before_len $before_len_r ($myinfo1) ($myinfo2)"

                    for {set j [r llen mylist]} {$j < $startlen} {incr j} {
                        set str [randomInt 9223372036854775807]
                        r rpush mylist $str
                        lappend mylist $str
                        assert_equal $mylist [r lrange mylist 0 -1] "failed append match"
                    }
                }
            }
        }
    }
}
