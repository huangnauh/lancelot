package store

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/util/execdetails"
)

const (
	slowTime = 10 * time.Millisecond
)

func execDetailsString(execDetails *execdetails.StmtExecDetails) string {
	if execDetails == nil {
		return ""
	}

	var builder strings.Builder
	waitKV := time.Duration(execDetails.WaitKVRespDuration)
	if waitKV > slowTime {
		builder.WriteString("Wait_kv_time: ")
		builder.WriteString(waitKV.String())
		builder.WriteString(" ")
	}

	waitPD := time.Duration(execDetails.WaitPDRespDuration)
	if waitPD > slowTime {
		builder.WriteString("Wait_pd_time: ")
		builder.WriteString(waitPD.String())
		builder.WriteString(" ")
	}

	backOff := time.Duration(execDetails.BackoffDuration)
	if backOff > slowTime {
		builder.WriteString("Back_off_time: ")
		builder.WriteString(backOff.String())
		builder.WriteString(" ")
	}
	return builder.String()
}

func commitDetailsString(commitDetails *execdetails.CommitDetails) string {
	if commitDetails == nil {
		return ""
	}
	var builder strings.Builder
	if commitDetails.PrewriteTime > slowTime {
		builder.WriteString("Prewrite_time: ")
		builder.WriteString(commitDetails.PrewriteTime.String())
		builder.WriteString(" ")
	}
	if commitDetails.CommitTime > slowTime {
		builder.WriteString("Commit_time: ")
		builder.WriteString(commitDetails.CommitTime.String())
		builder.WriteString(" ")
	}
	if commitDetails.GetCommitTsTime > slowTime {
		builder.WriteString("Get_commit_ts_time: ")
		builder.WriteString(commitDetails.GetCommitTsTime.String())
		builder.WriteString(" ")
	}

	commitBackOffTime := time.Duration(commitDetails.CommitBackoffTime)
	if commitBackOffTime > slowTime {
		builder.WriteString("Commit_back_off_time: ")
		builder.WriteString(commitBackOffTime.String())
		builder.WriteString(" ")
	}

	commitDetails.Mu.Lock()
	if len(commitDetails.Mu.BackoffTypes) > 0 {
		builder.WriteString("Back_off_types: ")
		builder.WriteString(fmt.Sprintf("%v", commitDetails.Mu.BackoffTypes))
		builder.WriteString(" ")
	}
	commitDetails.Mu.Unlock()

	resolveLockTime := time.Duration(commitDetails.ResolveLockTime)
	if resolveLockTime > slowTime {
		builder.WriteString("Resolve_lock_time: ")
		builder.WriteString(resolveLockTime.String())
		builder.WriteString(" ")
	}

	if commitDetails.LocalLatchTime > slowTime {
		builder.WriteString("Local_latch_wait_time: ")
		builder.WriteString(commitDetails.LocalLatchTime.String())
		builder.WriteString(" ")
	}

	if commitDetails.TxnRetry > 0 {
		builder.WriteString("Txn_retry: ")
		builder.WriteString(strconv.FormatInt(int64(commitDetails.TxnRetry), 10))
		builder.WriteString(" ")
	}
	return builder.String()
}
