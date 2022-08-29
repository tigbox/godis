package cluster

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/tigbox/godis/redis/connection"
	"github.com/tigbox/godis/redis/reply/asserts"
)

func TestRollback(t *testing.T) {
	// rollback uncommitted transaction
	conn := new(connection.FakeConn)
	FlushAll(testCluster, conn, toArgs("FLUSHALL"))
	txID := rand.Int63()
	txIDStr := strconv.FormatInt(txID, 10)
	keys := []string{"a", "b"}
	groupMap := testCluster.groupBy(keys)
	args := []string{txIDStr, "DEL"}
	args = append(args, keys...)
	testCluster.Exec(conn, toArgs("SET", "a", "a"))
	ret := execPrepare(testCluster, conn, makeArgs("Prepare", args...))
	asserts.AssertNotError(t, ret)
	requestRollback(testCluster, conn, txID, groupMap)
	ret = testCluster.Exec(conn, toArgs("GET", "a"))
	asserts.AssertBulkReply(t, ret, "a")

	// rollback committed transaction
	FlushAll(testCluster, conn, toArgs("FLUSHALL"))
	txID = rand.Int63()
	txIDStr = strconv.FormatInt(txID, 10)
	args = []string{txIDStr, "DEL"}
	args = append(args, keys...)
	testCluster.Exec(conn, toArgs("SET", "a", "a"))
	ret = execPrepare(testCluster, conn, makeArgs("Prepare", args...))
	asserts.AssertNotError(t, ret)
	_, err := requestCommit(testCluster, conn, txID, groupMap)
	if err != nil {
		t.Errorf("del failed %v", err)
		return
	}
	ret = testCluster.Exec(conn, toArgs("GET", "a"))
	asserts.AssertNullBulk(t, ret)
	requestRollback(testCluster, conn, txID, groupMap)
	ret = testCluster.Exec(conn, toArgs("GET", "a"))
	asserts.AssertBulkReply(t, ret, "a")
}
