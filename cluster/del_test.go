package cluster

import (
	"testing"

	"github.com/tigbox/godis/redis/connection"
	"github.com/tigbox/godis/redis/reply/asserts"
)

func TestDel(t *testing.T) {
	conn := &connection.FakeConn{}
	allowFastTransaction = false
	testCluster.Exec(conn, toArgs("SET", "a", "a"))
	ret := Del(testCluster, conn, toArgs("DEL", "a", "b", "c"))
	asserts.AssertNotError(t, ret)
	ret = testCluster.Exec(conn, toArgs("GET", "a"))
	asserts.AssertNullBulk(t, ret)
}
