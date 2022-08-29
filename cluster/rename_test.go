package cluster

import (
	"fmt"
	"testing"

	"github.com/tigbox/godis/lib/utils"
	"github.com/tigbox/godis/redis/connection"
	"github.com/tigbox/godis/redis/reply"
	"github.com/tigbox/godis/redis/reply/asserts"
)

func TestRename(t *testing.T) {
	conn := new(connection.FakeConn)
	testDB := testCluster.db
	testDB.Exec(conn, utils.ToCmdLine("FlushALL"))
	key := utils.RandString(10)
	value := utils.RandString(10)
	newKey := key + utils.RandString(2)
	testDB.Exec(conn, utils.ToCmdLine("SET", key, value, "ex", "1000"))
	result := Rename(testCluster, conn, utils.ToCmdLine("RENAME", key, newKey))
	if _, ok := result.(*reply.OkReply); !ok {
		t.Error("expect ok")
		return
	}
	result = testDB.Exec(conn, utils.ToCmdLine("EXISTS", key))
	asserts.AssertIntReply(t, result, 0)
	result = testDB.Exec(conn, utils.ToCmdLine("EXISTS", newKey))
	asserts.AssertIntReply(t, result, 1)
	// check ttl
	result = testDB.Exec(conn, utils.ToCmdLine("TTL", newKey))
	intResult, ok := result.(*reply.IntReply)
	if !ok {
		t.Error(fmt.Sprintf("expected int reply, actually %s", result.ToBytes()))
		return
	}
	if intResult.Code <= 0 {
		t.Errorf("expected ttl more than 0, actual: %d", intResult.Code)
		return
	}
}

func TestRenameNx(t *testing.T) {
	conn := new(connection.FakeConn)
	testDB := testCluster.db
	testDB.Exec(conn, utils.ToCmdLine("FlushALL"))
	key := utils.RandString(10)
	value := utils.RandString(10)
	newKey := key + utils.RandString(2)
	testCluster.db.Exec(conn, utils.ToCmdLine("SET", key, value, "ex", "1000"))
	result := RenameNx(testCluster, conn, utils.ToCmdLine("RENAMENX", key, newKey))
	asserts.AssertIntReply(t, result, 1)
	result = testDB.Exec(conn, utils.ToCmdLine("EXISTS", key))
	asserts.AssertIntReply(t, result, 0)
	result = testDB.Exec(conn, utils.ToCmdLine("EXISTS", newKey))

	asserts.AssertIntReply(t, result, 1)
	result = testDB.Exec(conn, utils.ToCmdLine("TTL", newKey))
	intResult, ok := result.(*reply.IntReply)
	if !ok {
		t.Error(fmt.Sprintf("expected int reply, actually %s", result.ToBytes()))
		return
	}
	if intResult.Code <= 0 {
		t.Errorf("expected ttl more than 0, actual: %d", intResult.Code)
		return
	}
}
