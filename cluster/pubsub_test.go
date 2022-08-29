package cluster

import (
	"testing"

	"github.com/tigbox/godis/lib/utils"
	"github.com/tigbox/godis/redis/connection"
	"github.com/tigbox/godis/redis/parser"
	"github.com/tigbox/godis/redis/reply/asserts"
)

func TestPublish(t *testing.T) {
	channel := utils.RandString(5)
	msg := utils.RandString(5)
	conn := &connection.FakeConn{}
	Subscribe(testCluster, conn, utils.ToCmdLine("SUBSCRIBE", channel))
	conn.Clean() // clean subscribe success
	Publish(testCluster, conn, utils.ToCmdLine("PUBLISH", channel, msg))
	data := conn.Bytes()
	ret, err := parser.ParseOne(data)
	if err != nil {
		t.Error(err)
		return
	}
	asserts.AssertMultiBulkReply(t, ret, []string{
		"message",
		channel,
		msg,
	})

	// unsubscribe
	UnSubscribe(testCluster, conn, utils.ToCmdLine("UNSUBSCRIBE", channel))
	conn.Clean()
	Publish(testCluster, conn, utils.ToCmdLine("PUBLISH", channel, msg))
	data = conn.Bytes()
	if len(data) > 0 {
		t.Error("expect no msg")
	}

	// unsubscribe all
	Subscribe(testCluster, conn, utils.ToCmdLine("SUBSCRIBE", channel))
	UnSubscribe(testCluster, conn, utils.ToCmdLine("UNSUBSCRIBE"))
	conn.Clean()
	Publish(testCluster, conn, utils.ToCmdLine("PUBLISH", channel, msg))
	data = conn.Bytes()
	if len(data) > 0 {
		t.Error("expect no msg")
	}
}
