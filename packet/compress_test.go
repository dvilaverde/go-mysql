package packet_test

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/client"
	_ "github.com/go-mysql-org/go-mysql/driver"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/siddontang/go/log"
	"github.com/stretchr/testify/require"
)

var (
	testAddr     = flag.String("host", "127.0.0.1:3307", "MySQL host")
	testUser     = flag.String("user", "root", "MySQL user")
	testPassword = flag.String("pass", "123456", "MySQL password")
	testDB       = flag.String("db", "test", "MySQL test database")
)

var _ server.Handler = &mockHandler{}

type testServer struct {
	*server.Server

	listener net.Listener
	handler  *mockHandler
}

type mockHandler struct {
	data []byte
}

func TestCompress_CompressionOn(t *testing.T) {
	log.SetLevel(log.LevelDebug)
	srv := CreateMockServer(t)

	c, err := client.ConnectWithContext(context.TODO(), *testAddr, *testUser, *testPassword, *testDB, func(c *client.Conn) {
		c.SetCapability(mysql.CLIENT_COMPRESS)
	})
	require.NoError(t, err)

	var b strings.Builder
	b.Grow(mysql.MaxPayloadLen + 100)
	for i := 0; i < 475000; i++ {
		b.WriteString(uuid.New().String())
	}

	r, err := c.Execute("INSERT INTO table (bigtextcol) VALUES (?)", b.String())
	require.NoError(t, err)
	id := r.InsertId
	require.Equal(t, uint64(1), id)

	require.NotEmpty(t, srv.handler.data)

	r, err = c.Execute("SELECT id, bigtextcol from table WHERE ID = ?", 1)
	require.NoError(t, err)

	bigtextcol, err := r.Resultset.GetValue(0, 1)
	require.NoError(t, err)

	require.Equal(t, []byte(b.String()), bigtextcol)

	c.Close()
	srv.Stop()
}

func TestCompress_CompressionOff(t *testing.T) {
	log.SetLevel(log.LevelDebug)
	srv := CreateMockServer(t)

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@%s/%s", *testUser, *testPassword, *testAddr, *testDB))
	require.NoError(t, err)
	db.SetMaxIdleConns(4)

	var b strings.Builder
	b.Grow(mysql.MaxPayloadLen + 100)
	for i := 0; i < 475000; i++ {
		b.WriteString(uuid.New().String())
	}

	r, err := db.ExecContext(context.TODO(), "INSERT INTO table (bigtextcol) VALUES (?)", b.String())
	require.NoError(t, err)
	id, err := r.LastInsertId()
	require.NoError(t, err)
	require.Equal(t, int64(1), id)

	require.NotEmpty(t, srv.handler.data)

	row := db.QueryRowContext(context.TODO(), "SELECT id, bigtextcol from table WHERE ID = ?", 1)
	require.NoError(t, row.Err())

	var rowId int
	var bigtextcol []byte
	require.NoError(t, row.Scan(&rowId, &bigtextcol))
	require.Equal(t, []byte(b.String()), bigtextcol)

	db.Close()
	srv.Stop()
}

func CreateMockServer(t *testing.T) *testServer {
	inMemProvider := server.NewInMemoryProvider()
	inMemProvider.AddUser(*testUser, *testPassword)
	defaultServer := server.NewDefaultServer()

	l, err := net.Listen("tcp", *testAddr)
	require.NoError(t, err)

	handler := &mockHandler{}

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}

			go func() {
				co, err := server.NewCustomizedConn(conn, defaultServer, inMemProvider, handler)
				require.NoError(t, err)
				for {
					err = co.HandleCommand()
					if err != nil {
						return
					}
				}
			}()
		}
	}()

	return &testServer{
		Server:   defaultServer,
		listener: l,
		handler:  handler,
	}
}

func (s *testServer) Stop() {
	s.listener.Close()
}

func (h *mockHandler) UseDB(dbName string) error {
	return nil
}

func (h *mockHandler) HandleQuery(query string) (*mysql.Result, error) {
	r, err := mysql.BuildSimpleResultset([]string{"id", "bigtextcol"}, [][]interface{}{
		{int32(1), h.data},
	}, true)

	if err != nil {
		return nil, errors.Trace(err)
	}

	return &mysql.Result{
		Status:       0,
		Warnings:     0,
		InsertId:     0,
		AffectedRows: 0,
		Resultset:    r,
	}, nil
}

func (h *mockHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	return nil, nil
}

func (h *mockHandler) HandleStmtPrepare(query string) (params int, columns int, context interface{}, err error) {
	params = 1
	columns = 0
	return params, columns, nil, nil
}

func (h *mockHandler) HandleStmtExecute(context interface{}, query string, args []interface{}) (*mysql.Result, error) {

	if strings.HasPrefix(strings.ToLower(query), "select") {
		return h.HandleQuery(query)
	}

	h.data = args[0].([]byte)
	return &mysql.Result{
		Status:       0,
		Warnings:     0,
		InsertId:     1,
		AffectedRows: 0,
		Resultset:    nil,
	}, nil
}

func (h *mockHandler) HandleStmtClose(context interface{}) error {
	return nil
}

func (h *mockHandler) HandleOtherCommand(cmd byte, data []byte) error {
	return nil
}
