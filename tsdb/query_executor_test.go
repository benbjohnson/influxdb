package tsdb_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/tsdb"
)

const (
	// DefaultDatabase is the default database name used by tests.
	DefaultDatabase = "db0"

	// DefaultRetentionPolicy is the default retention policy name used by tests.
	DefaultRetentionPolicy = "rp0"
)

// Ensure the query executor can execute a basic query.
func TestQueryExecutor_ExecuteQuery_Select(t *testing.T) {
	sh := MustOpenShard()
	defer sh.Close()
	sh.MustWritePointsString(`
cpu,region=serverA value=1 0
cpu,region=serverA value=2 10
cpu,region=serverB value=3 20
`)

	e := NewQueryExecutor()
	e.MetaStore.ShardIDsByTimeRangeFn = func(sources influxql.Sources, tmin, tmax time.Time) (a []uint64, err error) {
		if !reflect.DeepEqual(sources, influxql.Sources{&influxql.Measurement{Database: "db0", RetentionPolicy: "rp0", Name: "cpu"}}) {
			t.Fatalf("unexpected sources: %s", spew.Sdump(sources))
		} else if tmin.IsZero() {
			t.Fatalf("unexpected tmin: %s", tmin)
		} else if tmax.IsZero() {
			t.Fatalf("unexpected tmax: %s", tmax)
		}
		return []uint64{100}, nil
	}
	e.Store.ShardsFn = func(ids []uint64) []*tsdb.Shard {
		if !reflect.DeepEqual(ids, []uint64{100}) {
			t.Fatalf("unexpected shard ids: %+v", ids)
		}
		return []*tsdb.Shard{sh.Shard}
	}

	res := e.MustExecuteQueryString("db0", `SELECT value FROM cpu`)
	if s := MustMarshalJSON(res); s != `[{"series":[{"name":"cpu","columns":["time","value"],"values":[["1970-01-01T00:00:00Z",1],["1970-01-01T00:00:10Z",2],["1970-01-01T00:00:20Z",3]]}]}]` {
		t.Fatalf("unexpected results: %s", s)
	}
}

// Ensure the query executor can select from a tsdb.Store.
func TestQueryExecutor_ExecuteQuery_Select_Wildcard_Intg(t *testing.T) {
	s := MustOpenStore()
	defer s.Close()

	s.MustCreateShardWithData("db0", "rp0", 0,
		`cpu,host=serverA value=1 0`,
		`cpu,host=serverA value=2 10`,
		`cpu,host=serverB value=3 20`,
	)

	res := NewQueryExecutorStore(s).MustExecuteQueryStringJSON("db0", `SELECT * FROM cpu`)
	if res != `[{"series":[{"name":"cpu","columns":["time","host","value"],"values":[["1970-01-01T00:00:00Z","serverA",1],["1970-01-01T00:00:10Z","serverA",2],["1970-01-01T00:00:20Z","serverB",3]]}]}]` {
		t.Fatalf("unexpected results: %s", res)
	}
}

// Ensure the query executor returns an empty set if no points are returned.
/*
func TestQueryExecutor_ExecuteQuery_Select_Empty(t *testing.T) {
	e := NewQueryExecutor()

	// Return an empty iterator.
	e.IteratorCreator.CreateIteratorFn = func(opt influxql.IteratorOptions) (influxql.Iterator, error) {
		return &FloatIterator{}, nil
	}

	res := e.MustExecuteQueryString("db0", `SELECT value FROM cpu`)
	if MustMarshalJSON(res) != `[{}]` {
		t.Fatalf("unexpected results: %s", spew.Sdump(res))
	}
}
*/
// Ensure the query executor can execute a DROP SERIES statement.
func TestQueryExecutor_ExecuteQuery_DropSeries(t *testing.T) {
	e := NewQueryExecutor()

	e.Store.DeleteSeriesFn = func(database string, sources influxql.Sources, condition influxql.Expr) error {
		if database != `db0` {
			t.Fatalf("unexpected database: %s", database)
		} else if !reflect.DeepEqual(sources, influxql.Sources{&influxql.Measurement{Database: "db0", RetentionPolicy: "rp0", Name: "cpu"}}) {
			t.Fatalf("unexpected sources: %s", spew.Sdump(sources))
		} else if condition != nil {
			t.Fatalf("unexpected condition: %s", spew.Sdump(condition))
		}
		return nil
	}

	res := e.MustExecuteQueryString("db0", `drop series from cpu`)
	if s := MustMarshalJSON(res); s != `[{}]` {
		t.Fatalf("unexpected results: %s", s)
	}
}

// Ensure the query executor can execute a DROP MEASUREMENT statement.
func TestQueryExecutor_ExecuteQuery_DropMeasurement(t *testing.T) {
	e := NewQueryExecutor()
	e.Store.DeleteMeasurementFn = func(database, name string) error {
		if database != `db0` {
			t.Fatalf("unexpected database: %s", database)
		} else if name != `memory` {
			t.Fatalf("unexpected name: %s", name)
		}
		return nil
	}

	res := e.MustExecuteQueryString("db0", `drop measurement memory`)
	if s := MustMarshalJSON(res); s != `[{}]` {
		t.Fatalf("unexpected results: %s", s)
	}
}

// Ensure the query executor can execute a DROP DATABASE statement.
//
// Dropping a database involves executing against the meta store as well as
// removing all associated shards from the local TSDB storage.
func TestQueryExecutor_ExecuteQuery_DropDatabase(t *testing.T) {
	e := NewQueryExecutor()
	e.MetaStore.DatabaseFn = func(name string) (*meta.DatabaseInfo, error) {
		return &meta.DatabaseInfo{
			Name: name,
			DefaultRetentionPolicy: "rp0",
			RetentionPolicies: []meta.RetentionPolicyInfo{
				{
					Name: "rp0",
					ShardGroups: []meta.ShardGroupInfo{
						{
							ID:     1,
							Shards: []meta.ShardInfo{{ID: 10}, {ID: 20}},
						},
						{
							ID:     2,
							Shards: []meta.ShardInfo{{ID: 50}},
						},
					},
				},
				{
					Name: "rp1",
					ShardGroups: []meta.ShardGroupInfo{
						{
							ID:     3,
							Shards: []meta.ShardInfo{{ID: 60}},
						},
					},
				},
			},
		}, nil
	}
	e.MetaStatementExecutor.ExecuteStatementFn = func(stmt influxql.Statement) *influxql.Result {
		if s := stmt.String(); s != `DROP DATABASE db0` {
			t.Fatalf("unexpected meta statement: %s", s)
		}
		return &influxql.Result{}
	}

	e.Store.DeleteDatabaseFn = func(name string, shardIDs []uint64) error {
		if name != `db0` {
			t.Fatalf("unexpected name: %s", name)
		} else if !reflect.DeepEqual(shardIDs, []uint64{10, 20, 50, 60}) {
			t.Fatalf("unexpected shard ids: %+v", shardIDs)
		}
		return nil
	}

	res := e.MustExecuteQueryString("db0", `drop database db0`)
	if s := MustMarshalJSON(res); s != `[{}]` {
		t.Fatalf("unexpected results: %s", s)
	}
}

// Ensure that the query executor doesn't return an error when user count is zero
// and the user is attempting to create a user.
func TestQueryExecutor_Authorize_CreateUser_NoUsers(t *testing.T) {
	/*
		store, executor := testStoreAndExecutor("")
		defer os.RemoveAll(store.Path())
		ms := &testMetastore{userCount: 0}
		executor.MetaStore = ms

		if err := executor.Authorize(nil, MustParseQuery("create user foo with password 'asdf' with all privileges"), ""); err != nil {
			t.Fatalf("should have authenticated if no users and attempting to create a user but got error: %s", err.Error())
		}

		if executor.Authorize(nil, MustParseQuery("create user foo with password 'asdf'"), "") == nil {
			t.Fatalf("should have failed authentication if no user given and no users exist for create user query that doesn't grant all privileges")
		}

		if executor.Authorize(nil, MustParseQuery("select * from foo"), "") == nil {
			t.Fatalf("should have failed authentication if no user given and no users exist for any query other than create user")
		}

		ms.userCount = 1

		if executor.Authorize(nil, MustParseQuery("create user foo with password 'asdf'"), "") == nil {
			t.Fatalf("should have failed authentication if no user given and users exist")
		}

		if executor.Authorize(nil, MustParseQuery("select * from foo"), "") == nil {
			t.Fatalf("should have failed authentication if no user given and users exist")
		}
	*/
}

// QueryExecutor represents a test wrapper for tsdb.QueryExecutor.
type QueryExecutor struct {
	*tsdb.QueryExecutor

	Store                    QueryExecutorStore
	MetaStore                QueryExecutorMetaStore
	MetaStatementExecutor    StatementExecutor
	MonitorStatementExecutor StatementExecutor
	IntoWriter               IntoWriter
}

// NewQueryExecutor returns a new instance of QueryExecutor.
func NewQueryExecutor() *QueryExecutor {
	e := &QueryExecutor{}
	e.QueryExecutor = tsdb.NewQueryExecutor()
	e.QueryExecutor.Store = &e.Store
	e.QueryExecutor.MetaStore = &e.MetaStore
	e.QueryExecutor.MetaStatementExecutor = &e.MetaStatementExecutor
	e.QueryExecutor.MonitorStatementExecutor = &e.MonitorStatementExecutor
	e.QueryExecutor.IntoWriter = &e.IntoWriter

	// By default, always return a database when looking it up.
	e.MetaStore.DatabaseFn = MetaStoreDatabaseFoundFn

	// By default, returns the same sources when expanding.
	e.Store.ExpandSourcesFn = DefaultStoreExpandSourcesFn

	return e
}

// NewQueryExecutorStore returns a new instance of QueryExecutor attached to a store.
func NewQueryExecutorStore(s *Store) *QueryExecutor {
	e := NewQueryExecutor()
	e.QueryExecutor.Store = s

	// Always return all shards from store.
	e.MetaStore.ShardIDsByTimeRangeFn = func(sources influxql.Sources, tmin, tmax time.Time) ([]uint64, error) {
		return s.ShardIDs(), nil
	}

	return e
}

// MustExecuteQuery executes a query. Panic on error.
func (e *QueryExecutor) MustExecuteQueryString(database string, s string) []*influxql.Result {
	q := MustParseQuery(s)

	// Execute query.
	ch, err := e.ExecuteQuery(q, database, 1000, make(chan struct{}))
	if err != nil {
		panic(err)
	}

	// Read all results from the channel.
	var a []*influxql.Result
	for {
		select {
		case result, ok := <-ch:
			if !ok {
				return a
			}
			a = append(a, result)
		case <-time.After(10 * time.Second):
			panic("query timeout")
		}
	}
}

// MustExecuteQueryStringJSON executes a query and returns JSON. Panic on error.
func (e *QueryExecutor) MustExecuteQueryStringJSON(database string, s string) string {
	return MustMarshalJSON(e.MustExecuteQueryString(database, s))
}

// QueryExecutorStore is a mockable implementation of QueryExecutor.Store.
type QueryExecutorStore struct {
	ShardsFn            func(ids []uint64) []*tsdb.Shard
	ExpandSourcesFn     func(sources influxql.Sources) (influxql.Sources, error)
	DeleteDatabaseFn    func(name string, shardIDs []uint64) error
	DeleteMeasurementFn func(database, name string) error
	DeleteSeriesFn      func(database string, sources influxql.Sources, condition influxql.Expr) error
}

func (s *QueryExecutorStore) Shards(ids []uint64) []*tsdb.Shard {
	return s.ShardsFn(ids)
}
func (s *QueryExecutorStore) ExpandSources(sources influxql.Sources) (influxql.Sources, error) {
	return s.ExpandSourcesFn(sources)
}
func (s *QueryExecutorStore) DeleteDatabase(name string, shardIDs []uint64) error {
	return s.DeleteDatabaseFn(name, shardIDs)
}
func (s *QueryExecutorStore) DeleteMeasurement(database, name string) error {
	return s.DeleteMeasurementFn(database, name)
}
func (s *QueryExecutorStore) DeleteSeries(database string, sources influxql.Sources, condition influxql.Expr) error {
	return s.DeleteSeriesFn(database, sources, condition)
}

// DefaultStoreExpandSourcesFn returns the original sources unchanged.
func DefaultStoreExpandSourcesFn(sources influxql.Sources) (influxql.Sources, error) {
	return sources, nil
}

// QueryExecutorMetaStore is a mockable implementation of QueryExecutor.MetaStore.
type QueryExecutorMetaStore struct {
	DatabaseFn            func(name string) (*meta.DatabaseInfo, error)
	DatabasesFn           func() ([]meta.DatabaseInfo, error)
	UserFn                func(name string) (*meta.UserInfo, error)
	AdminUserExistsFn     func() (bool, error)
	AuthenticateFn        func(username, password string) (*meta.UserInfo, error)
	RetentionPolicyFn     func(database, name string) (rpi *meta.RetentionPolicyInfo, err error)
	UserCountFn           func() (int, error)
	ShardIDsByTimeRangeFn func(sources influxql.Sources, tmin, tmax time.Time) (a []uint64, err error)
	NodeIDFn              func() uint64
}

func (s *QueryExecutorMetaStore) Database(name string) (*meta.DatabaseInfo, error) {
	return s.DatabaseFn(name)
}
func (s *QueryExecutorMetaStore) Databases() ([]meta.DatabaseInfo, error) {
	return s.DatabasesFn()
}
func (s *QueryExecutorMetaStore) User(name string) (*meta.UserInfo, error) {
	return s.UserFn(name)
}
func (s *QueryExecutorMetaStore) AdminUserExists() (bool, error) {
	return s.AdminUserExistsFn()
}
func (s *QueryExecutorMetaStore) Authenticate(username, password string) (*meta.UserInfo, error) {
	return s.AuthenticateFn(username, password)
}
func (s *QueryExecutorMetaStore) RetentionPolicy(database, name string) (rpi *meta.RetentionPolicyInfo, err error) {
	return s.RetentionPolicyFn(database, name)
}
func (s *QueryExecutorMetaStore) UserCount() (int, error) {
	return s.UserCountFn()
}
func (s *QueryExecutorMetaStore) ShardIDsByTimeRange(sources influxql.Sources, tmin, tmax time.Time) (a []uint64, err error) {
	return s.ShardIDsByTimeRangeFn(sources, tmin, tmax)
}
func (s *QueryExecutorMetaStore) NodeID() uint64 {
	return s.NodeIDFn()
}

// MetaStoreDatabaseFoundFn always returns a database for a database name.
func MetaStoreDatabaseFoundFn(name string) (*meta.DatabaseInfo, error) {
	return &meta.DatabaseInfo{
		Name: name,
		DefaultRetentionPolicy: DefaultRetentionPolicy,
	}, nil
}

// StatementExecutor is a mockable implementation of QueryExecutor.StatementExecutor.
type StatementExecutor struct {
	ExecuteStatementFn func(stmt influxql.Statement) *influxql.Result
}

func (e *StatementExecutor) ExecuteStatement(stmt influxql.Statement) *influxql.Result {
	return e.ExecuteStatementFn(stmt)
}

// IteratorCreator is a mockable implementation of SelectStatementExecutor.IteratorCreator.
type IteratorCreator struct {
	CreateIteratorFn  func(opt influxql.IteratorOptions) (influxql.Iterator, error)
	FieldDimensionsFn func(sources influxql.Sources) (field, dimensions map[string]struct{}, err error)
}

func (ic *IteratorCreator) CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	return ic.CreateIteratorFn(opt)
}

func (ic *IteratorCreator) FieldDimensions(sources influxql.Sources) (field, dimensions map[string]struct{}, err error) {
	return ic.FieldDimensionsFn(sources)
}

// IntoWriter is a mockable implementation of QueryExecutor.IntoWriter.
type IntoWriter struct {
	WritePointsIntoFn func(p *tsdb.IntoWriteRequest) error
}

func (w *IntoWriter) WritePointsInto(p *tsdb.IntoWriteRequest) error {
	return w.WritePointsIntoFn(p)
}

// FloatIterator is a test implementation of influxql.FloatIterator.
type FloatIterator struct {
	Points []influxql.FloatPoint
}

// Close is a no-op.
func (itr *FloatIterator) Close() error { return nil }

// Next returns the next value and shifts it off the beginning of the points slice.
func (itr *FloatIterator) Next() *influxql.FloatPoint {
	if len(itr.Points) == 0 {
		return nil
	}

	v := &itr.Points[0]
	itr.Points = itr.Points[1:]
	return v
}

// MustParseQuery parses an InfluxQL query. Panic on error.
func MustParseQuery(s string) *influxql.Query {
	q, err := influxql.NewParser(strings.NewReader(s)).ParseQuery()
	if err != nil {
		panic(err.Error())
	}
	return q
}

// MustMarshalJSON marshals a value to a JSON string. Panic on error.
func MustMarshalJSON(v interface{}) string {
	buf, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(buf)
}
