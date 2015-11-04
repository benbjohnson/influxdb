package run_test

import (
	"net/url"
	"testing"
)

// Ensure the database commands work.
func TestServer_DatabaseCommands(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig(), "")
	defer s.Close()

	test := tests.load(t, "database_commands")

	for _, query := range test.queries {
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.Execute(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_Query_DropAndRecreateDatabase(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewConfig(), "")
	defer s.Close()

	s.MustWrite("db0", "rp0", `cpu,host=serverA,region=uswest val=23.2 0`, nil)

	if res := s.MustQuery(`DROP DATABASE db0`); res != `{"results":[{}]}` {
		t.Fatalf("unexpected result: %s", res)
	}
	if res := s.MustQuery(`CREATE DATABASE db0`); res != `{"results":[{}]}` {
		t.Fatalf("unexpected result: %s", res)
	}
	if res := s.MustQuery(`CREATE RETENTION POLICY rp0 ON db0 DURATION 365d REPLICATION 1 DEFAULT`); res != `{"results":[{}]}` {
		t.Fatalf("unexpected result: %s", res)
	}
	if res := s.MustQueryWithParams(`SHOW MEASUREMENTS`, url.Values{"db": []string{"db0"}}); res != `{"results":[{}]}` {
		t.Fatalf("unexpected result: %s", res)
	}
	if res := s.MustQueryWithParams(`SELECT * FROM cpu`, url.Values{"db": []string{"db0"}}); res != `{"results":[{}]}` {
		t.Fatalf("unexpected result: %s", res)
	}
}

func TestServer_Query_DropDatabaseIsolated(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewConfig(), "")
	defer s.Close()

	// Create separate DB.
	if err := s.CreateDatabaseAndRetentionPolicy("db1", newRetentionPolicyInfo("rp1", 1, 0)); err != nil {
		panic(err)
	}

	s.MustWrite("db0", "rp0", `cpu,host=serverA,region=uswest val=23.2 0`, nil)

	// Query data to verify it exists.
	if res := s.MustQueryWithParams(`SELECT * FROM cpu`, url.Values{"db": []string{"db0"}}); res != `{"results":[{"series":[{"name":"cpu","columns":["time","host","region","val"],"values":[["1970-01-01T00:00:00Z","serverA","uswest",23.2]]}]}]}` {
		t.Fatalf("unexpected result: %s", res)
	}
	if res := s.MustQueryWithParams(`SELECT * FROM cpu GROUP BY *`, url.Values{"db": []string{"db0"}}); res != `{"results":[{"series":[{"name":"cpu","tags":{"host":"serverA","region":"uswest"},"columns":["time","val"],"values":[["1970-01-01T00:00:00Z",23.2]]}]}]}` {
		t.Fatalf("unexpected result: %s", res)
	}

	// Drop a different database.
	if res := s.MustQuery(`DROP DATABASE db1`); res != `{"results":[{}]}` {
		t.Fatalf("unexpected result: %s", res)
	}

	// Query data to verify it still exists after deleting a different database.
	if res := s.MustQueryWithParams(`SELECT * FROM cpu`, url.Values{"db": []string{"db0"}}); res != `{"results":[{"series":[{"name":"cpu","columns":["time","host","region","val"],"values":[["1970-01-01T00:00:00Z","serverA","uswest",23.2]]}]}]}` {
		t.Fatalf("unexpected result: %s", res)
	}
	if res := s.MustQueryWithParams(`SELECT * FROM cpu GROUP BY *`, url.Values{"db": []string{"db0"}}); res != `{"results":[{"series":[{"name":"cpu","tags":{"host":"serverA","region":"uswest"},"columns":["time","val"],"values":[["1970-01-01T00:00:00Z",23.2]]}]}]}` {
		t.Fatalf("unexpected result: %s", res)
	}
}
