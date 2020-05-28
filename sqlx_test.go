package migrate_test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/joncalhoun/migrate"
	_ "github.com/mattn/go-sqlite3"
)

func sqliteInMem(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name()))
	if err != nil {
		t.Fatalf("Open() err = %v; want nil", err)
	}
	t.Cleanup(func() {
		err = db.Close()
		if err != nil {
			t.Errorf("Close() err = %v; want nil", err)
		}
	})
	return db
}

// TODO: Add more exhaustive testing. Perhaps try different dialects? Good enough for now tho.
func TestSqlx(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		db := sqliteInMem(t)
		migrator := migrate.Sqlx{
			Printf: func(format string, args ...interface{}) (int, error) {
				t.Logf(format, args...)
				return 0, nil
			},
			Migrations: []migrate.SqlxMigration{
				migrate.SqlxQueryMigration("001_create_courses", createCoursesSql),
			},
		}
		err := migrator.Migrate(db, "sqlite3")
		if err != nil {
			t.Fatalf("Migrate() err = %v; want nil", err)
		}
		_, err = db.Exec("INSERT INTO courses (name) VALUES ($1) ", "cor_test")
		if err != nil {
			t.Fatalf("db.Exec() err = %v; want nil", err)
		}
	})

	t.Run("existing migrations", func(t *testing.T) {
		db := sqliteInMem(t)
		migrator := migrate.Sqlx{
			Printf: func(format string, args ...interface{}) (int, error) {
				t.Logf(format, args...)
				return 0, nil
			},
			Migrations: []migrate.SqlxMigration{
				migrate.SqlxQueryMigration("001_create_courses", createCoursesSql),
			},
		}
		err := migrator.Migrate(db, "sqlite3")
		if err != nil {
			t.Fatalf("Migrate() err = %v; want nil", err)
		}
		_, err = db.Exec("INSERT INTO courses (name) VALUES ($1) ", "cor_test")
		if err != nil {
			t.Fatalf("db.Exec() err = %v; want nil", err)
		}

		// the real test
		migrator = migrate.Sqlx{
			Printf: func(format string, args ...interface{}) (int, error) {
				t.Logf(format, args...)
				return 0, nil
			},
			Migrations: []migrate.SqlxMigration{
				migrate.SqlxQueryMigration("001_create_courses", createCoursesSql),
				migrate.SqlxQueryMigration("002_create_users", createUsersSql),
			},
		}
		err = migrator.Migrate(db, "sqlite3")
		if err != nil {
			t.Fatalf("Migrate() err = %v; want nil", err)
		}
		_, err = db.Exec("INSERT INTO users (email) VALUES ($1) ", "abc@test.com")
		if err != nil {
			t.Fatalf("db.Exec() err = %v; want nil", err)
		}
	})

	t.Run("file", func(t *testing.T) {
		db := sqliteInMem(t)
		migrator := migrate.Sqlx{
			Printf: func(format string, args ...interface{}) (int, error) {
				t.Logf(format, args...)
				return 0, nil
			},
			Migrations: []migrate.SqlxMigration{
				migrate.SqlxFileMigration("001_create_widgets", "testdata/widgets.sql"),
			},
		}
		err := migrator.Migrate(db, "sqlite3")
		if err != nil {
			t.Fatalf("Migrate() err = %v; want nil", err)
		}
		_, err = db.Exec("INSERT INTO widgets (color, price) VALUES ($1, $2)", "red", 1200)
		if err != nil {
			t.Fatalf("db.Exec() err = %v; want nil", err)
		}
	})
}

var createCoursesSql = `
CREATE TABLE courses (
  id serial PRIMARY KEY,
  name text
);`

var createUsersSql = `
CREATE TABLE users (
  id serial PRIMARY KEY,
  email text UNIQUE NOT NULL
);`
