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
				migrate.SqlxQueryMigration("001_create_courses", createCoursesSql, ""),
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
				migrate.SqlxQueryMigration("001_create_courses", createCoursesSql, ""),
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
				migrate.SqlxQueryMigration("001_create_courses", createCoursesSql, ""),
				migrate.SqlxQueryMigration("002_create_users", createUsersSql, ""),
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
				migrate.SqlxFileMigration("001_create_widgets", "testdata/widgets.sql", ""),
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

	t.Run("rollback", func(t *testing.T) {
		db := sqliteInMem(t)
		migrator := migrate.Sqlx{
			Printf: func(format string, args ...interface{}) (int, error) {
				t.Logf(format, args...)
				return 0, nil
			},
			Migrations: []migrate.SqlxMigration{
				migrate.SqlxQueryMigration("001_create_courses", createCoursesSql, dropCoursesSql),
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
		err = migrator.Rollback(db, "sqlite3")
		if err != nil {
			t.Fatalf("Rollback() err = %v; want nil", err)
		}
		var count int
		err = db.QueryRow("SELECT COUNT(id) FROM courses;").Scan(&count)
		if err == nil {
			// Want an error here
			t.Fatalf("db.QueryRow() err = nil; want table missing error")
		}
		// Don't want to test inner workings of lib, so let's just migrate again and verify we have a table now
		err = migrator.Migrate(db, "sqlite3")
		if err != nil {
			t.Fatalf("Migrate() err = %v; want nil", err)
		}
		_, err = db.Exec("INSERT INTO courses (name) VALUES ($1) ", "cor_test")
		if err != nil {
			t.Fatalf("db.Exec() err = %v; want nil", err)
		}
		err = db.QueryRow("SELECT COUNT(*) FROM courses;").Scan(&count)
		if err != nil {
			// Want an error here
			t.Fatalf("db.QueryRow() err = %v; want nil", err)
		}
		if count != 1 {
			t.Fatalf("count = %d; want %d", count, 1)
		}
	})
}

var (
	createCoursesSql = `
CREATE TABLE courses (
  id serial PRIMARY KEY,
  name text
);`
	dropCoursesSql = `DROP TABLE courses;`

	createUsersSql = `
CREATE TABLE users (
  id serial PRIMARY KEY,
  email text UNIQUE NOT NULL
);`
	dropUsersSql = `DROP TABLE users;`
)
