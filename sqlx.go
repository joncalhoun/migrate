package migrate

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/jmoiron/sqlx"
)

// Sqlx is a migrator that uses github.com/jmoiron/sqlx
type Sqlx struct {
	Migrations []SqlxMigration
	// Printf is used to print out additional information during a migration, such
	// as which step the migration is currently on. It can be replaced with any
	// custom printf function, including one that just ignores inputs. If nil it
	// will default to fmt.Printf.
	Printf func(format string, a ...interface{}) (n int, err error)
}

// Migrate will run the migrations using the provided db connection.
func (s *Sqlx) Migrate(sqlDB *sql.DB, dialect string) error {
	db := sqlx.NewDb(sqlDB, dialect)

	s.printf("Creating/checking migrations table...\n")
	err := s.createMigrationTable(db)
	if err != nil {
		return err
	}
	for _, m := range s.Migrations {
		var found string
		err := db.Get(&found, "SELECT id FROM migrations WHERE id=$1", m.ID)
		switch err {
		case sql.ErrNoRows:
			s.printf("Running migration: %v\n", m.ID)
			// we need to run the migration so we continue to code below
		case nil:
			s.printf("Skipping migration: %v\n", m.ID)
			continue
		default:
			return fmt.Errorf("looking up migration by id: %w", err)
		}
		err = s.runMigration(db, m)
		if err != nil {
			return err
		}
	}
	return nil
}

// Rollback will run all rollbacks using the provided db connection.
func (s *Sqlx) Rollback(sqlDB *sql.DB, dialect string) error {
	db := sqlx.NewDb(sqlDB, dialect)

	s.printf("Creating/checking migrations table...\n")
	err := s.createMigrationTable(db)
	if err != nil {
		return err
	}
	for i := len(s.Migrations) - 1; i >= 0; i-- {
		m := s.Migrations[i]
		if m.Rollback == nil {
			s.printf("Rollback not provided: %v\n", m.ID)
			continue
		}
		var found string
		err := db.Get(&found, "SELECT id FROM migrations WHERE id=$1", m.ID)
		switch err {
		case sql.ErrNoRows:
			s.printf("Skipping rollback: %v\n", m.ID)
			continue
		case nil:
			s.printf("Running rollback: %v\n", m.ID)
			// we need to run the rollback so we continue to code below
		default:
			return fmt.Errorf("looking up rollback by id: %w", err)
		}
		err = s.runRollback(db, m)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Sqlx) printf(format string, a ...interface{}) (n int, err error) {
	printf := s.Printf
	if printf == nil {
		printf = fmt.Printf
	}
	return printf(format, a...)
}

func (s *Sqlx) createMigrationTable(db *sqlx.DB) error {
	_, err := db.Exec("CREATE TABLE IF NOT EXISTS migrations (id TEXT PRIMARY KEY )")
	if err != nil {
		return fmt.Errorf("creating migrations table: %w", err)
	}
	return nil
}

func (s *Sqlx) runMigration(db *sqlx.DB, m SqlxMigration) error {
	errorf := func(err error) error { return fmt.Errorf("running migration: %w", err) }

	tx, err := db.Beginx()
	if err != nil {
		return errorf(err)
	}
	_, err = tx.Exec("INSERT INTO migrations (id) VALUES ($1)", m.ID)
	if err != nil {
		tx.Rollback()
		return errorf(err)
	}
	err = m.Migrate(tx)
	if err != nil {
		tx.Rollback()
		return errorf(err)
	}
	err = tx.Commit()
	if err != nil {
		return errorf(err)
	}
	return nil
}

func (s *Sqlx) runRollback(db *sqlx.DB, m SqlxMigration) error {
	errorf := func(err error) error { return fmt.Errorf("running rollback: %w", err) }

	tx, err := db.Beginx()
	if err != nil {
		return errorf(err)
	}
	_, err = tx.Exec("DELETE FROM migrations WHERE id=$1", m.ID)
	if err != nil {
		tx.Rollback()
		return errorf(err)
	}
	err = m.Rollback(tx)
	if err != nil {
		tx.Rollback()
		return errorf(err)
	}
	err = tx.Commit()
	if err != nil {
		return errorf(err)
	}
	return nil
}

// SqlxMigration is a unique ID plus a function that uses a sqlx transaction
// to perform a database migration step.
//
// Note: Long term this could have a Rollback field if we wanted to support
// that.
type SqlxMigration struct {
	ID       string
	Migrate  func(tx *sqlx.Tx) error
	Rollback func(tx *sqlx.Tx) error
}

// SqlxQueryMigration will create a SqlxMigration using the provided id and
// query string. It is a helper function designed to simplify the process of
// creating migrations that only depending on a SQL query string.
func SqlxQueryMigration(id, upQuery, downQuery string) SqlxMigration {
	queryFn := func(query string) func(tx *sqlx.Tx) error {
		if query == "" {
			return nil
		}
		return func(tx *sqlx.Tx) error {
			_, err := tx.Exec(query)
			return err
		}
	}

	m := SqlxMigration{
		ID:       id,
		Migrate:  queryFn(upQuery),
		Rollback: queryFn(downQuery),
	}
	return m
}

// SqlxFileMigration will create a SqlxMigration using the provided file.
func SqlxFileMigration(id, upFile, downFile string) SqlxMigration {
	fileFn := func(filename string) func(tx *sqlx.Tx) error {
		if filename == "" {
			return nil
		}
		f, err := os.Open(filename)
		if err != nil {
			// We could return a migration that errors when the migration is run, but I
			// think it makes more sense to panic here.
			panic(err)
		}
		fileBytes, err := ioutil.ReadAll(f)
		if err != nil {
			panic(err)
		}
		return func(tx *sqlx.Tx) error {
			_, err := tx.Exec(string(fileBytes))
			return err
		}
	}

	m := SqlxMigration{
		ID:       id,
		Migrate:  fileFn(upFile),
		Rollback: fileFn(downFile),
	}
	return m
}
