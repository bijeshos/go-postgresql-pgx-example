package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"log"
	"os"
	"time"
)

func main() {
	log.Println("starting program")
	//get the database connection URL. usually, this is taken a value for environment variable
	databaseUrl := "postgres://postgres:postgres@123@localhost:5432/postgres"
	dbPool, err := pgxpool.Connect(context.Background(), databaseUrl)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	//to close DB pool
	defer dbPool.Close()

	ExecuteSelectQuery(dbPool)
	ExecuteFunction(dbPool)
	log.Println("stopping program")
}

func ExecuteSelectQuery(dbPool *pgxpool.Pool) bool {
	log.Println("starting execution of select query")
	//execute the query and get result rows
	rows, err := dbPool.Query(context.Background(), "select * from public.person")
	if err != nil {
		return true
	}

	//iterate through the rows
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return true
		}
		//convert DB types to Go types
		id := values[0].(int32)
		firstName := values[1].(string)
		lastName := values[2].(string)
		dateOfBirth := values[3].(time.Time)
		log.Println("[id:", id, ", first_name:", firstName, ", last_name:", lastName, ", date_of_birth:", dateOfBirth, "]")
	}
	return false
}

func ExecuteFunction(dbPool *pgxpool.Pool) bool {
	log.Println("starting execution of DB function")
	//execute the query and get result rows
	id := 1
	rows, err := dbPool.Query(context.Background(), "select * from public.get_person_details($1)", id)
	log.Println("id: ", id)
	if err != nil {
		return true
	}

	//iterate through the rows
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return true
		}
		//convert DB types to Go types
		firstName := values[0].(string)
		lastName := values[1].(string)
		dateOfBirth := values[2].(time.Time)
		log.Println("[first_name:", firstName, ", last_name:", lastName, ", date_of_birth:", dateOfBirth, "]")
	}
	return false
}
