package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

func main() {
	log.Println("starting program")
	// get the database connection URL.
	// usually, this is taken as an environment variable as in below commented out code
	// databaseUrl = os.Getenv("DATABASE_URL")
	// for the time being, let's hard code it as follows. change the values as needed.
	databaseUrl := "postgres://postgres:mypassword@localhost:5432/postgres"
	dbPool, err := pgxpool.Connect(context.Background(), databaseUrl)

	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	//to close DB pool
	defer dbPool.Close()

	ExecuteSelectQuery(dbPool)
	ExecuteFunction(dbPool)
	log.Println("stopping program")
}

func ExecuteSelectQuery(dbPool *pgxpool.Pool) {
	log.Println("starting execution of select query")
	//execute the query and get result rows
	rows, err := dbPool.Query(context.Background(), "select * from public.person")
	if err != nil {
		log.Fatal("error while executing query")
	}

	log.Println("result:")
	//iterate through the rows
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			log.Fatal("error while iterating dataset")
		}
		//convert DB types to Go types
		id := values[0].(int32)
		firstName := values[1].(string)
		lastName := values[2].(string)
		dateOfBirth := values[3].(time.Time)
		log.Println("[id:", id, ", first_name:", firstName, ", last_name:", lastName, ", date_of_birth:", dateOfBirth, "]")
	}

}

func ExecuteFunction(dbPool *pgxpool.Pool) {
	log.Println("starting execution of databse function")
	// id can be taken as a user input
	// for the time being, let's hard code it
	id := 1

	//execute the query and get result rows
	rows, err := dbPool.Query(context.Background(), "select * from public.get_person_details($1)", id)
	log.Println("input id: ", id)
	if err != nil {
		log.Fatal("error while executing query")
	}

	log.Println("result:")
	//iterate through the rows
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			log.Fatal("error while iterating dataset")
		}

		//convert DB types to Go types
		firstName := values[0].(string)
		lastName := values[1].(string)
		dateOfBirth := values[2].(time.Time)

		log.Println("[first_name:", firstName, ", last_name:", lastName, ", date_of_birth:", dateOfBirth, "]")
	}

}
