package fill

import (
	"exporter/internal/database"
	// "github.com/jmoiron/sqlx"
	// "github.com/icrowley/fake"
)

const (
	create = `create table people (
		id serial not null primary key,
		name varchar(256) not null,
		lastname varchar(256) not null,
		birthday_date not null,
		some_flag integer not null,
		created timestamp(0) not null default current_timestamp
	  ) ;

	  create table cities (
		id serial not null primary key,
		name varchar(256) not null,
		country_id integer not null,
		created timestamp(0) not null default current_timestamp
	  ) ;

	  create table countries (
		id serial not null primary key,
		name varchar(256) not null,
		created timestamp(0) not null default current_timestamp
	  ) ;`

	insertPeople    = `insert into people(name, lastname, birthday_date, some_flag) values($1, $2, $3, $4)`
	insertCities    = `insert into cities(name, country_id) values($1, $2)`
	insertCountries = `insert into countries(name) values($1)`
)

func Populate(DB *database.DB) {
	psInsertPeople, _ := DB.Database.Preparex(insertPeople)
	psInsertCities, _ := DB.Database.Preparex(insertCities)
	psInsertCountries, _ := DB.Database.Preparex(insertCountries)

	DB.Database.MustExec(create)

	for i := 0; i < 5000; i++ {
		psInsertPeople.MustExec(insertPeople)
		psInsertCities.MustExec(insertCities)
		psInsertCountries.MustExec(insertCountries)
	}

}
