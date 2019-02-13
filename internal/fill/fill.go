package fill

import (
	"exporter/internal/database"
	"math/rand"
	"strconv"

	"github.com/icrowley/fake"
)

const (
	create = `
		drop table if exists people;
		drop table if exists cities;
		drop table if exists countries;

		create table if not exists people (
		id serial not null primary key,
		name varchar(256) not null,
		lastname varchar(256) not null,
		birthday date not null,
		some_flag integer not null,
		created timestamp(0) not null default current_timestamp
	  ) ;

	  create table if not exists cities (
		id serial not null primary key,
		name varchar(256) not null,
		country_id integer not null,
		created timestamp(0) not null default current_timestamp
	  ) ;

	  create table  if not exists countries (
		id serial not null primary key,
		name varchar(256) not null,
		created timestamp(0) not null default current_timestamp
	  ) ;`

	insertPeople    = `insert into people(name, lastname, birthday, some_flag) values($1, $2, $3, $4)`
	insertCities    = `insert into cities(name, country_id) values($1, $2)`
	insertCountries = `insert into countries(name) values($1)`
)

func Populate(DB *database.DB) {
	DB.Database.MustExec(create)

	psInsertPeople, _ := DB.Database.Preparex(insertPeople)
	psInsertCities, _ := DB.Database.Preparex(insertCities)
	psInsertCountries, _ := DB.Database.Preparex(insertCountries)

	var date string

	for i := 0; i < 10; i++ {
		// february
		if month := strconv.Itoa(rand.Intn(11) + 1); month == "2" {
			date = strconv.Itoa(rand.Intn(119)+1900) + "-" + month + "-" + strconv.Itoa(rand.Intn(27)+1)
		} else {
			date = strconv.Itoa(rand.Intn(119)+1900) + "-" + month + "-" + strconv.Itoa(rand.Intn(30)+1)
		}

		psInsertPeople.Exec(fake.FirstName(), fake.LastName(), date, rand.Intn(50))
		psInsertCities.Exec(fake.City(), rand.Int31n(400))
		psInsertCountries.Exec(fake.Country())
	}
}
