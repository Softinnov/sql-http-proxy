package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
)

var (
	flagDriver = flag.String("driver", "mysql", "driver to use")

	databases = map[string]*sql.DB{}
)

type QueryResult struct {
	Columns []string         `json:"columns,omitempty"`
	Data    [][]*string      `json:"data,omitempty"`
	Infos   map[string]int64 `json:"infos,omitempty"`
	Error   string           `json:"error,omitempty"`
}

func WriteToJSON(w http.ResponseWriter, s int, v interface{}) error {
	d, e := json.Marshal(v)
	if e != nil {
		return e
	}
	log.Printf("%s", d)
	w.Header().Set("content-type", "application/json; charset=utf-8")
	w.WriteHeader(s)
	_, e = w.Write(d)
	if e != nil {
		return e
	}
	return nil
}

func open(dbn string) (*sql.DB, int, error) {

	for k, v := range databases {
		if k == dbn {
			log.Printf("Used cache for database %q", dbn)
			return v, http.StatusOK, nil
		}
	}

	db, e := sql.Open(*flagDriver, "root:@/"+dbn)
	if e != nil {
		return nil, http.StatusInternalServerError, e
	}
	e = db.Ping()
	if e != nil {
		return nil, http.StatusBadRequest, e
	}
	databases[dbn] = db
	log.Printf("Stored database %q", dbn)

	return db, http.StatusOK, nil
}

func HandlePing(w http.ResponseWriter, r *http.Request) {
	n := time.Now()

	dbn := mux.Vars(r)["db"]

	log.Printf("[DATABASE] %q\n", dbn)

	db, s, e := open(dbn)
	if e != nil {
		log.Println(e)
		w.WriteHeader(s)
		fmt.Fprintln(w, e)
		return
	}
	e = db.Ping()
	if e != nil {
		log.Println(e)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintln(w, e)
		return
	}

	fmt.Fprintf(w, "ping achieved")
	log.Printf("(Ping done in %v)\n", time.Now().Sub(n))
}

func main() {
	flag.Parse()

	databases = make(map[string]*sql.DB)

	m := mux.NewRouter()
	m.HandleFunc("/query/{db}/{query}", HandleQuery).Methods("POST")
	m.HandleFunc("/exec/{db}/{query}", HandleExec).Methods("POST")
	m.HandleFunc("/ping/{db}", HandlePing).Methods("GET")

	log.Fatal(http.ListenAndServe(":6033", m))
}
