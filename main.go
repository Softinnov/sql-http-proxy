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

func HandlePing(w http.ResponseWriter, r *http.Request) {
	n := time.Now()

	dbn := mux.Vars(r)["db"]

	log.Printf("[DATABASE] %q\n", dbn)

	db, e := sql.Open(*flagDriver, "root:@/"+dbn)
	if e != nil {
		log.Println(e)
		fmt.Fprintln(w, e)
		return
	}
	e = db.Ping()
	if e != nil {
		log.Println(e)
		fmt.Fprintln(w, e)
		return
	}

	fmt.Fprintf(w, "ping achieved")
	log.Printf("(Ping done in %v)\n", time.Now().Sub(n))
}

func HandleQuery(w http.ResponseWriter, r *http.Request) {
	qr := &QueryResult{}

	n := time.Now()

	dbn := mux.Vars(r)["db"]
	query := mux.Vars(r)["query"]

	log.Printf("[DATABASE] %q [QUERY] %q\n", dbn, query)

	s := qr.fetchQuery(dbn, query)
	e := WriteToJSON(w, s, qr)
	if e != nil {
		log.Println(e)
	}
	log.Printf("(Query rendered in %v)\n", time.Now().Sub(n))
}

func HandleExec(w http.ResponseWriter, r *http.Request) {
	qr := &QueryResult{}

	n := time.Now()

	dbn := mux.Vars(r)["db"]
	query := mux.Vars(r)["query"]

	log.Printf("[DATABASE] %q [QUERY] %q\n", dbn, query)

	s := qr.fetchExec(dbn, query)
	e := WriteToJSON(w, s, qr)
	if e != nil {
		log.Println(e)
	}
	log.Printf("(Exec rendered in %v)\n", time.Now().Sub(n))
}

func (qr *QueryResult) fetchExec(dbn string, query string) int {
	db, e := sql.Open(*flagDriver, "root:@/"+dbn)
	if e != nil {
		log.Fatal(e)
	}
	e = db.Ping()
	if e != nil {
		log.Fatal(e)
	}
	rs, e := db.Exec(query)
	if e != nil {
		log.Println(e)
		qr.Error = fmt.Sprintf("%s", e)
		return http.StatusBadRequest
	}

	li, e := rs.LastInsertId()
	if e != nil {
		log.Println(e)
		qr.Error = fmt.Sprintf("%s", e)
		return http.StatusBadRequest
	}
	ra, e := rs.RowsAffected()
	if e != nil {
		log.Println(e)
		qr.Error = fmt.Sprintf("%s", e)
		return http.StatusBadRequest
	}

	qr.Infos = map[string]int64{
		"lastInsertId": li,
		"rowsAffected": ra,
	}
	return http.StatusOK
}

func (qr *QueryResult) fetchQuery(dbn string, query string) int {
	db, e := sql.Open(*flagDriver, "root:@/"+dbn)
	if e != nil {
		log.Fatal(e)
	}
	e = db.Ping()
	if e != nil {
		log.Fatal(e)
	}
	rs, e := db.Query(query)
	if e != nil {
		log.Println(e)
		qr.Error = fmt.Sprintf("%s", e)
		return http.StatusBadRequest
	}
	defer rs.Close()
	cs, e := rs.Columns()
	if e != nil {
		log.Println(e)
		qr.Error = fmt.Sprintf("%s", e)
		return http.StatusInternalServerError
	}
	var res [][]*string
	tmpr := make([][]byte, len(cs))

	tmpi := make([]interface{}, len(cs))
	for i, _ := range tmpr {
		tmpi[i] = &tmpr[i]
	}
	for rs.Next() {
		raw := make([]*string, len(cs))
		e = rs.Scan(tmpi...)
		if e != nil {
			log.Println(e)
			qr.Error = fmt.Sprintf("%s", e)
			return http.StatusInternalServerError
		}

		for i, v := range tmpr {
			switch v {
			case nil:
				raw[i] = nil
			default:
				t := string(tmpr[i])
				raw[i] = &t
			}
		}

		res = append(res, raw)
	}

	qr.Data = res
	qr.Columns = cs
	return http.StatusOK
}

func main() {
	flag.Parse()

	m := mux.NewRouter()
	m.HandleFunc("/query/{db}/{query}", HandleQuery).Methods("POST")
	m.HandleFunc("/exec/{db}/{query}", HandleExec).Methods("POST")
	m.HandleFunc("/ping/{db}", HandlePing).Methods("GET")

	log.Fatal(http.ListenAndServe(":6033", m))
}
