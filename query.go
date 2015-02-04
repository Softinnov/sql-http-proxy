package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

func HandleQuery(w http.ResponseWriter, r *http.Request) {
	qr := &QueryResult{}

	n := time.Now()

	dbn := mux.Vars(r)["db"]
	query := mux.Vars(r)["query"]

	log.Printf("[DATABASE] %q [QUERY] %q\n", dbn, query)

	db, s, e := open(dbn)
	if e != nil {
		log.Println(e)
		w.WriteHeader(s)
		fmt.Fprintln(w, e)
		return
	}

	s = qr.fetchQuery(db, query)
	e = WriteToJSON(w, s, qr)
	if e != nil {
		log.Println(e)
	}
	log.Printf("(Query rendered in %v)\n", time.Now().Sub(n))
}

func (qr *QueryResult) fetchQuery(db *sql.DB, query string) int {
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
