package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

func HandleExec(w http.ResponseWriter, r *http.Request, info *DbInfo) {
	qr := &QueryResult{}

	n := time.Now()

	dbn := mux.Vars(r)["db"]
	query := mux.Vars(r)["query"]

	log.Printf("[DATABASE] %q [QUERY] %q\n", dbn, query)

	db, s, e := open(dbn, info)
	if e != nil {
		log.Println(e)
		w.WriteHeader(s)
		fmt.Fprintln(w, e)
		return
	}
	s = qr.fetchExec(db, query)
	e = WriteToJSON(w, s, qr)
	if e != nil {
		log.Println(e)
	}
	log.Printf("(Exec rendered in %v)\n", time.Now().Sub(n))
}

func (qr *QueryResult) fetchExec(db *sql.DB, query string) int {
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
