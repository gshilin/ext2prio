// Read all Completed messages from civicrm driver's database and write them to 4priority service

package main

import (
	"log"
	_ "github.com/jmoiron/sqlx"
	_ "github.com/pkg/errors"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/joho/godotenv/autoload"
	"fmt"
	"encoding/json"
	"net/http"
	"os"
	"bytes"
	"io/ioutil"
	"github.com/jmoiron/sqlx"
	"database/sql"
	"time"
)

// Read messages from database
type Contribution struct {
	ID                string          `db:"ID"`
	ORG               string          `db:"ORG"`
	CID               sql.NullString  `db:"CID"`
	QAMO_PARTNAME     sql.NullString  `db:"QAMO_PARTNAME"`
	QAMO_VAT          sql.NullString  `db:"QAMO_VAT"`
	QAMO_CUSTDES      sql.NullString  `db:"QAMO_CUSTDES"`
	QAMO_DETAILS      int64           `db:"QAMO_DETAILS"`
	QAMO_PARTDES      sql.NullString  `db:"QAMO_PARTDES"`
	QAMO_PAYMENTCODE  sql.NullString  `db:"QAMO_PAYMENTCODE"`
	QAMO_CARDNUM      sql.NullString  `db:"QAMO_CARDNUM"`
	QAMO_PAYMENTCOUNT sql.NullString  `db:"QAMO_PAYMENTCOUNT"`
	QAMO_VALIDMONTH   sql.NullString  `db:"QAMO_VALIDMONTH"`
	QAMO_PAYPRICE     float64         `db:"QAMO_PAYPRICE"`
	QAMO_CURRNCY      sql.NullString  `db:"QAMO_CURRNCY"`
	QAMO_PAYCODE      sql.NullInt64   `db:"QAMO_PAYCODE"`
	QAMO_FIRSTPAY     sql.NullFloat64 `db:"QAMO_FIRSTPAY"`
	QAMO_EMAIL        sql.NullString  `db:"QAMO_EMAIL"`
	QAMO_ADRESS       sql.NullString  `db:"QAMO_ADRESS"`
	QAMO_CITY         sql.NullString  `db:"QAMO_CITY"`
	QAMO_CELL         sql.NullString  `db:"QAMO_CELL"`
	QAMO_FROM         sql.NullString  `db:"QAMO_FROM"`
	QAMM_UDATE        sql.NullString  `db:"QAMM_UDATE"`
	QAMO_LANGUAGE     sql.NullString  `db:"QAMO_LANGUAGE"`
	QAMO_REFERENCE    sql.NullString  `db:"QAMO_REFERENCE"`
	IS_VISUAL         sql.NullString  `db:"IS_VISUAL"`
}

var (
	urlStr string
	err    error
)

func main() {

	host := os.Getenv("CIVI_HOST")
	if host == "" {
		host = "localhost"
	}
	dbName := os.Getenv("CIVI_DBNAME")
	if dbName == "" {
		dbName = "localhost"
	}
	user := os.Getenv("CIVI_USER")
	if user == "" {
		log.Fatalf("Unable to connect without username\n")
		os.Exit(2)
	}
	password := os.Getenv("CIVI_PASSWORD")
	if password == "" {
		log.Fatalf("Unable to connect without password\n")
	}
	protocol := os.Getenv("CIVI_PROTOCOL")
	if protocol == "" {
		log.Fatalf("Unable to connect without protocol\n")
	}
	prioHost := os.Getenv("PRIO_HOST")
	if prioHost == "" {
		log.Fatalf("Unable to connect Priority without host name\n")
	}
	prioPort := os.Getenv("PRIO_PORT")
	if prioPort == "" {
		log.Fatalf("Unable to connect Priority without port number\n")
	}

	db, stmt := OpenDb(host, user, password, protocol, dbName)
	defer closeDb(db)

	urlStr = "http://" + prioHost + ":" + prioPort + "/payment_event"

	ReadMessages(db, stmt)
}

// Connect to DB
func OpenDb(host string, user string, password string, protocol string, dbName string) (db *sqlx.DB, stmt *sql.Stmt) {

	dsn := fmt.Sprintf("%s:%s@%s(%s)/%s", user, password, protocol, host, dbName)
	if db, err = sqlx.Open("mysql", dsn); err != nil {
		log.Fatalf("DB connection error: %v\n", err)
	}
	if err = db.Ping(); err != nil {
		log.Fatalf("DB real connection error: %v\n", err)
	}

	if !isTableExists(db, dbName, "bb_ext_requests") {
		log.Fatalf("Table 'bb_ext_requests' does not exist\n")
	}
	if !isTableExists(db, dbName, "bb_ext_pelecard_responses") {
		log.Fatalf("Table 'bb_ext_pelecard_responses' does not exist\n")
	}
	if !isTableExists(db, dbName, "bb_ext_payment_responses") {
		log.Fatalf("Table 'bb_ext_payment_responses' does not exist\n")
	}

	stmt, err = db.Prepare("UPDATE bb_ext_requests SET pstatus = 'reported' WHERE id = ?")
	if err != nil {
		log.Fatalf("Unable to prepare UPDATE statement: %v\n", err)
	}

	return
}

func closeDb(db *sqlx.DB) {
	db.Close()
}

func isTableExists(db *sqlx.DB, dbName string, tableName string) (exists bool) {
	var name string

	if err = db.QueryRow(
		"SELECT table_name name FROM information_schema.tables WHERE table_schema = '" + dbName +
			"' AND table_name = '" + tableName + "' LIMIT 1").Scan(&name); err != nil {
		return false
	} else {
		return name == tableName
	}
}

func ReadMessages(db *sqlx.DB, markAsDone *sql.Stmt) {
	totalPaymentsRead := 0
	contribution := Contribution{}
	rows, err := db.Queryx(`
SELECT DISTINCT
  req.id ID,
  req.reference QAMO_REFERENCE,
  req.organization ORG,
  req.sku QAMO_PARTNAME,
  CASE req.vat 
	WHEN 'Y' THEN 1
	WHEN 'N' THEN 0
  END QAMO_VAT,
  req.name QAMO_CUSTDES, -- שם לקוח
  1 QAMO_DETAILS, -- participants
  SUBSTRING(req.details, 1, 48) QAMO_PARTDES, -- תאור מוצר
  CASE resp.credit_card_brand
	WHEN 1 THEN 'ISR' -- Isracard
	WHEN 2 THEN 'CAL' -- Visa CAL
	WHEN 3 THEN 'DIN' -- Diners
	WHEN 4 THEN 'AME' -- American Express
	WHEN 6 THEN 'LEU' -- LeumiCard
	ELSE 'ISR'
  END QAMO_PAYMENTCODE, -- קוד אמצעי תשלום
  NULL QAMO_CARDNUM,
  resp.credit_card_number QAMO_PAYMENTCOUNT, -- מס כרטיס/חשבון
  resp.credit_card_exp_date QAMO_VALIDMONTH, -- תוקף
  req.price QAMO_PAYPRICE, -- סכום בפועל
  CASE req.currency
    WHEN 'USD' THEN '$'
    WHEN 'EUR' THEN 'EUR'
    ELSE 'ש"ח'
  END QAMO_CURRNCY, -- קוד מטבע
  resp.total_payments QAMO_PAYCODE, -- קוד תנאי תשלום
  resp.first_payment_total QAMO_FIRSTPAY, -- גובה תשלום ראשון
  req.email QAMO_EMAIL, -- אי מייל
  req.street QAMO_ADRESS, -- כתובת
  req.city QAMO_CITY, -- עיר
  req.phone QAMO_CELL, -- נייד
  req.country QAMO_FROM, -- מקור הגעה (country)
  req.created_at QAMM_UDATE,
  req.language QAMO_LANGUAGE,
  req.is_visual IS_VISUAL
FROM bb_ext_requests req
  INNER JOIN bb_ext_payment_responses resp ON resp.user_key = req.user_key
WHERE
  req.status = 'valid' AND req.pstatus = 'valid'
	`)
	if err != nil {
		log.Fatalf("Unable to select rows: %v\n", err)
	}

	for rows.Next() {
		// Read messages from DB
		err = rows.StructScan(&contribution)
		if err != nil {
			log.Fatalf("Table  access error: %v\n", err)
		}

		// Submit 2 priority
		submit2priority(contribution)

		// Update Reported2prio in case of success
		updateReported2prio(markAsDone, contribution.ID)
		totalPaymentsRead++
	}

	fmt.Printf("Total of %d payments were transferred to Priority\n", totalPaymentsRead)
}

func timeIn(from string, name string) string {
	loc, err := time.LoadLocation(name)
	if err != nil {
		return from;
	}
	t, err := time.Parse("2006-01-02 15:04:05", from)
	if err != nil {
		return from;
	}
	return t.In(loc).Format("2006-01-02 15:04:05")
}

func submit2priority(contribution Contribution) {
	// priority's database structure
	type Priority struct {
		ID           string  `json:"id"`
		UserName     string  `json:"name"`
		Amount       float64 `json:"amount"`
		Currency     string  `json:"currency"`
		Email        string  `json:"email"`
		Phone        string  `json:"phone"`
		Address      string  `json:"address"`
		City         string  `json:"city"`
		Country      string  `json:"country"`
		Description  string  `json:"event"`
		Participants int64   `json:"participants"`
		Income       string  `json:"income"`
		Is46         bool    `json:"is46"`
		Token        string  `json:"token"`
		CardType     string  `json:"cardtype"`
		CardNum      string  `json:"cardnum"`
		CardExp      string  `json:"cardexp"`
		Installments int64   `json:"installments"`
		FirstPay     float64 `json:"firstpay"`
		CreatedAt    string  `json:"created_at"`
		Language     string  `json:"language"`
		Reference    string  `json:"reference"`
		Organization string  `json:"organization"`
		IsVisual     bool    `json:"is_visual"`
	}

	type Message struct {
		Error   bool
		Message string
	}

	priority := Priority{
		ID:           contribution.ID,
		UserName:     contribution.QAMO_CUSTDES.String,
		Participants: contribution.QAMO_DETAILS,
		Income:       contribution.QAMO_PARTNAME.String,
		Description:  contribution.QAMO_PARTDES.String,
		CardType:     contribution.QAMO_PAYMENTCODE.String,
		CardNum:      contribution.QAMO_PAYMENTCOUNT.String,
		CardExp:      contribution.QAMO_VALIDMONTH.String,
		Amount:       contribution.QAMO_PAYPRICE,
		Currency:     contribution.QAMO_CURRNCY.String,
		Installments: contribution.QAMO_PAYCODE.Int64,
		FirstPay:     contribution.QAMO_FIRSTPAY.Float64,
		Token:        contribution.QAMO_CARDNUM.String,
		Is46:         contribution.QAMO_VAT.String == "1",
		Email:        contribution.QAMO_EMAIL.String,
		Address:      contribution.QAMO_ADRESS.String,
		City:         contribution.QAMO_CITY.String,
		Country:      contribution.QAMO_FROM.String,
		Phone:        contribution.QAMO_CELL.String,
		CreatedAt:    contribution.QAMM_UDATE.String,
		Language:     contribution.QAMO_LANGUAGE.String,
		Reference:    contribution.QAMO_REFERENCE.String,
		Organization: contribution.ORG,
		IsVisual:     contribution.IS_VISUAL.String == "1",
	}

	// convert QAMM_UDATE to IST
	priority.CreatedAt = timeIn(priority.CreatedAt, "Asia/Jerusalem")

	marshal, err := json.Marshal(priority)
	if err != nil {
		log.Fatalf("Marshal error: %v\n", err)
	}
	log.Printf("%s\n", marshal)

	req, err := http.NewRequest("POST", urlStr, bytes.NewBuffer(marshal))
	if err != nil {
		log.Fatalf("NewRequest error: %v\n", err)
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("client.Do error: %v\n", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("ReadAll error: %v\n", err)
	}
	message := Message{}
	if err := json.Unmarshal(body, &message); err != nil {
		log.Fatalf("Unmarshal error: %v\n", err)
	}
	if message.Error {
		log.Fatalf("Response error: %s\n", message.Message)
	}
}

func updateReported2prio(stmt *sql.Stmt, id string) {
	res, err := stmt.Exec(id)
	if err != nil {
		log.Fatalf("Update error: %v\n", err)
	}
	rowCnt, err := res.RowsAffected()
	if err != nil {
		log.Fatalf("Update error: %v\n", err)
	}
	if rowCnt != 1 {
		log.Fatalf("Update error: %d rows were updated instead of 1\n", rowCnt)
	}
}
