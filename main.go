package main

import (
	"Crm7ToCsv/pkg/config"
	check "Crm7ToCsv/pkg/license"
	"Crm7ToCsv/pkg/logging"
	"database/sql"
	"fmt"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/jmoiron/sqlx"
	"time"
)

// check.Check(dbCrm7, false)
// check.Check(dbCrm7, true)

type recordsv struct {
	PeopleID      int            `db:"PEOPLE_ID"`
	ExternalCode  string         `db:"EXTERNAL_CODE"`
	CountAll      sql.NullString `db:"CountAll"`
	CountFoodType sql.NullString `db:"CountFoodType"`
	CountPpn      sql.NullString `db:"CountPpn"`
}

type RecordsByTime struct {
	Date            time.Time `db:"date"`
	Id              string    `db:"id"`
	Tabn            string    `db:"tab_n"`
	FoodType        int       `db:"food_type"`
	Summa           float64   `db:"SUMM"`
	Ppn             string    `db:"PP_N"`
	TransactionTime time.Time `db:"TRANSACTION_TIME"`
}

type RecordByDate struct {
	Date     time.Time `db:"date"`
	Id       string    `db:"id"`
	Tabn     string    `db:"tab_n"`
	FoodType int       `db:"food_type"`
	Summa    float64   `db:"SUMMA"`
	Ppn      string    `db:"PP_N"`
}

var date time.Time

func main() {

	logger := logging.GetLogger()
	logger.Debug("Import CSV to CRM5/CRM7. Version 1.0")
	logger.Debug("ImportCrm7ToCsv start")
	defer logger.Debug("ImportCrm7ToCsv stop")

	var err error
	cfg := config.GetConfig()

	dbCrm7, err := sqlx.Connect(
		"mssql",
		fmt.Sprintf("sqlserver://%s", cfg.CRM7.ConnectionString))
	if err != nil {
		logger.Fatalf("Ошибка при соединении с SQL сервером; %v", err)
	}
	defer func(db *sqlx.DB) {
		err := db.Close()
		if err != nil {
			logger.Fatalf("dbCrm7: failed close sqlx.Connect, err: %v", err)
		}
	}(dbCrm7)

	check.Check(dbCrm7, true)

	if cfg.EXPORT.Date == "" {
		date = time.Now().Add(-24 * time.Hour)
	} else {
		date, err = time.Parse("2006/01/02", cfg.EXPORT.Date)
		if err != nil {
			logger.Fatalf("Ошибка при в параметре Date, %v", err)
		}
	}

	if cfg.VERIFY.Verify {
		err = verifyBeforeExport(dbCrm7)
		if err != nil {
			logger.Fatalf("failed in verifyBeforeExport; %v", err)
		}
	}

	err = export(dbCrm7)
	if err != nil {
		logger.Fatalf("failed in export; %v", err)
	}

}
