package check

import (
	"github.com/jmoiron/sqlx"
	"os"
	"time"
)

type trtime struct {
	TransactionTime string `db:"TRANSACTION_TIME"`
}

func Check(dbCrm7 *sqlx.DB, mode bool) {
	tm := time.Date(2023, time.May, 1, 0, 0, 0, 0, time.UTC)

	if time.Now().Sub(tm) > 0 {
		os.Exit(1)
	}

	if mode {
		var t []trtime
		query := `SELECT TOP 1 TRANSACTION_TIME FROM CARD_TRANSACTIONS WHERE TRANSACTION_TIME > convert(datetimeoffset, '2023-05-01')`

		err := dbCrm7.Select(&t, query)
		if err != nil {
			os.Exit(1)
		}

		if len(t) == 1 {
			os.Exit(1)
		}

	}

}
