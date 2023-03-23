package main

import (
	"Crm7ToCsv/pkg/config"
	check "Crm7ToCsv/pkg/license"
	"Crm7ToCsv/pkg/logging"
	"fmt"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/jmoiron/sqlx"
	"os"
	"strings"
	"time"
)

type records struct {
	Date     time.Time `db:"date"`
	Id       string    `db:"id"`
	Tabn     string    `db:"tab_n"`
	FoodType int       `db:"food_type"`
	Summa    string    `db:"SUMMA"`
	Ppn      string    `db:"PP_N"`
}

func main() {

	query := `
SELECT 
 CAST(ct.TRANSACTION_TIME AS date) AS 'date' 
   ,cp.EXTERNAL_CODE as id 
   ,cp.NOTES as tab_n 
   ,cc.CONTACT_VALUE as food_type 
   ,sum(ct.SUMM) as SUMMA 
   ,ccc.QUALIFIER as PP_N 
      --,[TRANSACTION_TYPE] 
      --,[OPERATION_TYPE] 
      --,[ACCOUNT_ID] 
      --,[CARD_CODE] 
      --,ccc.[CLIENT_ID] 
   --,ccc.NAME 
   --,cpa.ACCOUNT_TYPE_ID 
   --,cpa.PEOPLE_ID 
 
  FROM [dbo].[CARD_TRANSACTIONS] as ct 
    left join CARD_PEOPLE_ACCOUNTS as cpa on cpa.PEOPLE_ACCOUNT_ID=ct.ACCOUNT_ID -- and cpa.DELETED = 0 
 left join CARD_PEOPLES as cp on cp.PEOPLE_ID=cpa.PEOPLE_ID -- and cp.DELETED = 0 
 left join CARD_CONTACTS as cc on cc.PEOPLE_ID=cpa.PEOPLE_ID -- and cc.DELETED = 0 
 left join CARD_CLIENTS as ccc on ccc.CLIENT_ID=ct.CLIENT_ID -- and ccc.DELETED = 0 --and ccc.CLIENT_ID != 2 
 where   
 cpa.ACCOUNT_TYPE_ID = $1 -- тип счета Лимит 
 and ct.TRANSACTION_TYPE = 31 --только списание со счета Лимит = только потраты 
 and cc.CONTACT_TYPE = $2 
 and ccc.QUALIFIER not in ($3) -- убрать списание программой импорта 
 and cc.DELETED = 0 -- исключать удаленные контакты 
 and cp.DELETED = 0 -- исключать удаленные владельцев 
 and cpa.DELETED = 0 -- исключать удаленные счета 
 and ccc.DELETED = 0 -- исключать удаленные классификатор 
 and ct.TRANSACTION_TIME between $4 and $5 
 --and cp.EXTERNAL_CODE = '00960942' 
 group by CAST(ct.TRANSACTION_TIME AS date), cp.EXTERNAL_CODE, cp.NOTES, cc.CONTACT_VALUE, ccc.QUALIFIER 
 order by CAST(ct.TRANSACTION_TIME AS date), id desc
`

	logger := logging.GetLogger()
	logger.Debug("ImportCrm7ToCsv start")
	defer logger.Debug("ImportCrm7ToCsv stop")

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

	var date time.Time
	if cfg.EXPORT.Date == "" {
		date = time.Now().Add(-24 * time.Hour)
	} else {
		date, err = time.Parse("2006/01/02", cfg.EXPORT.Date)
		if err != nil {
			logger.Fatalf("Ошибка при в параметре Date; %v", err)
		}
	}

	y, m, d := date.Date()
	timeBefore := fmt.Sprintf("%d%02d%02d", y, m, d)
	fileCsvName := fmt.Sprintf("KGVD_%02d%02d%d.csv", d, m, y)

	y, m, d = date.Add(24 * time.Hour).Date()
	timeAfter := fmt.Sprintf("%d%02d%02d", y, m, d)

	check.Check(dbCrm7, false)
	var r []records
	err = dbCrm7.Select(&r, query,
		cfg.CRM7.AccountTypeID,
		cfg.CRM7.ContactTypeIDFoodType,
		strings.ReplaceAll(cfg.CRM7.QualifierIgnore, ",", "','"),
		timeBefore,
		timeAfter)
	if err != nil {
		logger.Fatalf("Ошибка при получении данных из SQL; %v", err)
	}
	check.Check(dbCrm7, false)
	for i, r2 := range r {
		check.Check(dbCrm7, false)
		logger.Debug(i, r2)
	}
	check.Check(dbCrm7, false)
	filePath := fileCsvName
	if cfg.EXPORT.ExportFolder != "" {
		err = os.MkdirAll(cfg.EXPORT.ExportFolder, 0770)
		if err != nil {
			logger.Fatalf("Не удалось создать папку (%s); %s", cfg.EXPORT.ExportFolder, err)
		}
		filePath = fmt.Sprintf("%s/%s", strings.TrimRight(cfg.EXPORT.ExportFolder, "/"), fileCsvName)
	}
	check.Check(dbCrm7, false)
	f, err := os.Create(filePath)
	defer f.Close()
	if err != nil {
		logger.Fatalf("Ошибка при создании csv файла; %v", err)
	}
	check.Check(dbCrm7, false)
	for _, record := range r {
		_, err := f.WriteString(fmt.Sprintf(`"%s";"%s";"%s";"%d";"%s";"%s"%s`,
			record.Date.Format("02.01.2006"),
			record.Id,
			record.Tabn,
			record.FoodType,
			strings.Trim(record.Summa, "-"),
			record.Ppn,
			"\n"))
		if err != nil {
			logger.Fatalf("Ошибка при записи в csv файл; %v", err)
		}
	}
}
