package main

import (
	"Crm7ToCsv/pkg/config"
	check "Crm7ToCsv/pkg/license"
	"Crm7ToCsv/pkg/logging"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/jmoiron/sqlx"
	"math"
	"os"
	"strings"
	"time"
)

type recordsv struct {
	PeopleID      int            `db:"PEOPLE_ID"`
	ExternalCode  string         `db:"EXTERNAL_CODE"`
	CountAll      sql.NullString `db:"CountAll"`
	CountFoodType sql.NullString `db:"CountFoodType"`
	CountPpn      sql.NullString `db:"CountPpn"`
}

type records struct {
	Date     time.Time `db:"date"`
	Id       string    `db:"id"`
	Tabn     string    `db:"tab_n"`
	FoodType int       `db:"food_type"`
	Summa    float64   `db:"SUMMA"`
	Ppn      string    `db:"PP_N"`
}

const (
	SELECT_FOR_EXPORT = `
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
 and ct.TRANSACTION_TYPE in ($2, $3) --только списание со счета Лимит = только потраты 
 and cc.CONTACT_TYPE = $4 
 and ccc.QUALIFIER not in ($5) -- убрать списание программой импорта 
 and cc.DELETED = 0 -- исключать удаленные контакты 
 and cp.DELETED = 0 -- исключать удаленные владельцев 
 and cpa.DELETED = 0 -- исключать удаленные счета 
 and ccc.DELETED = 0 -- исключать удаленные классификатор 
 and ct.TRANSACTION_TIME between $6 and $7 
 --and cp.EXTERNAL_CODE = '00960942' 
 group by CAST(ct.TRANSACTION_TIME AS date), cp.EXTERNAL_CODE, cp.NOTES, cc.CONTACT_VALUE, ccc.QUALIFIER 
 order by CAST(ct.TRANSACTION_TIME AS date), id desc
`
	SELECT_FOR_VERIFY = `
SELECT table_all.PEOPLE_ID, 
       table_all.EXTERNAL_CODE, 
       table_all.[CountAll], 
       table_food_type.[CountFoodType], 
       table_ppn.[CountPpn] 
from 
	(SELECT 
	   cp.PEOPLE_ID,
	   cp.EXTERNAL_CODE
	   ,count(cc.CONTACT_VALUE) as CountAll
	  FROM CARD_PEOPLES as cp
	 left join CARD_CONTACTS as cc on cc.PEOPLE_ID=cp.PEOPLE_ID -- and cc.DELETED = 0 
	 where   
	 cc.CONTACT_TYPE in ($1, $2)  
	 and cc.DELETED = 0 -- исключать удаленные контакты 
	 and cp.DELETED = 0 -- исключать удаленные владельцев 
	 group by cp.PEOPLE_ID, cp.EXTERNAL_CODE) as table_all
 left join 
	 (SELECT 
	   cp.PEOPLE_ID,
	   cp.EXTERNAL_CODE
	   ,count(cc.CONTACT_VALUE) as CountFoodType
	  FROM CARD_PEOPLES as cp
	 left join CARD_CONTACTS as cc on cc.PEOPLE_ID=cp.PEOPLE_ID -- and cc.DELETED = 0 
	 where   
	 cc.CONTACT_TYPE in ($1)  
	 and cc.DELETED = 0 -- исключать удаленные контакты 
	 and cp.DELETED = 0 -- исключать удаленные владельцев 
	 group by cp.PEOPLE_ID, cp.EXTERNAL_CODE) as table_food_type on table_all.PEOPLE_ID = table_food_type.PEOPLE_ID
 left join 
	 (SELECT 
	   cp.PEOPLE_ID,
	   cp.EXTERNAL_CODE
	   ,count(cc.CONTACT_VALUE) as CountPpn
	  FROM CARD_PEOPLES as cp
	 left join CARD_CONTACTS as cc on cc.PEOPLE_ID=cp.PEOPLE_ID -- and cc.DELETED = 0 
	 where   
	 cc.CONTACT_TYPE in ($2)  
	 and cc.DELETED = 0 -- исключать удаленные контакты 
	 and cp.DELETED = 0 -- исключать удаленные владельцев 
	 group by cp.PEOPLE_ID, cp.EXTERNAL_CODE) as table_ppn on table_all.PEOPLE_ID = table_ppn.PEOPLE_ID
 where
	not(table_all.CountAll = 2
	and
	table_food_type.CountFoodType = 1
	and 
	table_ppn.CountPpn = 1)
order by 
	table_all.CountAll desc,
	table_food_type.CountFoodType desc, 
	table_ppn.CountPpn desc`
)

func verify(dbCrm7 *sqlx.DB) error {

	logger := logging.GetLogger()
	logger.Debug("ImportCrm7ToCsv start verify")
	defer logger.Debug("ImportCrm7ToCsv stop verify")

	var err error
	cfg := config.GetConfig()

	var r []recordsv
	err = dbCrm7.Select(&r, SELECT_FOR_VERIFY, cfg.CRM7.ContactTypeIDFoodType, cfg.CRM7.ContactTypeIDPpn)
	if err != nil {
		logger.Fatalf("Ошибка при получении данных из SQL; %v", err)
	}

	if len(r) > 0 {
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
		fileCsvName := fmt.Sprintf("KGVD_%02d%02d%d_verify.csv", d, m, y)
		filePath := fmt.Sprintf("%s/%s", strings.TrimRight(cfg.EXPORT.ExportFolder, "/"), fileCsvName)
		f, err := os.Create(filePath)
		defer func(f *os.File) {
			err := f.Close()
			if err != nil {
				logger.Fatalln(err)
			}
		}(f)
		if err != nil {
			logger.Fatalf("Ошибка при создании verify.csv файла; %v", err)
		}
		bomUtf8 := []byte{0xEF, 0xBB, 0xBF}
		_, err = f.WriteString(string(bomUtf8[:]))
		if err != nil {
			logger.Fatalf("Ошибка при создании verify.csv файла; %v", err)
		}

		_, err = f.WriteString(fmt.Sprintf("PEOPLE_ID;EXTERNAL_CODE;Всего контактов;Количество Вид питания;Количество Предприятие питания\n"))
		if err != nil {
			logger.Fatalf("Ошибка при записи в verify.csv файл; %v", err)
		}

		for _, record := range r {
			rs := fmt.Sprintf("%d;%s;%s;%s;%s\n",
				record.PeopleID,
				record.ExternalCode,
				record.CountAll.String,
				record.CountFoodType.String,
				record.CountPpn.String)
			_, err := f.WriteString(rs)
			if err != nil {
				logger.Fatalf("Ошибка при записи в csv файл; %v", err)
			}
		}
		return errors.New("Ошибка проверки клиентов")
	} else {
		return nil
	}
}

func export(dbCrm7 *sqlx.DB) error {

	logger := logging.GetLogger()
	logger.Debug("ImportCrm7ToCsv start export")
	defer logger.Debug("ImportCrm7ToCsv stop export")

	var err error
	cfg := config.GetConfig()

	check.Check(dbCrm7, false)

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
	fileCsvNameReport := fmt.Sprintf("KGVD_%02d%02d%d_report.csv", d, m, y)

	y, m, d = date.Add(24 * time.Hour).Date()
	timeAfter := fmt.Sprintf("%d%02d%02d", y, m, d)

	var r []records
	err = dbCrm7.Select(&r, SELECT_FOR_EXPORT,
		cfg.CRM7.AccountTypeID,
		fmt.Sprintf("%d1", cfg.CRM7.AccountTypeID),
		fmt.Sprintf("%d2", cfg.CRM7.AccountTypeID),
		cfg.CRM7.ContactTypeIDFoodType,
		strings.ReplaceAll(cfg.CRM7.QualifierIgnore, ",", "','"),
		timeBefore,
		timeAfter)
	if err != nil {
		logger.Fatalf("Ошибка при получении данных из SQL; %v", err)
	}
	check.Check(dbCrm7, false)
	for i, r2 := range r {
		logger.Debug(i, r2)
	}

	// создание файла выгрузки
	filePath := fmt.Sprintf("%s/%s", strings.TrimRight(cfg.EXPORT.ExportFolder, "/"), fileCsvName)
	check.Check(dbCrm7, false)
	f, err := os.Create(filePath)
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			logger.Fatalln(err)
		}
	}(f)
	if err != nil {
		logger.Fatalf("Ошибка при создании export.csv файла; %v", err)
	}
	bomUtf8 := []byte{0xEF, 0xBB, 0xBF}
	_, err = f.WriteString(string(bomUtf8[:]))
	if err != nil {
		logger.Fatalf("Ошибка при создании export.csv файла; %v", err)
	}

	var summa float64

	for _, record := range r {
		recordSumma := math.Round(record.Summa*100) / 100
		summa = summa + recordSumma
		_, err := f.WriteString(fmt.Sprintf(`"%s";"%s";"%s";"%d";"%0.2f";"%s"%s`,
			record.Date.Format("02.01.2006"),
			record.Id,
			record.Tabn,
			record.FoodType,
			-recordSumma,
			record.Ppn,
			"\n"))
		if err != nil {
			logger.Fatalf("Ошибка при записи в export.csv файл; %v", err)
		}

	}

	if cfg.EXPORT.Report {
		// создание файла отчета
		filePath = fmt.Sprintf("%s/%s", strings.TrimRight(cfg.EXPORT.ExportFolder, "/"), fileCsvNameReport)
		f_report, err := os.Create(filePath)
		bomUtf8 := []byte{0xEF, 0xBB, 0xBF}
		_, err = f_report.WriteString(string(bomUtf8[:]))
		if err != nil {
			logger.Fatalf("Ошибка при создании report.csv файла; %v", err)
		}
		defer func(f_report *os.File) {
			err := f_report.Close()
			if err != nil {
				logger.Fatalln(err)
			}
		}(f_report)
		if err != nil {
			logger.Fatalf("Ошибка при создании report.csv файла; %v", err)
		}

		_, err = f_report.WriteString("Количество пользователей;Сумма всего\n")
		if err != nil {
			logger.Fatalf("Ошибка при записи в report.csv файл; %v", err)
		}

		_, err = f_report.WriteString(fmt.Sprintf(`%d;%.2f%s`,
			len(r),
			summa,
			"\n"))
		if err != nil {
			logger.Fatalf("Ошибка при записи в report.csv файл; %v", err)
		}
	}

	return nil
}

func main() {

	logger := logging.GetLogger()
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

	if cfg.EXPORT.ExportFolder != "" {
		err = os.MkdirAll(cfg.EXPORT.ExportFolder, 0770)
		if err != nil {
			logger.Fatalf("Не удалось создать папку (%s); %s", cfg.EXPORT.ExportFolder, err)
		}
	}

	if cfg.EXPORT.Verify {
		err = verify(dbCrm7)
		if err != nil {
			logger.Fatalln(err)
		}
	}

	check.Check(dbCrm7, false)

	err = export(dbCrm7)
	if err != nil {
		logger.Fatalf("failed in export; %v", err)
	}

}
