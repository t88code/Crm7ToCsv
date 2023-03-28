package main

import (
	"Crm7ToCsv/pkg/config"
	"Crm7ToCsv/pkg/logging"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"golang.org/x/text/encoding/charmap"
	"math"
	"os"
	"strings"
	"time"
)

const (
	SELECT_FOR_EXPORT_BY_TIME = `
SELECT 
 CAST(ct.TRANSACTION_TIME AS date) AS 'date' 
   ,cp.EXTERNAL_CODE as id 
   ,cp.NOTES as tab_n 
   ,cc.CONTACT_VALUE as food_type 
   --,sum(ct.SUMM) as SUMMA 
   ,ct.SUMM
   ,ccc.QUALIFIER as PP_N
   ,ct.TRANSACTION_TIME
      --,[TRANSACTION_TYPE] 
      --,[OPERATION_TYPE] 
      --,[ACCOUNT_ID] 
      --,[CARD_CODE] 
      --,ccc.[CLIENT_ID] 
   --,ccc.NAME 
   --,cpa.ACCOUNT_TYPE_ID 
   --,cpa.PEOPLE_ID 
 
  FROM CARD_TRANSACTIONS as ct 
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
 --and cp.EXTERNAL_CODE = '00396127' 																				 
 order by ct.TRANSACTION_TIME
 `
	SELECT_FOR_EXPORT_BY_DATE = `
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
 
  FROM CARD_TRANSACTIONS as ct 
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
)

type ExportResult struct {
	countLine          int
	summa              float64
	recordsByDateDuble map[string][]RecordByDate
}

func export(dbCrm7 *sqlx.DB) error {

	logger := logging.GetLogger()
	logger.Debug("ImportCrm7ToCsv start export")
	defer logger.Debug("ImportCrm7ToCsv stop export")

	var err error
	cfg := config.GetConfig()

	if cfg.EXPORT.Path != "" {
		err = os.MkdirAll(cfg.EXPORT.Path, 0770)
		if err != nil {
			return errors.Wrapf(err, "Не удалось создать папку (%s)", cfg.EXPORT.Path)
		}
	}

	y, m, d := date.Date()
	timeBefore := fmt.Sprintf("%d%02d%02d", y, m, d)
	fileCsvName := fmt.Sprintf("KGVD_%02d%02d%d.csv", d, m, y)

	y, m, d = date.Add(24 * time.Hour).Date()
	timeAfter := fmt.Sprintf("%d%02d%02d", y, m, d)

	// создание файла выгрузки
	filePath := fmt.Sprintf("%s/%s", strings.TrimRight(cfg.EXPORT.Path, "/"), fileCsvName)
	fileExport, err := os.Create(filePath)
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			logger.Fatalln(err)
		}
	}(fileExport)
	if err != nil {
		return errors.Wrapf(err, "Ошибка при создании файла %s", filePath)
	}
	encoder := charmap.Windows1251.NewEncoder().Writer(fileExport)
	var exportResult ExportResult
	var recordsByDate []RecordByDate
	err = dbCrm7.Select(&recordsByDate, SELECT_FOR_EXPORT_BY_DATE,
		cfg.CRM7.AccountTypeID,
		fmt.Sprintf("%d1", cfg.CRM7.AccountTypeID),
		fmt.Sprintf("%d2", cfg.CRM7.AccountTypeID),
		cfg.CRM7.ContactTypeIDFoodType,
		strings.ReplaceAll(cfg.CRM7.QualifierIgnore, ",", "','"),
		timeBefore,
		timeAfter)
	if err != nil {
		return errors.Wrap(err, "Ошибка при получении данных из SQL")
	}

	recordsByDateMapById := make(map[string][]RecordByDate, 0)
	for _, record := range recordsByDate {
		recordsByDateMapById[record.Id] = append(recordsByDateMapById[record.Id], record)
	}

	exportResult.recordsByDateDuble = make(map[string][]RecordByDate, 0)
	for id, record := range recordsByDateMapById {
		if len(record) > 1 {
			exportResult.recordsByDateDuble[id] = record
		}
	}

	if cfg.EXPORT.Mode == 3 {
		exportResult.countLine = len(recordsByDate)
		for _, record := range recordsByDate {
			recordSumma := math.Round(record.Summa*100) / 100
			exportResult.summa = exportResult.summa + recordSumma
			recordString := fmt.Sprintf(`"%s";"%s";"%s";"%d";"%0.2f";"%s"%s`,
				record.Date.Format("02.01.2006"),
				record.Id,
				record.Tabn,
				record.FoodType,
				-recordSumma,
				record.Ppn,
				"\r\n")
			_, err := encoder.Write([]byte(recordString))
			if err != nil {
				return errors.Wrapf(err, "Ошибка при записи в файл %s", filePath)
			}
		}
	} else {
		// Добавить все без дублей
		for _, record := range recordsByDate {
			if _, ok := exportResult.recordsByDateDuble[record.Id]; ok {
				continue
			} else {
				exportResult.countLine++
			}
			recordSumma := math.Round(record.Summa*100) / 100
			exportResult.summa = exportResult.summa + recordSumma
			recordString := fmt.Sprintf(`"%s";"%s";"%s";"%d";"%0.2f";"%s"%s`,
				record.Date.Format("02.01.2006"),
				record.Id,
				record.Tabn,
				record.FoodType,
				-recordSumma,
				record.Ppn,
				"\r\n")
			_, err := encoder.Write([]byte(recordString))
			if err != nil {
				return errors.Wrapf(err, "Ошибка при записи в файл %s", filePath)
			}
		}

		// Получить recordsByTime
		var recordsByTime []RecordsByTime
		err = dbCrm7.Select(&recordsByTime, SELECT_FOR_EXPORT_BY_TIME,
			cfg.CRM7.AccountTypeID,
			fmt.Sprintf("%d1", cfg.CRM7.AccountTypeID),
			fmt.Sprintf("%d2", cfg.CRM7.AccountTypeID),
			cfg.CRM7.ContactTypeIDFoodType,
			strings.ReplaceAll(cfg.CRM7.QualifierIgnore, ",", "','"),
			timeBefore,
			timeAfter)
		if err != nil {
			return errors.Wrap(err, "Ошибка при получении данных из SQL")
		}

		recordsByTimeMapById := make(map[string][]RecordsByTime, 0)
		for _, record := range recordsByTime {
			recordsByTimeMapById[record.Id] = append(recordsByTimeMapById[record.Id], record)
		}

		// выполнить поиск последнего PPN
		for id, recordDubles := range exportResult.recordsByDateDuble {
			exportResult.countLine++

			var record RecordByDate
			ppnLast := recordsByTimeMapById[id][len(recordsByTimeMapById[id])-1].Ppn

			switch cfg.EXPORT.Mode {
			case 1:
				// записать только последнюю PPN
				for _, duble := range recordDubles {
					if duble.Ppn == ppnLast {
						record = duble
						break
					}
				}
			case 2:
				// записать сумму всех PPN
				record = recordDubles[0]
				record.Summa = 0
				record.Ppn = ppnLast
				for _, duble := range recordDubles {
					record.Summa = record.Summa + duble.Summa
				}
			}

			recordSumma := math.Round(record.Summa*100) / 100
			exportResult.summa = exportResult.summa + recordSumma
			recordString := fmt.Sprintf(`"%s";"%s";"%s";"%d";"%0.2f";"%s"%s`,
				record.Date.Format("02.01.2006"),
				record.Id,
				record.Tabn,
				record.FoodType,
				-recordSumma,
				record.Ppn,
				"\r\n")
			_, err := encoder.Write([]byte(recordString))
			if err != nil {
				return errors.Wrapf(err, "Ошибка при записи в файл %s", filePath)
			}

			logger.Warningln(recordDubles)
			logger.Warningln(recordsByTimeMapById[id])
			logger.Warningln(record)
		}
	}

	if cfg.REPORT.Report {
		err := report(exportResult)
		if err != nil {
			return errors.Wrapf(err, "failed in report")
		}
	}

	return nil
}
