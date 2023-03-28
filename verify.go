package main

import (
	"Crm7ToCsv/pkg/config"
	"Crm7ToCsv/pkg/logging"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"golang.org/x/text/encoding/charmap"
	"os"
	"strings"
)

const (
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

func verifyBeforeExport(dbCrm7 *sqlx.DB) error {

	logger := logging.GetLogger()
	logger.Debug("ImportCrm7ToCsv start verifyBeforeExport")
	defer logger.Debug("ImportCrm7ToCsv stop verifyBeforeExport")

	var err error
	cfg := config.GetConfig()

	if cfg.VERIFY.Path != "" {
		err = os.MkdirAll(cfg.VERIFY.Path, 0770)
		if err != nil {
			return errors.Wrapf(err, "Не удалось создать папку (%s)", cfg.VERIFY.Path)
		}
	}

	var r []recordsv
	err = dbCrm7.Select(&r, SELECT_FOR_VERIFY, cfg.CRM7.ContactTypeIDFoodType, cfg.CRM7.ContactTypeIDPpn)
	if err != nil {
		return errors.Wrap(err, "Ошибка при получении данных из SQL")
	}

	if len(r) > 0 {
		y, m, d := date.Date()
		fileCsvName := fmt.Sprintf("KGVD_%02d%02d%d_verify.csv", d, m, y)
		filePath := fmt.Sprintf("%s/%s", strings.TrimRight(cfg.VERIFY.Path, "/"), fileCsvName)
		fileVerify, err := os.Create(filePath)
		defer func(f *os.File) {
			err := f.Close()
			if err != nil {
				logger.Fatalln(err)
			}
		}(fileVerify)
		if err != nil {
			return errors.Wrapf(err, "Ошибка при создании файла %s", filePath)
		}

		encoder := charmap.Windows1251.NewEncoder().Writer(fileVerify)
		_, err = encoder.Write([]byte("PEOPLE_ID;EXTERNAL_CODE;Всего контактов;Количество Вид питания;Количество Предприятие питания\r\n"))
		if err != nil {
			return errors.Wrapf(err, "Ошибка при записи в файл %s", filePath)
		}

		for i, record := range r {
			endLine := "\r\n"
			if i == len(r)-1 {
				endLine = ""
			}
			rs := fmt.Sprintf("%d;%s;%s;%s;%s%s",
				record.PeopleID,
				record.ExternalCode,
				record.CountAll.String,
				record.CountFoodType.String,
				record.CountPpn.String,
				endLine)
			_, err = encoder.Write([]byte(rs))
			if err != nil {
				return errors.Wrapf(err, "Ошибка при записи в файл %s", filePath)
			}
		}

		if cfg.VERIFY.VeryfyErrorsSkip {
			return nil
		} else {
			return errors.New("Ошибка проверки клиентов. Экпорт запущен не будет, пока не будут исправлены ошибки")
		}
	} else {
		return nil
	}
}
