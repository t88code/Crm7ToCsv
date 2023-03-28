package main

import (
	"Crm7ToCsv/pkg/config"
	"Crm7ToCsv/pkg/logging"
	"fmt"
	"github.com/pkg/errors"
	"golang.org/x/text/encoding/charmap"
	"math"
	"os"
	"strings"
)

func report(exportResult ExportResult) error {

	logger := logging.GetLogger()
	logger.Debug("ImportCrm7ToCsv start report")
	defer logger.Debug("ImportCrm7ToCsv stop report")

	var err error
	cfg := config.GetConfig()

	if cfg.REPORT.Path != "" {
		err = os.MkdirAll(cfg.REPORT.Path, 0770)
		if err != nil {
			return errors.Wrapf(err, "Не удалось создать папку (%s)", cfg.REPORT.Path)
		}
	}

	y, m, d := date.Date()
	fileCsvNameReport := fmt.Sprintf("KGVD_%02d%02d%d_report.csv", d, m, y)
	filePath := fmt.Sprintf("%s/%s", strings.TrimRight(cfg.REPORT.Path, "/"), fileCsvNameReport)
	fileReport, err := os.Create(filePath)
	defer func(fileReport *os.File) {
		err := fileReport.Close()
		if err != nil {
			logger.Fatalln(err)
		}
	}(fileReport)
	if err != nil {
		return errors.Wrapf(err, "Ошибка при создании файла %s", filePath)
	}
	encoder := charmap.Windows1251.NewEncoder().Writer(fileReport)

	_, err = encoder.Write([]byte(fmt.Sprintf("Количество строк: %d\r\nСумма: %.2f\n", exportResult.countLine, exportResult.summa)))
	if err != nil {
		return errors.Wrapf(err, "Ошибка при записи в файл %s", filePath)
	}

	if len(exportResult.recordsByDateDuble) > 0 {
		_, err = encoder.Write([]byte("Найдены дубли строк:\r\n"))
		if err != nil {
			return errors.Wrapf(err, "Ошибка при записи в файл %s", filePath)
		}

		for _, records := range exportResult.recordsByDateDuble {
			for _, record := range records {
				recordSumma := math.Round(record.Summa*100) / 100
				exportResult.summa = exportResult.summa + recordSumma
				_, err := encoder.Write([]byte(fmt.Sprintf(`"%s";"%s";"%s";"%d";"%0.2f";"%s"%s`,
					record.Date.Format("02.01.2006"),
					record.Id,
					record.Tabn,
					record.FoodType,
					-recordSumma,
					record.Ppn,
					"\r\n")))
				if err != nil {
					return errors.Wrapf(err, "Ошибка при записи в файл %s", filePath)
				}
			}
		}
	}

	return nil
}
