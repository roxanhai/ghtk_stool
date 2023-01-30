package main

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"github.com/urfave/cli"
	"github.com/xuri/excelize/v2"
)

type RowExcel struct {
	Count     int     `json:count`
	Label     string  `json:label`
	Min       float64 `json:avg_response`
	Max       float64 `json:avg_response`
	Avg       float64 `json:avg_response`
	Query     string  `json:query`
	Sheetname string  `json:query`
	TaskId    int     `json:task_id`
}

var summary = map[string]string{
	"ECOM-DB-OTHERSERVICES":      "SAIYAN-4581",
	"ECOM-DB-ADMIN":              "SAIYAN-4592",
	"NEWKT":                      "SAIYAN-4553",
	"FULFILLMENT":                "SAIYAN-4549",
	"ERP":                        "SAIYAN-4543",
	"BANK-PAYMENT":               "SAIYAN-4764",
	"ECOM-DB-CS":                 "SAIYAN-4548",
	"ECOM-DB-MASTER":             "SAIYAN-4582",
	"EKYC":                       "SAIYAN-4763",
	"LOYALTY":                    "SAIYAN-4551",
	"ECOM-DB-BACKGROUND":         "SAIYAN-4603",
	"QLTS":                       "SAIYAN-4544",
	"QC-TOOL":                    "SAIYAN-4560",
	"PAYMENT":                    "SAIYAN-4555",
	"XTEAM":                      "SAIYAN-4557",
	"NEW-CODS":                   "SAIYAN-4552",
	"SA-ARCHIVED":                "SAIYAN-4554",
	"MOSHOP":                     "SAIYAN-4546",
	"THUNDER":                    "SAIYAN-4551",
	"BIGDATA":                    "SAIYAN-4547",
	"INTER":                      "SAIYAN-4550",
	"QLTM":                       "SAIYAN-4556",
	"ECOM-DB-CODS-OTHERSERVICES": "SAIYAN-4604",
}

func main() {
	app := cli.NewApp()
	app.Name = "Slowqueries Tool"
	app.Usage = "Tool for tracking Slowqueries"

	myFlags := []cli.Flag{
		cli.StringFlag{
			Name:  "path",
			Value: "performance.xlsx",
		},
	}
	app.Commands = []cli.Command{
		{
			//VD: go run stool.go gp --path {path}
			Name:  "gp",
			Usage: "Lấy dữ liệu, lọc và tiến hành tạo Task Jira",
			Flags: myFlags,
			Action: func(c *cli.Context) error {
				gp := c.String("path")
				err := godotenv.Load(".env")
				if err != nil {
					log.Fatalf("Error loading .env file")
				}
				finalRows, weekTask := processRowFromExcel(gp)
				for _, entry := range finalRows {
					println(entry.TaskId, " ", entry.Sheetname)
				}
				writeToCSVandDB(finalRows, weekTask)
				return nil
			},
		},
		{
			//VD: go run stool.go icd --path {path}
			Name:  "icd",
			Usage: "Kiểm tra và xuất dữ liệu bất thường (Incident) kèm Ticket ID tương ứng",
			Flags: myFlags,
			Action: func(c *cli.Context) error {
				icd := c.String("path")
				err := godotenv.Load(".env")
				if err != nil {
					log.Fatalf("Error loading .env file")
				}
				getHighPriorityQuery(icd)
				return nil
			},
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		fmt.Print("Error:")
		log.Fatal(err)
	}
}

// Functions
func writeToCSVandDB(finalRows []RowExcel, weekTask int) {
	db, _ := sql.Open(os.Getenv("DRIVER_NAME"), os.ExpandEnv("${DB_USERNAME}:${DB_PASSWORD}@tcp($DB_HOST:$DB_PORT)/$DB_NAME"))
	defer db.Close()
	for _, entry := range finalRows {
		checkQuerry1 := strings.Split(entry.Query, " ")
		builderQuerySample := strings.Builder{}
		for i := 0; i < len(checkQuerry1); i++ {
			builderQuerySample.WriteString(checkQuerry1[i])
			if checkQuerry1[i] == "from" {
				builderQuerySample.WriteString(checkQuerry1[i+1])
				break
			}

		}
		mainTable := " "
		splitQuery := strings.Split(entry.Query, " ")
		for i := 0; i < len(splitQuery); i++ {
			if splitQuery[i] == "from" {
				mainTable = splitQuery[i+1]
				break
			}
		}

		summary_ticket := "Slow query by " + entry.Label + " table " + mainTable + " #" + strconv.Itoa(entry.TaskId)
		description := strings.Builder{}
		description.WriteString("{code:sql}" + entry.Query)
		description.WriteString("{code}\n * *Avg Response Time* : " + fmt.Sprintf("%.2f", entry.Avg))
		description.WriteString("\n * *Count/Week*: " + strconv.Itoa(entry.Count))
		description.WriteString("\n * *Main Table*: " + mainTable)
		description.WriteString("\n * *User*: " + entry.Label)
		description.WriteString("\n * *Sample Query*: ")
		description.WriteString("{code:sql} {code}")
		description.WriteString("\n * *Query Time:*: ")
		description.WriteString("\n * *HTTP Request*: ")
		description.WriteString("{code:sql} {code}")

		//Request Body (Json)
		url := "https://jira.ghtklab.com/rest/api/2/issue/"
		jsonData := map[string]interface{}{
			"fields": map[string]interface{}{
				"project": map[string]interface{}{
					"key": "SAIYAN",
				},
				"customfield_10108": summary[strings.ToUpper(entry.Sheetname)],
				"summary":           summary_ticket,
				"labels":            []string{"SLQ", "newStool", entry.Label},
				"assignee":          nil,
				"description":       description.String(),
				"issuetype": map[string]interface{}{
					"name": "Task",
				},
			},
		}
		jsonValue, _ := json.MarshalIndent(jsonData, "", "\t")
		req, _ := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(jsonValue))
		req.Header.Add("Content-Type", "application/json")
		req.Header.Add("Authorization", "Bearer OTc3ODMxOTU1ODkyOqWkqAZaN7a0iRGlEhB29ZHi/gAC")
		response, _ := http.DefaultClient.Do(req)

		var result map[string]any
		responeBody, _ := ioutil.ReadAll(response.Body)
		json.Unmarshal([]byte(string(responeBody)), &result)
		jiraLink := fmt.Sprint("https://jira.ghtklab.com/browse/", result["key"])
		fmt.Println(jiraLink)

		//Insert in Database
		stmt, _ := db.Prepare("INSERT INTO slow_queries (count, min_response, max_response, avg_response, label, query, task_id, week_id, database_sheet_name, priority, sample_data, J_link) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")
		_, err := stmt.Exec(entry.Count, entry.Min, entry.Max, entry.Avg, entry.Label, entry.Query, entry.TaskId, weekTask, strings.ToUpper(entry.Sheetname), getPriority(entry.Count), strings.Trim(builderQuerySample.String(), " "), jiraLink)
		if err != nil {
			panic(err.Error())
		}

	}
}

func filterRows(rowsExcel []RowExcel) []RowExcel {
	db, _ := sql.Open(os.Getenv("DRIVER_NAME"), os.ExpandEnv("${DB_USERNAME}:${DB_PASSWORD}@tcp($DB_HOST:$DB_PORT)/$DB_NAME"))
	list := []RowExcel{}
	checkQueryInExcelMap := make(map[string]bool)
	for _, entry := range rowsExcel {
		checkQuerry1 := strings.Split(entry.Query, " ")
		builderQuerySample := strings.Builder{}
		for i := 0; i < len(checkQuerry1); i++ {
			builderQuerySample.WriteString(checkQuerry1[i])
			if checkQuerry1[i] == "from" {
				builderQuerySample.WriteString(checkQuerry1[i+1])
				break
			}

		}
		results := db.QueryRow("SELECT count(*) FROM slow_queries WHERE sample_data = ?", strings.Trim(builderQuerySample.String(), " "))
		var rsCount int
		results.Scan(&rsCount)

		checkQuerry2 := strings.Split(entry.Query, ",")
		checkLabel := strings.Split(entry.Label, "_")

		if (checkLabel[0] != "redash" && checkLabel[0] != "dev") &&
			(checkQuerry2[0] != "delete" && checkQuerry2[0] != "truncate") &&
			(entry.Count > 20 || entry.Avg > 10) &&
			(entry.Label != "data_etl[data_etl]") && (entry.Label != "etl[etl]") &&
			(entry.Query != "rollback;" && entry.Query != "commit;") && !checkQueryInExcelMap[strings.Trim(builderQuerySample.String(), " ")] && entry.Query != "" {

			if rsCount == 0 {
				list = append(list, entry)
			} else {
				if rsCount > 0 { //Update newest AVG and Count of Query in Excel
					stmt2, _ := db.Prepare("UPDATE slow_queries SET count = ?, avg_response = ?,min_response = ?, max_response = ? WHERE sample_data = ?")
					_, err2 := stmt2.Exec(entry.Count, entry.Avg, entry.Min, entry.Max, strings.Trim(builderQuerySample.String(), " "))
					if err2 != nil {
						panic(err2.Error())
					}
				}
			}

		}

		if !checkQueryInExcelMap[strings.Trim(builderQuerySample.String(), " ")] {
			checkQueryInExcelMap[strings.Trim(builderQuerySample.String(), " ")] = true
		}
	}
	db.Close()
	return list
}

func getWeeks(sheets []string) int {
	_, week := time.Now().UTC().ISOWeek()
	weekDay := time.Now().Weekday()

	if int(weekDay) > 4 {
		week = week + 2
	}
	if int(weekDay) > 0 && int(weekDay) < 5 {
		week = week + 1
	}
	rows := [][]string{
		{"Summary", "Epic Name", "Reporter", "Assignee", "Issue Type"},
	}

	//SPECIAL
	rows = append(rows, []string{
		"SLQ SPECIAL CS W" + strconv.Itoa(week),
		"SLQ SPECIAL CS W" + strconv.Itoa(week),
		"anbh2", "tinhdd2", "Epic",
	})
	rows = append(rows, []string{
		"SLQ SPECIAL PAYMENT W" + strconv.Itoa(week),
		"SLQ SPECIAL PAYMENT W" + strconv.Itoa(week),
		"anbh2", "tinhdd2", "Epic",
	})
	rows = append(rows, []string{
		"SLQ SPECIAL OS W" + strconv.Itoa(week),
		"SLQ SPECIAL OS W" + strconv.Itoa(week),
		"anbh2", "tinhdd2", "Epic",
	})
	rows = append(rows, []string{
		"SLQ SPECIAL ERP W" + strconv.Itoa(week),
		"SLQ SPECIAL ERP W" + strconv.Itoa(week),
		"anbh2", "tinhdd2", "Epic",
	})

	//Main Excel Sheet
	for ind := 0; ind < len(sheets); ind += 1 {
		rows = append(rows, []string{
			summary[strings.ToUpper(sheets[ind])] + "W" + strconv.Itoa(week),
			summary[strings.ToUpper(sheets[ind])] + "W" + strconv.Itoa(week),
			"anbh2", "tinhdd2", "Epic",
		})
	}

	csvfile, _ := os.Create("epicWeek.csv")
	csvWriter := csv.NewWriter(csvfile)
	for _, row := range rows {
		_ = csvWriter.Write(row)
	}
	csvWriter.Flush()
	csvfile.Close()
	return week
}

func getPrevTaskId(sheetName string) int {
	db, _ := sql.Open(os.Getenv("DRIVER_NAME"), os.ExpandEnv("${DB_USERNAME}:${DB_PASSWORD}@tcp($DB_HOST:$DB_PORT)/$DB_NAME"))
	resultsId := db.QueryRow("SELECT MAX(task_id) FROM slow_queries WHERE database_sheet_name = ?", sheetName)
	var prevTaskId int
	resultsId.Scan(&prevTaskId)
	db.Close()
	return prevTaskId
}

func getPriority(count int) string {
	if count < 50 {
		return "Low"
	} else if count >= 500 {
		return "High"
	}
	return "Medium"
}

func processRowFromExcel(path string) ([]RowExcel, int) {
	finalRows := []RowExcel{}
	f, _ := excelize.OpenFile(path)
	sheets := f.GetSheetList()
	for ind := 0; ind < len(sheets); ind += 1 {
		fmt.Println("Sheetname:", strings.ToUpper(sheets[ind]))
		rowsExcel := []RowExcel{}
		//Get rows from Excel
		rows, _ := f.GetRows(sheets[ind])
		for indRow := 1; indRow < len(rows); indRow += 1 {
			count_excel, _ := strconv.ParseInt(rows[indRow][0], 0, 64)
			label_excel := rows[indRow][1]
			min_excel, _ := strconv.ParseFloat(rows[indRow][2], 64)
			max_excel, _ := strconv.ParseFloat(rows[indRow][3], 64)
			avg_excel, _ := strconv.ParseFloat(rows[indRow][4], 64)
			query_excel := rows[indRow][5]

			rowExe := RowExcel{int(count_excel), label_excel, min_excel, max_excel, avg_excel, query_excel, sheets[ind], 0}
			rowsExcel = append(rowsExcel, rowExe)

		}
		prevTaskId := getPrevTaskId(sheets[ind]) + 1
		filterRows := filterRows(rowsExcel)
		for _, entry := range filterRows {
			entry.TaskId = prevTaskId
			finalRows = append(finalRows, entry)
			prevTaskId += 1
		}
	}
	weekTask := getWeeks(sheets)
	return finalRows, weekTask
}

func getHighPriorityQuery(path string) {
	db, _ := sql.Open(os.Getenv("DRIVER_NAME"), os.ExpandEnv("${DB_USERNAME}:${DB_PASSWORD}@tcp($DB_HOST:$DB_PORT)/$DB_NAME"))
	rowsExcel := []RowExcel{}
	f, _ := excelize.OpenFile(path)
	sheets := f.GetSheetList()
	for ind := 0; ind < len(sheets); ind += 1 {
		checkQueryInExcelMap := make(map[string]bool)
		//Get rows from Excel
		rows, _ := f.GetRows(sheets[ind])
		for indRow := 1; indRow < len(rows); indRow += 1 {
			count_excel, _ := strconv.ParseInt(rows[indRow][0], 0, 64)
			label_excel := rows[indRow][1]
			min_excel, _ := strconv.ParseFloat(rows[indRow][2], 64)
			max_excel, _ := strconv.ParseFloat(rows[indRow][3], 64)
			avg_excel, _ := strconv.ParseFloat(rows[indRow][4], 64)
			query_excel := rows[indRow][5]

			if count_excel > 500 || avg_excel > 200 {
				checkQuerry1 := strings.Split(query_excel, " ")
				builderQuerySample := strings.Builder{}
				for i := 0; i < len(checkQuerry1); i++ {
					builderQuerySample.WriteString(checkQuerry1[i])
					if checkQuerry1[i] == "from" {
						builderQuerySample.WriteString(checkQuerry1[i+1])
						break
					}

				}
				results := db.QueryRow("SELECT task_id, database_sheet_name FROM slow_queries WHERE sample_data = ? LIMIT 1", strings.Trim(builderQuerySample.String(), " "))
				var task_id int
				var sheetName string
				results.Scan(&task_id, &sheetName)
				rowExe := RowExcel{int(count_excel), label_excel, min_excel, max_excel, avg_excel, query_excel, sheets[ind], task_id}
				rowsExcel = append(rowsExcel, rowExe)

				if !checkQueryInExcelMap[strings.Trim(builderQuerySample.String(), " ")] {
					fmt.Println("Jira ID:", task_id, "/", sheetName, "/ COUNT:", count_excel, "/ AVG:", avg_excel)
					checkQueryInExcelMap[strings.Trim(builderQuerySample.String(), " ")] = true
				}
			}

		}
	}
	db.Close()
}
