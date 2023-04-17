package gforce

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	// "bitbucket.org/everymind/evmd-golib/logger"

	uuid "github.com/satori/go.uuid"
	"github.com/spf13/cast"
)

type queryType int

const (
	enumNil queryType = iota
	enumQuery
	enumQueryAll
	enumTooling
)

const stringNil = ""

// Count func
func (f *Force) Count(sobject string) (result int, err error) {
	var res ForceQueryResult

	url := fmt.Sprintf("%s/services/data/%s/queryAll?q=SELECT+COUNT()+FROM+%s", f.Credentials.InstanceUrl, apiVersion, sobject)
	body, err := f.httpGet(url, false)
	if err != nil {
		return
	}

	json.Unmarshal(body, &res)
	result = res.TotalSize

	return
}

// GetIDs func
func (f *Force) GetIDs(sobject string, pkField string, where map[string]interface{}, limit, offset int) (result []string, totalSize int, err error) {
	query := writeQuery(sobject, []string{pkField}, where, limit, offset)

	res, err := f.exec(query, stringNil, enumQueryAll)
	if err != nil {
		return
	}

	for _, record := range res.Records {
		result = append(result, cast.ToString(record[pkField]))
	}

	totalSize = res.TotalSize

	return
}

func (f *Force) GetIDsStream(sobject string, pkField string, where map[string]interface{}, limit, offset int) (files []string, err error) {
	query := writeQuery(sobject, []string{pkField}, where, limit, offset)

	filename, nextRecordsURL, err := f.execStream(query, stringNil, sobject, enumQueryAll)
	if err != nil {
		return nil, fmt.Errorf("f.execStream(): %w", err)
	}
	files = append(files, filename)
	for len(nextRecordsURL) > 0 {
		filename, nextRecordsURL, err = f.execStream("", nextRecordsURL, sobject, enumNil)
		if err != nil {
			return nil, fmt.Errorf("f.execStream(): %w", err)
		}
		files = append(files, filename)
	}

	return
}

// Select func
func (f *Force) Select(sobject string, fields []string, where map[string]interface{}, limit, offset int, customQuery string) (results []ForceRecord, totalSize int, err error) {

	var query string
	if len(customQuery) > 0 {
		query = customQuery
	} else {
		query = writeQuery(sobject, fields, where, limit, offset)
	}

	res, err := f.exec(query, stringNil, enumQueryAll)
	if err != nil {
		log.Printf("Error f.exec(): %w", err)
		return
	}

	results = res.Records
	totalSize = res.TotalSize

	return
}

// SelectByID func
func (f *Force) SelectByID(sobject string, fields []string, id string) (result ForceRecord, totalSize int, err error) {
	r, t, err := f.Select(sobject, fields, map[string]interface{}{"Id": id}, 1, 0, "")
	if err != nil {
		return
	}

	if len(r) > 0 {
		result = r[0]
	}

	return result, t, err
}

// Tooling func
func (f *Force) Tooling(sobject string, fields []string, where map[string]interface{}, limit, offset int) (results []ForceRecord, totalSize int, err error) {
	query := writeQuery(sobject, fields, where, limit, offset)

	res, err := f.exec(query, stringNil, enumTooling)
	if err != nil {
		return
	}

	results = res.Records
	totalSize = res.TotalSize

	return
}

func (f *Force) exec(query, nextRecordsURL string, qType queryType) (results ForceQueryResult, err error) {
	var res ForceQueryResult
	var q string

	switch qType {
	case enumQuery:
		q = fmt.Sprintf("/services/data/%s/query?q=%s", apiVersion, query)
	case enumQueryAll:
		q = fmt.Sprintf("/services/data/%s/queryAll?q=%s", apiVersion, query)
	case enumTooling:
		q = fmt.Sprintf("/services/data/%s/tooling/query?q=%s", apiVersion, query)
	default:
		q = fmt.Sprintf("%s", nextRecordsURL)
	}

	url := fmt.Sprintf("%s%s", f.Credentials.InstanceUrl, q)

	body, err := f.httpGet(url, false)
	if err != nil {
		log.Printf("Error body f.exec(): %w", err)
		return
	}

	err = json.Unmarshal(body, &res)
	if err != nil {
		// logger.Debugf("Error on use new json library: %v", err)
		return
	}
	results = res

	if len(res.NextRecordsUrl) > 0 {
		nextResults, e := f.exec("", res.NextRecordsUrl, enumNil)
		if e != nil {
			log.Printf("Error nextResults f.exec(): %w", e)
			err = e
		}
		results.Records = append(results.Records, nextResults.Records...)
	}

	return
}

func (f *Force) execStream(query, nextRecordsURL, object string, qType queryType) (filename string, nextRecords string, err error) {
	var res ForceQueryResult
	var q string

	switch qType {
	case enumQuery:
		q = fmt.Sprintf("/services/data/%s/query?q=%s", apiVersion, query)
	case enumQueryAll:
		q = fmt.Sprintf("/services/data/%s/queryAll?q=%s", apiVersion, query)
	case enumTooling:
		q = fmt.Sprintf("/services/data/%s/tooling/query?q=%s", apiVersion, query)
	default:
		q = nextRecordsURL
	}

	url := fmt.Sprintf("%s%s", f.Credentials.InstanceUrl, q)

	resp, err := f.httpGetStream(url, false)
	if err != nil {
		return "", "", fmt.Errorf("f.httpGetStream(): %w", err)
	}
	// logger.Debugf("Exec request to => %v", url)

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", "", fmt.Errorf("ioutil.ReadAll(): %w", err)
	}
	defer resp.Body.Close()
	err = json.Unmarshal(body, &res)

	id := uuid.NewV4()
	filename = fmt.Sprintf("/tmp/%s_%s.json", object, id.String())
	out, err := os.Create(filename)
	if err != nil {
		return "", "", fmt.Errorf("os.Create(): %w", err)
	}
	defer out.Close()

	_, err = io.Copy(out, ioutil.NopCloser(bytes.NewReader(body)))
	if err != nil {
		return "", "", fmt.Errorf("io.Copy(): %w", err)
	}

	nextRecords = res.NextRecordsUrl

	return
}

func writeQuery(sobject string, fields []string, where map[string]interface{}, limit, offset int) string {
	var query strings.Builder

	query.WriteString("SELECT+")
	query.WriteString(strings.Join(fields, ","))
	query.WriteString("+FROM+")
	query.WriteString(sobject)

	if len(where) > 0 {
		query.WriteString("+WHERE+")

		addAnd := func(i int, k string) {
			if i > 0 {
				query.WriteString("+AND+")
			}
			query.WriteString(k)
		}

		var i int = 0
		for k, v := range where {
			switch t := v.(type) {
			case string:
				{
					addAnd(i, k)
					query.WriteString("='")
					query.WriteString(strings.ReplaceAll(t, "'", ""))
					query.WriteString("'")
				}
			case []string:
				{
					addAnd(i, k)
					query.WriteString("+IN+(")
					for index, item := range t {
						query.WriteString("'")
						query.WriteString(strings.ReplaceAll(item, "'", ""))
						query.WriteString("'")
						if len(t) > 1 && index < len(t)-1 {
							query.WriteString(",")
						}
					}
					query.WriteString(")")
				}
			case bool:
				{
					addAnd(i, k)
					query.WriteString("=")
					query.WriteString(strconv.FormatBool(t))
				}
			case int, int8, int16, int32, int64:
				{
					addAnd(i, k)
					query.WriteString("=")
					query.WriteString(strconv.FormatInt(t.(int64), 10))
				}
			case []int, []int8, []int16, []int32, []int64:
				{
					addAnd(i, k)
					query.WriteString("+IN+(")
					slice := t.([]int64)
					for index, item := range slice {
						query.WriteString(strconv.FormatInt(item, 10))
						if len(slice) > 1 && index < len(slice)-1 {
							query.WriteString(",")
						}
					}
					query.WriteString(")")
				}
			case float32, float64:
				{
					addAnd(i, k)
					query.WriteString("=")
					query.WriteString(strconv.FormatFloat(t.(float64), 'f', -1, 64))
				}
			case []float32, []float64:
				{
					addAnd(i, k)
					query.WriteString("+IN+(")
					slice := t.([]float64)
					for index, item := range slice {
						query.WriteString(strconv.FormatFloat(item, 'f', -1, 64))
						if len(slice) > 1 && index < len(slice)-1 {
							query.WriteString(",")
						}
					}
					query.WriteString(")")
				}
			case time.Time:
				{
					addAnd(i, k)
					query.WriteString("='")
					query.WriteString(t.Format(time.RFC3339))
					query.WriteString("'")
				}
			default:
			}

			i++
		}
	}

	if limit > 0 {
		query.WriteString("+LIMIT+")
		query.WriteString(strconv.Itoa(limit))
	}

	if offset > 0 {
		query.WriteString("+OFFSET+")
		query.WriteString(strconv.Itoa(offset))
	}

	return query.String()
}
