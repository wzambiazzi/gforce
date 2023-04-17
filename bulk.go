package gforce

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
)

const (
	BatchStateQueued       string = "Queued"
	BatchStateInProgress   string = "InProgress"
	BatchStateCompleted    string = "Completed"
	BatchStateFailed       string = "Failed"
	BatchStateNotProcessed string = "Not Processed"
)

type BatchResult struct {
	Results []string
}

type BatchInfo struct {
	Id                      string      `xml:"id,omitempty" json:"id,omitempty"`
	JobId                   string      `xml:"jobId,omitempty" json:"jobId,omitempty"`
	State                   string      `xml:"state,omitempty" json:"state,omitempty"`
	StateMessage            interface{} `xml:"stateMessage,omitempty" json:"stateMessage,omitempty"`
	CreatedDate             string      `xml:"createdDate,omitempty" json:"createdDate,omitempty"`
	NumberRecordsProcessed  int64       `xml:"numberRecordsProcessed,omitempty" json:"numberRecordsProcessed,omitempty"`
	NumberRecordsFailed     int64       `xml:"numberRecordsFailed,omitempty" json:"numberRecordsFailed,omitempty"`
	SystemModstamp          string      `xml:"systemModstamp,omitempty" json:"systemModstamp,omitempty"`
	TotalProcessingTime     int64       `xml:"totalProcessingTime,omitempty" json:"totalProcessingTime,omitempty"`
	ApexProcessingTime      int64       `xml:"apexProcessingTime,omitempty" json:"apexProcessingTime,omitempty"`
	ApiActiveProcessingTime int64       `xml:"apiActiveProcessingTime,omitempty" json:"apiActiveProcessingTime,omitempty"`
}

type JobInfo struct {
	XMLName                 xml.Name `xml:"http://www.force.com/2009/06/asyncapi/dataload jobInfo"`
	Id                      string   `xml:"id,omitempty"`
	Operation               string   `xml:"operation,omitempty"`
	Object                  string   `xml:"object,omitempty"`
	ExternalIdFieldName     string   `xml:"externalIdFieldName,omitempty"`
	CreatedById             string   `xml:"createdById,omitempty"`
	CreatedDate             string   `xml:"createdDate,omitempty"`
	SystemModStamp          string   `xml:"systemModstamp,omitempty"`
	State                   string   `xml:"state,omitempty"`
	ConcurrencyMode         string   `xml:"concurrencyMode,omitempty"`
	ContentType             string   `xml:"contentType,omitempty"`
	NumberBatchesQueued     int      `xml:"numberBatchesQueued,omitempty"`
	NumberBatchesInProgress int      `xml:"numberBatchesInProgress,omitempty"`
	NumberBatchesCompleted  int      `xml:"numberBatchesCompleted,omitempty"`
	NumberBatchesFailed     int      `xml:"numberBatchesFailed,omitempty"`
	NumberBatchesTotal      int      `xml:"numberBatchesTotal,omitempty"`
	NumberRecordsProcessed  int      `xml:"numberRecordsProcessed,omitempty"`
	NumberRetries           int      `xml:"numberRetries,omitempty"`
	ApiVersion              string   `xml:"apiVersion,omitempty"`
	NumberRecordsFailed     int      `xml:"numberRecordsFailed,omitempty"`
	TotalProcessingTime     int      `xml:"totalProcessingTime,omitempty"`
	ApiActiveProcessingTime int      `xml:"apiActiveProcessingTime,omitempty"`
	ApexProcessingTime      int      `xml:"apexProcessingTime,omitempty"`
}

type JobInfoV2 struct {
	ID                     string      `json:"id,omitempty"`
	ApiVersion             float32     `json:"apiVersion,omitempty"`
	ColumnDelimiter        string      `json:"columnDelimiter,omitempty"`
	ConcurrencyMode        string      `json:"concurrencyMode,omitempty"`
	ContentType            string      `json:"contentType,omitempty"`
	ContentUrl             string      `json:"contentUrl,omitempty"`
	CreatedById            string      `json:"createdById,omitempty"`
	CreatedDate            string      `json:"createdDate,omitempty"`
	ErrorMessage           string      `json:"errorMessage,omitempty"`
	ExternalIdFieldName    string      `json:"externalIdFieldName,omitempty"`
	JobType                string      `json:"jobType,omitempty"`
	LineEnding             string      `json:"lineEnding,omitempty"`
	NumberRecordsProcessed int64       `json:"numberRecordsProcessed,omitempty"`
	NumberRecordsFailed    int64       `json:"numberRecordsFailed,omitempty"`
	Object                 string      `json:"object,omitempty"`
	Operation              string      `json:"operation,omitempty"`
	State                  string      `json:"state,omitempty"`
	StateMessage           interface{} `json:"stateMessage,omitempty"`
	SystemModStamp         string      `json:"systemModstamp,omitempty"`
}

var InvalidBulkObject = errors.New("Object Does Not Support Bulk API")

func (f *Force) CreateBulkJob(jobInfo JobInfo) (result JobInfo, err error) {
	xmlbody, err := xml.Marshal(jobInfo)
	if err != nil {
		err = fmt.Errorf("Could not create job request: %w", err)
		return
	}
	url := fmt.Sprintf("%s/services/async/%s/job", f.Credentials.InstanceUrl, apiVersionNumber)
	body, err := f.httpPostXML(url, string(xmlbody), false)
	xml.Unmarshal(body, &result)
	if len(result.Id) == 0 {
		var fault LoginFault
		xml.Unmarshal(body, &fault)
		if fault.ExceptionCode == "InvalidEntity" {
			err = InvalidBulkObject
		} else {
			err = fmt.Errorf("%s: %s", fault.ExceptionCode, fault.ExceptionMessage)
		}
	}
	return
}

func (f *Force) CreateBulkJobV2(jobInfo JobInfoV2) (result JobInfoV2, err error) {
	jsonbody, err := json.Marshal(jobInfo)
	if err != nil {
		err = fmt.Errorf("Could not create job request: %w", err)
		return
	}

	url := fmt.Sprintf("%s/services/data/%s/jobs/ingest", f.Credentials.InstanceUrl, apiVersion)

	body, err := f.httpPostJSON(url, string(jsonbody), false)
	if err != nil {
		return
	}

	json.Unmarshal(body, &result)
	if len(result.ID) == 0 {
		var fault LoginFault
		json.Unmarshal(body, &fault)
		if fault.ExceptionCode == "InvalidEntity" {
			err = InvalidBulkObject
		} else {
			err = fmt.Errorf("%s: %s", fault.ExceptionCode, fault.ExceptionMessage)
		}
	}

	return
}

func (f *Force) CloseBulkJob(jobId string) (result JobInfo, err error) {
	jobInfo := JobInfo{
		State: "Closed",
	}
	xmlbody, _ := xml.Marshal(jobInfo)
	url := fmt.Sprintf("%s/services/async/%s/job/%s", f.Credentials.InstanceUrl, apiVersionNumber, jobId)
	body, err := f.httpPostXML(url, string(xmlbody), false)
	xml.Unmarshal(body, &result)
	if len(result.Id) == 0 {
		var fault LoginFault
		xml.Unmarshal(body, &fault)
		err = fmt.Errorf("%s: %s", fault.ExceptionCode, fault.ExceptionMessage)
	}
	return
}

func (f *Force) CloseBulkJobV2(jobId string) (result JobInfoV2, err error) {
	url := fmt.Sprintf("%s/services/data/%s/jobs/ingest/%s", f.Credentials.InstanceUrl, apiVersion, jobId)
	jsonbody, _ := json.Marshal(JobInfoV2{State: "UploadComplete"})
	body, err := f.httpPatchJSON(url, string(jsonbody), false)
	if err != nil {
		return
	}
	json.Unmarshal(body, &result)
	if len(result.ID) == 0 {
		var fault LoginFault
		json.Unmarshal(body, &fault)
		err = fmt.Errorf("%s: %s", fault.ExceptionCode, fault.ExceptionMessage)
	}
	return
}

func (f *Force) AbortBulkJob(jobId string) (result JobInfo, err error) {
	jobInfo := JobInfo{
		State: "Aborted",
	}
	xmlbody, _ := xml.Marshal(jobInfo)
	url := fmt.Sprintf("%s/services/async/%s/job/%s", f.Credentials.InstanceUrl, apiVersionNumber, jobId)
	body, err := f.httpPostXML(url, string(xmlbody), false)
	xml.Unmarshal(body, &result)
	if len(result.Id) == 0 {
		var fault LoginFault
		xml.Unmarshal(body, &fault)
		err = fmt.Errorf("%s: %s", fault.ExceptionCode, fault.ExceptionMessage)
	}
	return
}

func (f *Force) AbortBulkJobV2(jobId string) (result JobInfoV2, err error) {
	url := fmt.Sprintf("%s/services/data/%s/jobs/ingest/%s", f.Credentials.InstanceUrl, apiVersion, jobId)
	jsonbody, _ := json.Marshal(JobInfoV2{State: "Aborted"})
	body, err := f.httpPatchJSON(url, string(jsonbody), false)
	if err != nil {
		return
	}
	json.Unmarshal(body, &result)
	if len(result.ID) == 0 {
		var fault LoginFault
		json.Unmarshal(body, &fault)
		err = fmt.Errorf("%s: %s", fault.ExceptionCode, fault.ExceptionMessage)
	}
	return
}

func (f *Force) GetBulkJobs() (result []JobInfo, err error) {
	url := fmt.Sprintf("%s/services/async/%s/jobs", f.Credentials.InstanceUrl, apiVersionNumber)
	body, err := f.httpGetBulk(url, false)
	xml.Unmarshal(body, &result)
	if len(result[0].Id) == 0 {
		var fault LoginFault
		xml.Unmarshal(body, &fault)
		err = fmt.Errorf("%s: %s", fault.ExceptionCode, fault.ExceptionMessage)
	}
	return
}

func (f *Force) GetBulkJobsV2() (result []JobInfoV2, err error) {
	url := fmt.Sprintf("%s/services/data/%s/jobs/ingest", f.Credentials.InstanceUrl, apiVersion)

	body, err := f.httpGet(url, false)
	if err != nil {
		return
	}

	var jobs struct {
		Done           bool        `json:"done"`
		Records        []JobInfoV2 `json:"records"`
		NextRecordsUrl string      `json:"nextRecordsUrl"`
	}
	fmt.Println(string(body))

	err = json.Unmarshal(body, &jobs)
	if err != nil {
		return
	}

	if jobs.Records == nil {
		var fault LoginFault
		xml.Unmarshal(body, &fault)
		err = fmt.Errorf("%s: %s", fault.ExceptionCode, fault.ExceptionMessage)
	}
	result = jobs.Records

	return result, err
}

func (f *Force) BulkQuery(soql string, jobId string, contenttype string) (result BatchInfo, err error) {
	url := fmt.Sprintf("%s/services/async/%s/job/%s/batch", f.Credentials.InstanceUrl, apiVersionNumber, jobId)
	var body []byte

	switch contenttype {
	case "CSV":
		body, err = f.httpPostCSV(url, soql, false)
		xml.Unmarshal(body, &result)
	case "JSON":
		body, err = f.httpPostJSON(url, soql, false)
		json.Unmarshal(body, &result)
	default:
		body, err = f.httpPostXML(url, soql, false)
		xml.Unmarshal(body, &result)
	}

	if err != nil {
		return
	}

	if len(result.Id) == 0 {
		var fault LoginFault
		if contenttype == "JSON" {
			json.Unmarshal(body, &fault)
		} else {
			xml.Unmarshal(body, &fault)
		}
		err = fmt.Errorf("%s: %s", fault.ExceptionCode, fault.ExceptionMessage)
	}

	return
}

func (f *Force) AddBatchToJob(content string, job JobInfo) (result BatchInfo, err error) {
	switch job.ContentType {
	case "CSV":
		return f.addCSVBatchToJob(content, job)
	case "JSON":
		return f.addJSONBatchToJob(content, job)
	case "XML":
		return f.addXMLBatchToJob(content, job)
	default:
		err = fmt.Errorf("Invalid content type for bulk API: %s", job.ContentType)
	}
	return
}

func (f *Force) AddBatchToJobV2(job JobInfoV2, content string) (result BatchInfo, err error) {
	switch job.ContentType {
	case "CSV":
		return f.addCSVBatchToJobV2(content, job)
	case "JSON":
		return f.addJSONBatchToJobV2(content, job)
	case "XML":
		return f.addXMLBatchToJobV2(content, job)
	default:
		err = fmt.Errorf("Invalid content type for bulk API: %s", job.ContentType)
	}
	return
}

func (f *Force) GetBatchInfo(jobId string, batchId string, contenttype string) (result BatchInfo, err error) {
	var body []byte
	url := fmt.Sprintf("%s/services/async/%s/job/%s/batch/%s", f.Credentials.InstanceUrl, apiVersionNumber, jobId, batchId)

	switch contenttype {
	case "JSON":
		body, err = f.httpGetBulkJSON(url, false)
		if err != nil {
			return
		}
		err = json.Unmarshal(body, &result)
		if len(result.Id) == 0 {
			var fault LoginFault
			json.Unmarshal(body, &fault)
			err = fmt.Errorf("%s: %s", fault.ExceptionCode, fault.ExceptionMessage)
		}
	default:
		body, err = f.httpGetBulk(url, false)
		if err != nil {
			return
		}
		err = xml.Unmarshal(body, &result)
		if len(result.Id) == 0 {
			var fault LoginFault
			xml.Unmarshal(body, &fault)
			err = fmt.Errorf("%s: %s", fault.ExceptionCode, fault.ExceptionMessage)
		}
	}
	return
}

func (f *Force) GetBatches(jobId string) (result []BatchInfo, err error) {
	url := fmt.Sprintf("%s/services/async/%s/job/%s/batch", f.Credentials.InstanceUrl, apiVersionNumber, jobId)
	body, err := f.httpGetBulk(url, false)

	var batchInfoList struct {
		BatchInfos []BatchInfo `xml:"batchInfo" json:"batchInfo"`
	}

	xml.Unmarshal(body, &batchInfoList)
	result = batchInfoList.BatchInfos
	if len(result) == 0 {
		var fault LoginFault
		xml.Unmarshal(body, &fault)
		err = fmt.Errorf("%s: %s", fault.ExceptionCode, fault.ExceptionMessage)
	}
	return
}

func (f *Force) GetJobInfo(jobId string) (result JobInfo, err error) {
	url := fmt.Sprintf("%s/services/async/%s/job/%s", f.Credentials.InstanceUrl, apiVersionNumber, jobId)
	body, err := f.httpGetBulk(url, false)
	xml.Unmarshal(body, &result)
	if len(result.Id) == 0 {
		var fault LoginFault
		xml.Unmarshal(body, &fault)
		err = fmt.Errorf("%s: %s", fault.ExceptionCode, fault.ExceptionMessage)
	}
	return
}

func (f *Force) GetJobInfoV2(jobId string) (result JobInfoV2, err error) {
	url := fmt.Sprintf("%s/services/data/%s/jobs/ingest/%s", f.Credentials.InstanceUrl, apiVersion, jobId)
	body, err := f.httpGetBulkJSON(url, false)
	if err != nil {
		return
	}
	err = json.Unmarshal(body, &result)
	if len(result.ID) == 0 {
		var fault LoginFault
		json.Unmarshal(body, &fault)
		err = fmt.Errorf("%s: %s", fault.ExceptionCode, fault.ExceptionMessage)
	}
	return
}

func (f *Force) RetrieveBulkQuery(jobId string, batchId string) (result []byte, err error) {
	url := fmt.Sprintf("%s/services/async/%s/job/%s/batch/%s/result", f.Credentials.InstanceUrl, apiVersionNumber, jobId, batchId)
	result, err = f.httpGetBulk(url, false)
	return
}

func (f *Force) RetrieveBulkQueryResults(jobId string, batchId string, resultId string) (result []byte, err error) {
	url := fmt.Sprintf("%s/services/async/%s/job/%s/batch/%s/result/%s", f.Credentials.InstanceUrl, apiVersionNumber, jobId, batchId, resultId)
	result, err = f.httpGetBulk(url, false)
	return
}

func (f *Force) RetrieveBulkJobQueryResults(job JobInfo, batchId string, resultId string) ([]byte, error) {
	url := fmt.Sprintf("%s/services/async/%s/job/%s/batch/%s/result/%s", f.Credentials.InstanceUrl, apiVersionNumber, job.Id, batchId, resultId)
	return f.retrieveBulkResult(url, job.ContentType)
}

func (f *Force) RetrieveBulkResultStream(job JobInfo, batchId string, resultId string) (*http.Response, error) {
	url := fmt.Sprintf("%s/services/async/%s/job/%s/batch/%s/result/%s", f.Credentials.InstanceUrl, apiVersionNumber, job.Id, batchId, resultId)
	return f.retrieveBulkStream(url, job.ContentType)
}

func (f *Force) RetrieveBulkBatchResults(job JobInfo, batchId string) (results []string, err error) {
	url := fmt.Sprintf("%s/services/async/%s/job/%s/batch/%s/result", f.Credentials.InstanceUrl, apiVersionNumber, job.Id, batchId)
	data, err := f.httpGetBulkJSON(url, false)
	if err == nil {
		err = json.Unmarshal(data, &results)
	}
	return
}

func (f *Force) addCSVBatchToJob(content string, job JobInfo) (result BatchInfo, err error) {
	url := fmt.Sprintf("%s/services/data/%s/jobs/ingest/%s/batches", f.Credentials.InstanceUrl, apiVersion, job.Id)
	body, err := f.httpPostCSV(url, content, false)
	if err != nil {
		err = fmt.Errorf("Failed to add batch: " + err.Error())
		return
	}
	err = xml.Unmarshal(body, &result)
	if len(result.Id) == 0 {
		var fault LoginFault
		xml.Unmarshal(body, &fault)
		err = fmt.Errorf("%s: %s", fault.ExceptionCode, fault.ExceptionMessage)
	}
	return
}

func (f *Force) addCSVBatchToJobV2(content string, job JobInfoV2) (result BatchInfo, err error) {
	url := fmt.Sprintf("%s/services/data/%s/jobs/ingest/%s/batches", f.Credentials.InstanceUrl, apiVersion, job.ID)
	body, err := f.httpPutCSV(url, content, false)
	if err != nil {
		err = fmt.Errorf("Failed to add batch: " + err.Error())
		return
	}
	json.Unmarshal(body, &result)
	return
}

func (f *Force) addXMLBatchToJob(content string, job JobInfo) (result BatchInfo, err error) {
	url := fmt.Sprintf("%s/services/data/%s/jobs/ingest/%s/batches", f.Credentials.InstanceUrl, apiVersion, job.Id)
	body, err := f.httpPostXML(url, content, false)
	if err != nil {
		err = fmt.Errorf("Failed to add batch: " + err.Error())
		return
	}
	err = xml.Unmarshal(body, &result)
	if len(result.Id) == 0 {
		var fault LoginFault
		xml.Unmarshal(body, &fault)
		err = fmt.Errorf("%s: %s", fault.ExceptionCode, fault.ExceptionMessage)
	}
	return
}

func (f *Force) addXMLBatchToJobV2(content string, job JobInfoV2) (result BatchInfo, err error) {
	url := fmt.Sprintf("%s/services/data/%s/jobs/ingest/%s/batches", f.Credentials.InstanceUrl, apiVersion, job.ID)
	body, err := f.httpPutXML(url, content, false)
	if err != nil {
		err = fmt.Errorf("Failed to add batch: " + err.Error())
		return
	}
	json.Unmarshal(body, &result)
	return
}

func (f *Force) addJSONBatchToJob(content string, job JobInfo) (result BatchInfo, err error) {
	url := fmt.Sprintf("%s/services/data/%s/jobs/ingest/%s/batches", f.Credentials.InstanceUrl, apiVersion, job.Id)
	body, err := f.httpPostJSON(url, content, false)
	if err != nil {
		err = fmt.Errorf("Failed to add batch: " + err.Error())
		return
	}
	err = json.Unmarshal(body, &result)
	return
}

func (f *Force) addJSONBatchToJobV2(content string, job JobInfoV2) (result BatchInfo, err error) {
	url := fmt.Sprintf("%s/services/data/%s/jobs/ingest/%s/batches", f.Credentials.InstanceUrl, apiVersion, job.ID)
	body, err := f.httpPutJSON(url, content, false)
	if err != nil {
		err = fmt.Errorf("Failed to add batch: %w", err)
		return
	}
	json.Unmarshal(body, &result)
	return
}

func (f *Force) retrieveBulkResult(url string, contentType string) (result []byte, err error) {
	switch contentType {
	case "JSON":
		return f.httpGetBulkJSON(url, false)
	case "CSV":
		fallthrough
	case "XML":
		return f.httpGetBulk(url, false)
	default:
		err = fmt.Errorf("Invalid content type for bulk API: %s", contentType)
	}
	return nil, err
}

func (f *Force) retrieveBulkStream(url string, contentType string) (*http.Response, error) {
	var err error

	switch contentType {
	case "JSON":
		return f.httpGetBulkJSONStream(url, false)
	case "CSV":
		fallthrough
	case "XML":
		return f.httpGetBulkStream(url, false)
	default:
		err = fmt.Errorf("Invalid content type for bulk API: %s", contentType)
	}

	return nil, err
}
