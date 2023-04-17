package gforce

import (
	"encoding/xml"
	"errors"
	"fmt"
	"time"
)

type ForceConnectedApps []ForceConnectedApp

type ForceConnectedApp struct {
	Name string `xml:"fullName"`
	Id   string `xml:"id"`
	Type string `xml:"type"`
}

type ComponentFailure struct {
	Changed     bool   `xml:"changed"`
	Created     bool   `xml:"created"`
	Deleted     bool   `xml:"deleted"`
	FileName    string `xml:"fileName"`
	FullName    string `xml:"fullName"`
	LineNumber  int    `xml:"lineNumber"`
	Problem     string `xml:"problem"`
	ProblemType string `xml:"problemType"`
	Success     bool   `xml:"success"`
}

type ComponentSuccess struct {
	Changed  bool   `xml:"changed"`
	Created  bool   `xml:"created"`
	Deleted  bool   `xml:"deleted"`
	FileName string `xml:"fileName"`
	FullName string `xml:"fullName"`
	Id       string `xml:"id"`
	Success  bool   `xml:"success"`
}

type TestFailure struct {
	Message    string  `xml:"message"`
	Name       string  `xml:"name"`
	MethodName string  `xml:"methodName"`
	StackTrace string  `xml:"stackTrace"`
	Time       float32 `xml:"time"`
}

type TestSuccess struct {
	Name       string  `xml:"name"`
	MethodName string  `xml:"methodName"`
	Time       float32 `xml:"time"`
}

type CodeCoverageWarning struct {
	Name    string `xml:"name"`
	Message string `xml:"message"`
}

type RunTestResult struct {
	NumberOfFailures     int                   `xml:"numFailures"`
	NumberOfTestsRun     int                   `xml:"numTestsRun"`
	TotalTime            float32               `xml:"totalTime"`
	TestFailures         []TestFailure         `xml:"failures"`
	TestSuccesses        []TestSuccess         `xml:"successes"`
	CodeCoverageWarnings []CodeCoverageWarning `xml:"codeCoverageWarnings"`
}

type ComponentDetails struct {
	ComponentSuccesses []ComponentSuccess `xml:"componentSuccesses"`
	ComponentFailures  []ComponentFailure `xml:"componentFailures"`
	RunTestResult      RunTestResult      `xml:"runTestResult"`
}

type ForceCheckDeploymentStatusResult struct {
	CheckOnly                bool             `xml:"checkOnly"`
	CompletedDate            time.Time        `xml:"completedDate"`
	CreatedDate              time.Time        `xml:"createdDate"`
	Details                  ComponentDetails `xml:"details"`
	Done                     bool             `xml:"done"`
	Id                       string           `xml:"id"`
	NumberComponentErrors    int              `xml:"numberComponentErrors"`
	NumberComponentsDeployed int              `xml:"numberComponentsDeployed"`
	NumberComponentsTotal    int              `xml:"numberComponentsTotal"`
	NumberTestErrors         int              `xml:"numberTestErrors"`
	NumberTestsCompleted     int              `xml:"numberTestsCompleted"`
	NumberTestsTotal         int              `xml:"numberTestsTotal"`
	RollbackOnError          bool             `xml:"rollbackOnError"`
	Status                   string           `xml:"status"`
	StateDetail              string           `xml:"stateDetail"`
	Success                  bool             `xml:"success"`
}

type ForceMetadata struct {
	ApiVersion string
	Force      *Force
}

/* These structs define which options are available and which are
   required for the various field types you can create. Reflection
   is used to leverage these structs in validating options when creating
   a custom field.
*/

type DescribeMetadataObject struct {
	ChildXmlNames []string `xml:"childXmlNames"`
	DirectoryName string   `xml:"directoryName"`
	InFolder      bool     `xml:"inFolder"`
	MetaFile      bool     `xml:"metaFile"`
	Suffix        string   `xml:"suffix"`
	XmlName       string   `xml:"xmlName"`
}

type MetadataDescribeResult struct {
	NamespacePrefix    string                   `xml:"organizationNamespace"`
	PartialSaveAllowed bool                     `xml:"partialSaveAllowed"`
	TestRequired       bool                     `xml:"testRequired"`
	MetadataObjects    []DescribeMetadataObject `xml:"metadataObjects"`
}

var (
	metadataType string
)

func NewForceMetadata(force *Force) (fm *ForceMetadata) {
	fm = &ForceMetadata{ApiVersion: apiVersionNumber, Force: force}
	return
}

func (fm *ForceMetadata) CheckStatus(id string) (err error) {
	body, err := fm.soapExecute("checkStatus", fmt.Sprintf("<id>%s</id>", id))
	if err != nil {
		return
	}
	var status struct {
		Done    bool   `xml:"Body>checkStatusResponse>result>done"`
		State   string `xml:"Body>checkStatusResponse>result>state"`
		Message string `xml:"Body>checkStatusResponse>result>message"`
	}
	if err = xml.Unmarshal(body, &status); err != nil {
		return
	}
	switch {
	case !status.Done:
		fmt.Printf("Not done yet: %s  Will check again in five seconds.\n", status.State)
		//fmt.Printf("ID: %s State: %s - message: %s\n", id, status.State, status.Message)
		time.Sleep(5000 * time.Millisecond)
		return fm.CheckStatus(id)
	case status.State == "Error":
		return errors.New(status.Message)
	}
	return
}

func (results ForceCheckDeploymentStatusResult) String() string {
	complete := ""
	if results.Status == "InProgress" {
		complete = fmt.Sprintf(" (%d/%d)", results.NumberComponentsDeployed, results.NumberComponentsTotal)
	}
	if results.NumberTestsCompleted > 0 {
		complete = fmt.Sprintf(" (%d/%d)", results.NumberTestsCompleted, results.NumberTestsTotal)
	}

	return fmt.Sprintf("Status: %s%s %s", results.Status, complete, results.StateDetail)
}

func (fm *ForceMetadata) DescribeMetadata() (describe MetadataDescribeResult, err error) {
	body, err := fm.soapExecute("describeMetadata", fmt.Sprintf("<apiVersion>%s</apiVersion>", apiVersionNumber))
	if err != nil {
		return
	}
	var result struct {
		Data MetadataDescribeResult `xml:"Body>describeMetadataResponse>result"`
	}

	err = xml.Unmarshal([]byte(body), &result)

	if err != nil {
		fmt.Println(err.Error())
	} else {
		describe = result.Data
	}
	//fm.DescribeMetadataValue("{http://soap.sforce.com/2006/04/metadata}EmailTemplate")
	return
}

func (fm *ForceMetadata) soapExecute(action, query string) (response []byte, err error) {
	url := fmt.Sprintf("%s/services/Soap/m/%s", fm.Force.Credentials.InstanceUrl, fm.ApiVersion)
	soap := NewSoap(url, "http://soap.sforce.com/2006/04/metadata", fm.Force.Credentials.AccessToken)
	response, err = soap.Execute(action, query)
	if err == SessionExpiredError {
		fm.Force.RefreshSession()
		return fm.soapExecute(action, query)
	}
	return
}
