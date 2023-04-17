package gforce

import (
	"container/list"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/spf13/cast"
)

// CONFIGS
var (
	ClientId       string = "3MVG9QBLg8QGkFeoxS81gotuAQnbfy4bLU7HErU2yl5I8hFgEP42qQHtJRZelb_rogsKrSfLz_gE5uFRquYwf"
	Timeout        int64  = 0
	CustomEndpoint string = ""
)

// ERRORS
var (
	SalesforceError                    = errors.New("Salesforce internal error")
	SessionExpiredError                = errors.New("Session expired")
	SessionFailRefresh                 = errors.New("Failed to refresh session")
	DeleteRecordResourceNotExistsError = errors.New("The requested resource does not exist")
	EntityIsDeleted                    = errors.New("Entity is deleted")
)

var (
	traceHTTPRequest       bool
	traceHTTPRequestDetail bool
)

const (
	EndpointProduction ForceEndpoint = iota
	EndpointTest
	EndpointPrerelease
	EndpointMobile1
	EndpointCustom
	EndpointInstace
)

const (
	RefreshUnavailable RefreshMethod = iota
	RefreshOauth
)

type ForceEndpoint int

type RefreshMethod int

type ForceSobjectFields []interface{}

type Force struct {
	Credentials *ForceSession
	Metadata    *ForceMetadata
	Partner     *ForcePartner
}

type UserInfo struct {
	UserName     string `json:"preferred_username"`
	OrgId        string `json:"organization_id"`
	UserId       string `json:"user_id"`
	ProfileId    string
	OrgNamespace string
}

type SessionOptions struct {
	ApiVersion    string
	Alias         string
	RefreshMethod RefreshMethod
}

type OAuthError struct {
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

type ForceSession struct {
	AccessToken      string `json:"access_token"`
	InstanceUrl      string `json:"instance_url"`
	IssuedAt         string `json:"issued_at"`
	Scope            string `json:"scope"`
	ClientId         string
	RefreshToken     string `json:"refresh_token"`
	ForceEndpoint    ForceEndpoint
	UserInfo         *UserInfo
	UserId           string `json:"id"`
	SessionOptions   *SessionOptions
	SessionRefreshed bool
}

type LoginFault struct {
	ExceptionCode    string `xml:"exceptionCode" json:"exceptionCode"`
	ExceptionMessage string `xml:"exceptionMessage" json:"exceptionMessage"`
}

type SoapFault struct {
	FaultCode   string     `xml:"Body>Fault>faultcode"`
	FaultString string     `xml:"Body>Fault>faultstring"`
	Detail      LoginFault `xml:"Body>Fault>detail>LoginFault"`
}

type GenericForceError struct {
	Error_Description string
	Error             string
}

type ForceError struct {
	Message   string   `json:"message"`
	ErrorCode string   `json:"errorCode"`
	Fields    []string `json:"fields"`
}

type FieldName struct {
	FieldName string
	IsObject  bool
}

type SelectStruct struct {
	ObjectName string
	FieldNames []FieldName
}

type ForceSobject map[string]interface{}

type ForceCreateRecordResult struct {
	Errors  []string
	Id      string
	Success bool
}

type ForceLimits map[string]ForceLimit

type ForceLimit struct {
	Name      string
	Remaining int64
	Max       int64
}

type ForcePasswordStatusResult struct {
	IsExpired bool
}

type ForcePasswordResetResult struct {
	NewPassword string
}

type ForceRecord map[string]interface{}

//easyjson:json
type ForceQueryResult struct {
	Done           bool          `json:"done"`
	Records        []ForceRecord `json:"records"`
	TotalSize      int           `json:"totalSize"`
	NextRecordsUrl string        `json:"nextRecordsUrl"`
}

type ForceCommunity struct {
	AllowChatterAccessWithoutLogin bool        `json:"allowChatterAccessWithoutLogin"`
	AllowMembersToFlag             bool        `json:"allowMembersToFlag"`
	Description                    interface{} `json:"description"`
	ID                             string      `json:"id"`
	InvitationsEnabled             bool        `json:"invitationsEnabled"`
	KnowledgeableEnabled           bool        `json:"knowledgeableEnabled"`
	LoginURL                       string      `json:"loginUrl"`
	Name                           string      `json:"name"`
	NicknameDisplayEnabled         bool        `json:"nicknameDisplayEnabled"`
	PrivateMessagesEnabled         bool        `json:"privateMessagesEnabled"`
	ReputationEnabled              bool        `json:"reputationEnabled"`
	SendWelcomeEmail               bool        `json:"sendWelcomeEmail"`
	SiteAsContainerEnabled         bool        `json:"siteAsContainerEnabled"`
	SiteURL                        string      `json:"siteUrl"`
	Status                         string      `json:"status"`
	URL                            string      `json:"url"`
	URLPathPrefix                  string      `json:"urlPathPrefix"`
}

type ForceCommunitiesResult struct {
	Communities []ForceCommunity `json:"communities"`
	Total       int              `json:"total"`
}

type ForceSobjectsResult struct {
	Encoding     string
	MaxBatchSize int
	Sobjects     []ForceSobject
}

type Result struct {
	Id      string
	Success bool
	Created bool
	Message string
}

type QueryOptions struct {
	IsTooling bool
	QueryAll  bool
}

type AuraDefinitionBundleResult struct {
	Done           bool
	Records        []ForceRecord
	TotalSize      int
	QueryLocator   string
	Size           int
	EntityTypeName string
	NextRecordsUrl string
}

type AuraDefinitionBundle struct {
	Id               string
	IsDeleted        bool
	DeveloperName    string
	Language         string
	MasterLabel      string
	NamespacePrefix  string
	CreatedDate      string
	CreatedById      string
	LastModifiedDate string
	LastModifiedById string
	SystemModstamp   string
	ApiVersion       int
	Description      string
}

type AuraDefinition struct {
	Id                     string
	IsDeleted              bool
	CreatedDate            string
	CreatedById            string
	LastModifiedDate       string
	LastModifiedById       string
	SystemModstamp         string
	AuraDefinitionBundleId string
	DefType                string
	Format                 string
	Source                 string
}

type ComponentFile struct {
	FileName    string
	ComponentId string
}

type BundleManifest struct {
	Name  string
	Id    string
	Files []ComponentFile
}

func NewForce(creds *ForceSession) (force *Force) {
	force = new(Force)
	force.Credentials = creds
	force.Metadata = NewForceMetadata(force)
	force.Partner = NewForcePartner(force)
	return
}

func (e *ForceError) Error() string {
	return fmt.Sprintf("[%s]: %s", e.ErrorCode, e.Message)
}

func GetEndpointURL(endpoint ForceEndpoint) (endpointURL string, err error) {
	switch endpoint {
	case EndpointProduction:
		endpointURL = "https://login.salesforce.com"
	case EndpointTest:
		endpointURL = "https://test.salesforce.com"
	case EndpointPrerelease:
		endpointURL = "https://prerellogin.pre.salesforce.com"
	case EndpointMobile1:
		endpointURL = "https://EndpointMobile1.t.salesforce.com"
	case EndpointCustom:
		endpointURL = CustomEndpoint
	default:
		err = fmt.Errorf("no such endpoint type")
	}

	return
}

func ForceSoapLogin(endpoint ForceEndpoint, username string, password string) (creds ForceSession, err error) {
	var surl string

	version := strings.Split(apiVersion, "v")[1]

	endpointURL, err := GetEndpointURL(endpoint)
	if err != nil {
		err = errors.New("Unable to login with SOAP. Unknown endpoint type")
		return creds, err
	}

	surl = fmt.Sprintf("%s/services/Soap/u/%s", endpointURL, version)

	soap := NewSoap(surl, "", "")
	response, err := soap.ExecuteLogin(username, password)
	if err != nil {
		return creds, err
	}

	var result struct {
		SessionId    string `xml:"Body>loginResponse>result>sessionId"`
		Id           string `xml:"Body>loginResponse>result>userId"`
		Instance_url string `xml:"Body>loginResponse>result>serverUrl"`
	}

	var fault SoapFault

	if err = xml.Unmarshal(response, &fault); err != nil {
		return creds, err
	} else if fault.Detail.ExceptionMessage != "" {
		err = fmt.Errorf("%s: %s", fault.Detail.ExceptionCode, fault.Detail.ExceptionMessage)
		return creds, err
	}

	if err = xml.Unmarshal(response, &result); err != nil {
		return creds, err
	}

	orgid := strings.Split(result.SessionId, "!")[0]
	u, err := url.Parse(result.Instance_url)
	if err != nil {
		return creds, err
	}

	instanceUrl := u.Scheme + "://" + u.Host

	creds = ForceSession{
		AccessToken:   result.SessionId,
		InstanceUrl:   instanceUrl,
		ForceEndpoint: endpoint,
		UserInfo: &UserInfo{
			OrgId:  orgid,
			UserId: result.Id,
		},
		SessionOptions: &SessionOptions{
			ApiVersion: apiVersionNumber,
		},
	}

	return creds, nil
}

func tokenURL(endpoint ForceEndpoint) (string, error) {
	endpointURL, err := GetEndpointURL(endpoint)
	if err != nil {
		return "", err
	}

	tokenURL := fmt.Sprintf("%s/services/oauth2/token", endpointURL)

	log.Printf("tokenURL: %v", tokenURL)

	return tokenURL, nil
}

func (f *Force) refreshTokenURL() (refreshURL string, err error) {
	endpoint := f.Credentials.ForceEndpoint

	if endpoint == EndpointInstace {
		refreshURL = fmt.Sprintf("%s/services/oauth2/token", f.Credentials.InstanceUrl)
	} else {
		refreshURL, err = tokenURL(endpoint)
		if err != nil {
			return refreshURL, err
		}
	}

	log.Printf("refreshURL: %v - endpoint: %v", refreshURL, endpoint)

	return refreshURL, nil
}

func (f *Force) GetCodeCoverage(classId string, className string) (err error) {
	url := fmt.Sprintf("%s/services/data/%s/query/?q=Select+Id+From+ApexClass+Where+Name+=+'%s'", f.Credentials.InstanceUrl, apiVersion, className)

	body, err := f.httpGet(url, false)
	if err != nil {
		return
	}
	var result ForceQueryResult
	json.Unmarshal(body, &result)

	classId = cast.ToString(result.Records[0]["Id"])
	url = fmt.Sprintf("%s/services/data/%s/tooling/query/?q=Select+Coverage,+NumLinesCovered,+NumLinesUncovered,+ApexTestClassId,+ApexClassorTriggerId+From+ApexCodeCoverage+Where+ApexClassorTriggerId='%s'", f.Credentials.InstanceUrl, apiVersion, classId)

	body, err = f.httpGet(url, false)
	if err != nil {
		return
	}

	//var result ForceSobjectsResult
	json.Unmarshal(body, &result)
	fmt.Printf("\n%d lines covered\n%d lines not covered\n", cast.ToInt(result.Records[0]["NumLinesCovered"]), cast.ToInt(result.Records[0]["NumLinesUncovered"]))
	return
}

func (f *Force) DeleteDataPipeline(id string) (err error) {
	url := fmt.Sprintf("%s/services/data/%s/tooling/sobjects/DataPipeline/%s", f.Credentials.InstanceUrl, apiVersion, id)
	_, err = f.httpDelete(url, false)
	return
}

func (f *Force) UpdateDataPipeline(id string, masterLabel string, scriptContent string) (err error) {
	url := fmt.Sprintf("%s/services/data/%s/tooling/sobjects/DataPipeline/%s", f.Credentials.InstanceUrl, apiVersion, id)
	attrs := make(map[string]string)
	attrs["MasterLabel"] = masterLabel
	attrs["ScriptContent"] = scriptContent

	_, err = f.httpPatch(url, attrs, false)
	return
}

func (f *Force) CreateDataPipeline(name string, masterLabel string, apiVersionNumber string, scriptContent string, scriptType string) (result ForceCreateRecordResult, err error, emessages []ForceError) {
	aurl := fmt.Sprintf("%s/services/data/%s/tooling/sobjects/DataPipeline", f.Credentials.InstanceUrl, apiVersion)

	attrs := make(map[string]string)
	attrs["DeveloperName"] = name
	attrs["ScriptType"] = scriptType
	attrs["MasterLabel"] = masterLabel
	attrs["ApiVersion"] = apiVersionNumber
	attrs["ScriptContent"] = scriptContent

	body, err, emessages := f.httpPost(aurl, attrs, false)
	if err != nil {
		return
	}
	json.Unmarshal(body, &result)

	return

}

func (f *Force) CreateDataPipelineJob(id string) (result ForceCreateRecordResult, err error, emessages []ForceError) {
	aurl := fmt.Sprintf("%s/services/data/%s/tooling/sobjects/DataPipelineJob", f.Credentials.InstanceUrl, apiVersion)

	attrs := make(map[string]string)
	attrs["DataPipelineId"] = id

	body, err, emessages := f.httpPost(aurl, attrs, false)
	if err != nil {
		return
	}
	json.Unmarshal(body, &result)

	return

}

func (f *Force) GetDataPipeline(name string) (results ForceQueryResult, err error) {
	query := fmt.Sprintf("SELECT Id, MasterLabel, DeveloperName, ScriptContent, ScriptType FROM DataPipeline Where DeveloperName = '%s'", name)
	results, err = f.QueryDataPipeline(query)
	return
}

func (f *Force) QueryDataPipeline(soql string) (results ForceQueryResult, err error) {
	body, err := f.QueryDataPipelineAsBytes(soql)
	if err != nil {
		return
	}
	json.Unmarshal(body, &results)

	return
}

func (f *Force) QueryDataPipelineAsBytes(soql string) (sobject []byte, err error) {
	aurl := fmt.Sprintf("%s/services/data/%s/tooling/query?q=%s", f.Credentials.InstanceUrl, apiVersion, url.QueryEscape(soql))
	return f.httpGet(aurl, false)
}

func (f *Force) ListSObjectsAsByte() (sobjects []byte, err error) {
	url := fmt.Sprintf("%s/services/data/%s/sobjects", f.Credentials.InstanceUrl, apiVersion)
	return f.httpGet(url, false)
}

func (f *Force) ListSObjects() (sobjects []ForceSobject, err error) {
	body, err := f.ListSObjectsAsByte()
	if err != nil {
		return
	}
	var result ForceSobjectsResult
	json.Unmarshal(body, &result)
	sobjects = result.Sobjects
	return
}

func (f *Force) ListSObjectName() (sobjects []string, err error) {
	objs, err := f.ListSObjects()
	if err != nil {
		return
	}
	for _, obj := range objs {
		objName := cast.ToString(obj["name"])
		if len(objName) > 0 {
			sobjects = append(sobjects, objName)
		}
	}
	return
}

func (f *Force) GetSobjectAsBytes(name string) (sobject []byte, err error) {
	url := fmt.Sprintf("%s/services/data/%s/sobjects/%s/describe", f.Credentials.InstanceUrl, apiVersion, name)
	return f.httpGet(url, false)
}

func (f *Force) GetSobject(name string) (sobject ForceSobject, err error) {
	body, err := f.GetSobjectAsBytes(name)
	if err != nil {
		return
	}
	json.Unmarshal(body, &sobject)
	return
}

func (f *Force) GetCompactLayoutsAsBytes(name string) (sobject []byte, err error) {
	url := fmt.Sprintf("%s/services/data/%s/sobjects/%s/describe/compactLayouts", f.Credentials.InstanceUrl, apiVersion, name)
	return f.httpGet(url, false)
}

func (f *Force) GetLayoutsAsBytes(name string) (sobject []byte, err error) {
	url := fmt.Sprintf("%s/services/data/%s/sobjects/%s/describe/layouts", f.Credentials.InstanceUrl, apiVersion, name)
	return f.httpGet(url, false)
}

func (f *Force) GetListviewsAsBytes(name string) (sobject []byte, err error) {
	url := fmt.Sprintf("%s/services/data/%s/sobjects/%s/listviews", f.Credentials.InstanceUrl, apiVersion, name)
	return f.httpGet(url, false)
}

func (f *Force) GetListviewDescribeAsBytes(name, id string) (sobject []byte, err error) {
	url := fmt.Sprintf("%s/services/data/%s/sobjects/%s/listviews/%s/describe", f.Credentials.InstanceUrl, apiVersion, name, id)
	return f.httpGet(url, false)
}

func (f *Force) CompactLayoutsSObject(name string) (result string, err error) {
	body, err := f.GetCompactLayoutsAsBytes(name)
	if err != nil {
		return
	}
	result = string(body)
	return
}

func (f *Force) LayoutsSObject(name string) (result string, err error) {
	body, err := f.GetLayoutsAsBytes(name)
	if err != nil {
		return
	}
	result = string(body)
	return
}

func (f *Force) ListViewsSObject(name string) (result string, err error) {
	body, err := f.GetListviewsAsBytes(name)
	if err != nil {
		return
	}
	result = string(body)
	return
}

func (f *Force) ListViewDescribeSObject(name, id string) (result string, err error) {
	body, err := f.GetListviewDescribeAsBytes(name, id)
	if err != nil {
		return
	}
	result = string(body)
	return
}

func (f *Force) QueryAndSend(query string, processor chan<- ForceRecord, options ...func(*QueryOptions)) (err error) {
	queryOptions := QueryOptions{}
	for _, option := range options {
		option(&queryOptions)
	}
	cmd := "query"
	if queryOptions.QueryAll {
		cmd = "queryAll"
	}
	if queryOptions.IsTooling {
		cmd = "tooling/" + cmd
	}
	processResults := func(body []byte) (result ForceQueryResult, err error) {
		err = json.Unmarshal(body, &result)
		if err != nil {
			return
		}
		for _, row := range result.Records {
			processor <- row
		}
		return
	}

	var body []byte
	url := fmt.Sprintf("%s/services/data/%s/%s?q=%s", f.Credentials.InstanceUrl, apiVersion, cmd, url.QueryEscape(query))
	for {
		body, err = f.httpGet(url, false)
		if err != nil {
			return
		}
		var result ForceQueryResult
		result, err = processResults(body)
		if err != nil {
			return
		}
		if result.Done {
			break
		}
		url = fmt.Sprintf("%s%s", f.Credentials.InstanceUrl, result.NextRecordsUrl)
	}
	close(processor)
	return
}

func (f *Force) Query(query string, queryAll, tooling bool) (result ForceQueryResult, err error) {
	// queryOptions := QueryOptions{}
	// for _, option := range options {
	// 	option(&queryOptions)
	// }
	cmd := "query"
	if queryAll {
		cmd = "queryAll"
	}
	if tooling {
		cmd = "tooling/" + cmd
	}

	result = ForceQueryResult{
		Done:           false,
		NextRecordsUrl: fmt.Sprintf("%s/services/data/%s/%s?q=%s", f.Credentials.InstanceUrl, apiVersion, cmd, url.QueryEscape(query)),
		TotalSize:      0,
		Records:        []ForceRecord{},
	}

	/* The Force API will split queries returning large result sets into
	 * multiple pieces (generally every 200 records). We need to repeatedly
	 * query until we've retrieved all of them. */
	for !result.Done {
		var body []byte
		body, err = f.httpGet(result.NextRecordsUrl, false)

		if err != nil {
			return
		}

		var currResult ForceQueryResult
		json.Unmarshal(body, &currResult)
		result.Update(currResult, f)
	}

	return
}

func (f *Force) DumpListStack(l *list.List) {
	fmt.Printf("\nDecode Results:\n")
	for e := l.Front(); e != nil; e = e.Next() {
		spec := e.Value.(*SelectStruct)
		fmt.Println(spec.ObjectName)
		for _, v := range spec.FieldNames {
			fmt.Printf("\t%v", v.FieldName)
			if v.IsObject {
				fmt.Printf(" (Object)\n")
			} else {
				fmt.Printf("\n")
			}
		}
	}
	fmt.Printf("\n\n")
}

func (f *Force) PushFieldName(fieldName string, spec *SelectStruct, IsObject bool) {
	for _, v := range spec.FieldNames {
		if v.FieldName == fieldName {
			return
		}
	}
	spec.FieldNames = append(spec.FieldNames, FieldName{fieldName, IsObject})
	return
}

func (f *Force) GetPrevObjectSpec(objectName string, l *list.List) (foundItem *SelectStruct) {
	for e := l.Front(); e != nil; e = e.Next() {
		if e.Value.(*SelectStruct).ObjectName == objectName {
			p := e.Prev()
			if p != nil {
				foundItem = e.Prev().Value.(*SelectStruct)
				return
			}
		}
	}
	return
}

func (f *Force) GetObjectSpec(objectName string, l *list.List) (result *SelectStruct) {
	found, result := f.HasObject(objectName, l)
	if !found {
		result = new(SelectStruct)
		result.ObjectName = objectName
		l.PushBack(result)
	}
	return
}

func (f *Force) HasObject(objectName string, l *list.List) (result bool, foundItem *SelectStruct) {
	for e := l.Front(); e != nil; e = e.Next() {
		if e.Value.(*SelectStruct).ObjectName == objectName {
			result = true
			foundItem = e.Value.(*SelectStruct)
			return
		}
	}
	result = false
	return
}

func (f *Force) Get(url string) (object ForceRecord, err error) {
	body, err := f.httpGet(url, false)
	if err != nil {
		return
	}
	err = json.Unmarshal(body, &object)
	return
}

func (f *Force) GetResources() (result ForceRecord, err error) {
	url := fmt.Sprintf("%s/services/data/%s", f.Credentials.InstanceUrl, apiVersion)
	body, err := f.httpGet(url, false)
	if err != nil {
		return
	}
	json.Unmarshal(body, &result)
	return
}

func (f *Force) GetCommunities() (result ForceCommunitiesResult, err error) {
	url := fmt.Sprintf("%s/services/data/%s/connect/communities", f.Credentials.InstanceUrl, apiVersion)
	body, err := f.httpGet(url, false)
	if err != nil {
		return
	}
	json.Unmarshal(body, &result)
	return
}

func (f *Force) GetCommunity(id string) (result ForceCommunity, err error) {
	url := fmt.Sprintf("%s/services/data/%s/connect/communities/%s", f.Credentials.InstanceUrl, apiVersion, id)
	body, err := f.httpGet(url, false)
	if err != nil {
		return
	}
	json.Unmarshal(body, &result)
	return
}

func (f *Force) GetIdentifyAsBytes() (result []byte, err error) {
	resources, err := f.GetResources()
	if err != nil {
		return
	}
	if rid, ok := resources["identity"]; ok {
		result, err = f.httpGet(cast.ToString(rid), false)
		if err != nil {
			return
		}
	} else {
		err = errors.New("identity not found")
	}
	return
}

func (f *Force) GetIdentify() (result ForceRecord, err error) {
	body, err := f.GetIdentifyAsBytes()
	if err != nil {
		return
	}
	json.Unmarshal(body, &result)
	return
}

func (f *Force) GetLimits() (result map[string]ForceLimit, err error) {
	url := fmt.Sprintf("%s/services/data/%s/limits", f.Credentials.InstanceUrl, apiVersion)
	body, err := f.httpGet(url, false)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(body), &result)
	return

}

func (f *Force) GetPasswordStatus(id string) (result ForcePasswordStatusResult, err error) {
	url := fmt.Sprintf("%s/services/data/%s/sobjects/User/%s/password", f.Credentials.InstanceUrl, apiVersion, id)
	body, err := f.httpGet(url, false)
	if err != nil {
		return
	}
	err = json.Unmarshal(body, &result)
	return
}

func (f *Force) ResetPassword(id string) (result ForcePasswordResetResult, err error) {
	url := fmt.Sprintf("%s/services/data/%s/sobjects/User/%s/password", f.Credentials.InstanceUrl, apiVersion, id)
	body, err := f.httpDelete(url, false)
	if err != nil {
		return
	}
	err = json.Unmarshal(body, &result)
	return
}

func (f *Force) ChangePassword(id string, attrs map[string]string) (result string, err error, emessages []ForceError) {
	url := fmt.Sprintf("%s/services/data/%s/sobjects/User/%s/password", f.Credentials.InstanceUrl, apiVersion, id)
	_, err, emessages = f.httpPost(url, attrs, false)
	return
}

func (f *Force) QueryProfile(fields ...string) (results ForceQueryResult, err error) {

	url := fmt.Sprintf("%s/services/data/%s/tooling/query?q=Select+%s+From+Profile+Where+Id='%s'",
		f.Credentials.InstanceUrl,
		apiVersion,
		strings.Join(fields, ","),
		f.Credentials.UserInfo.ProfileId)

	body, err := f.httpGet(url, false)
	if err != nil {
		return
	}
	json.Unmarshal(body, &results)
	return
}

func (f *Force) QueryTraceFlags() (results ForceQueryResult, err error) {
	url := fmt.Sprintf("%s/services/data/%s/tooling/query/?q=Select+Id,+DebugLevel.DeveloperName,++ApexCode,+ApexProfiling,+Callout,+CreatedDate,+Database,+ExpirationDate,+System,+TracedEntity.Name,+Validation,+Visualforce,+Workflow+From+TraceFlag+Order+By+ExpirationDate,TracedEntity.Name", f.Credentials.InstanceUrl, apiVersion)
	body, err := f.httpGet(url, false)
	if err != nil {
		return
	}
	json.Unmarshal(body, &results)
	return
}

func (f *Force) QueryDefaultDebugLevel() (id string, err error) {
	url := fmt.Sprintf("%s/services/data/%s/tooling/query/?q=Select+Id+From+DebugLevel+Where+DeveloperName+=+'Force_CLI'", f.Credentials.InstanceUrl, apiVersion)
	body, err := f.httpGet(url, false)
	if err != nil {
		return
	}
	var results ForceQueryResult
	json.Unmarshal(body, &results)
	if len(results.Records) == 1 {
		id = cast.ToString(results.Records[0]["Id"])
	}
	return
}

func (f *Force) DefaultDebugLevel() (id string, err error, emessages []ForceError) {
	id, err = f.QueryDefaultDebugLevel()
	if err != nil || id != "" {
		return
	}
	url := fmt.Sprintf("%s/services/data/%s/tooling/sobjects/DebugLevel", f.Credentials.InstanceUrl, apiVersion)

	// The log levels are currently hard-coded to a useful level of logging
	// without hitting the maximum log size of 2MB in most cases, hopefully.
	attrs := make(map[string]string)
	attrs["ApexCode"] = "Fine"
	attrs["ApexProfiling"] = "Error"
	attrs["Callout"] = "Info"
	attrs["Database"] = "Info"
	attrs["System"] = "Info"
	attrs["Validation"] = "Warn"
	attrs["Visualforce"] = "Info"
	attrs["Workflow"] = "Info"
	attrs["DeveloperName"] = "Force_CLI"
	attrs["MasterLabel"] = "Force_CLI"

	body, err, emessages := f.httpPost(url, attrs, false)
	if err != nil {
		return
	}
	var result ForceCreateRecordResult
	json.Unmarshal(body, &result)
	if result.Success {
		id = result.Id
	}
	return
}

func (f *Force) StartTrace(userId ...string) (result ForceCreateRecordResult, err error, emessages []ForceError) {
	debugLevel, err, emessages := f.DefaultDebugLevel()
	if err != nil {
		return
	}
	url := fmt.Sprintf("%s/services/data/%s/tooling/sobjects/TraceFlag", f.Credentials.InstanceUrl, apiVersion)
	attrs := make(map[string]string)
	attrs["DebugLevelId"] = debugLevel
	if len(userId) == 1 {
		attrs["TracedEntityId"] = userId[0]
		attrs["LogType"] = "USER_DEBUG"
	} else {
		attrs["TracedEntityId"] = f.Credentials.UserInfo.UserId
		attrs["LogType"] = "DEVELOPER_LOG"
	}
	body, err, emessages := f.httpPost(url, attrs, false)
	if err != nil {
		return
	}
	json.Unmarshal(body, &result)
	return
}

func (f *Force) GetConsoleLogLevelId() (result string, err error) {
	url := fmt.Sprintf("%s/services/data/%s/tooling/query?q=Select+Id+From+DebugLevel+Where+DeveloperName+=+'SFDC_DevConsole'", f.Credentials.InstanceUrl, apiVersion)
	body, err := f.httpGet(url, false)
	if err != nil {
		return
	}
	var res ForceQueryResult
	json.Unmarshal(body, &res)
	result = cast.ToString(res.Records[0]["Id"])
	fmt.Println(result)
	return
}

func (f *Force) RetrieveLog(logId string) (result string, err error) {
	url := fmt.Sprintf("%s/services/data/%s/tooling/sobjects/ApexLog/%s/Body", f.Credentials.InstanceUrl, apiVersion, logId)
	body, err := f.httpGet(url, false)
	if err != nil {
		return
	}
	result = string(body)
	return
}

func (f *Force) QueryLogs() (results ForceQueryResult, err error) {
	url := fmt.Sprintf("%s/services/data/%s/tooling/query/?q=Select+Id,+Application,+DurationMilliseconds,+Location,+LogLength,+LogUser.Name,+Operation,+Request,StartTime,+Status+From+ApexLog+Order+By+StartTime", f.Credentials.InstanceUrl, apiVersion)
	body, err := f.httpGet(url, false)
	if err != nil {
		return
	}
	json.Unmarshal(body, &results)
	return
}

func (f *Force) RetrieveEventLogFile(elfId string) (result string, err error) {
	url := fmt.Sprintf("%s/services/data/%s/sobjects/EventLogFile/%s/LogFile", f.Credentials.InstanceUrl, apiVersion, elfId)
	body, err := f.httpGet(url, false)
	if err != nil {
		return
	}
	result = string(body)
	return
}

func (f *Force) UpdateAuraComponent(source map[string]string, id string) (err error) {
	url := fmt.Sprintf("%s/services/data/%s/tooling/sobjects/AuraDefinition/%s", f.Credentials.InstanceUrl, apiVersion, id)
	_, err = f.httpPatch(url, source, false)
	return
}

func (f *Force) DeleteToolingRecord(objecttype string, id string) (err error) {
	url := fmt.Sprintf("%s/services/data/%s/tooling/sobjects/%s/%s", f.Credentials.InstanceUrl, apiVersion, objecttype, id)
	_, err = f.httpDelete(url, false)
	return
}

func (f *Force) CreateToolingRecord(objecttype string, attrs map[string]string) (result ForceCreateRecordResult, err error) {
	aurl := fmt.Sprintf("%s/services/data/%s/tooling/sobjects/%s", f.Credentials.InstanceUrl, apiVersion, objecttype)
	body, err, _ := f.httpPost(aurl, attrs, false)
	if err != nil {
		return
	}
	json.Unmarshal(body, &result)
	return
}

func (f *Force) GetToolingRecord(sobject, id string) (object ForceRecord, err error) {
	url := fmt.Sprintf("%s/services/data/%s/tooling/sobjects/%s/%s", f.Credentials.InstanceUrl, apiVersion, sobject, id)
	body, err := f.httpGet(url, false)
	if err != nil {
		return
	}
	err = json.Unmarshal(body, &object)
	return
}

func (f *Force) GetToolingRecordAsBytes(sobject, id string) ([]byte, error) {
	url := fmt.Sprintf("%s/services/data/%s/tooling/sobjects/%s/%s", f.Credentials.InstanceUrl, apiVersion, sobject, id)
	return f.httpGet(url, false)
}

func (f *Force) DescribeSObject(name string) (result string, err error) {
	body, err := f.GetSobjectAsBytes(name)
	if err != nil {
		return
	}
	result = string(body)
	return
}

func (f *Force) GetRecord(sobject, id string) (object ForceRecord, err error) {
	fields := strings.Split(id, ":")
	var url string
	if len(fields) == 1 {
		url = fmt.Sprintf("%s/services/data/%s/sobjects/%s/%s", f.Credentials.InstanceUrl, apiVersion, sobject, id)
	} else {
		url = fmt.Sprintf("%s/services/data/%s/sobjects/%s/%s/%s", f.Credentials.InstanceUrl, apiVersion, sobject, fields[0], fields[1])
	}

	body, err := f.httpGet(url, false)
	if err != nil {
		return
	}
	err = json.Unmarshal(body, &object)
	return
}

func (f *Force) GetBase64(sobject, id, field string) (object []byte, err error) {
	url := fmt.Sprintf("%s/services/data/%s/sobjects/%s/%s/%s", f.Credentials.InstanceUrl, apiVersion, sobject, id, field)
	object, err = f.httpGet(url, false)
	if err != nil {
		return
	}
	return
}

func (f *Force) GetBase64Stream(sobject, id, field string) (response *http.Response, err error) {
	url := fmt.Sprintf("%s/services/data/%s/sobjects/%s/%s/%s", f.Credentials.InstanceUrl, apiVersion, sobject, id, field)
	log.Printf("URL: %s | InstanceURL: %s | apiVersion: %s | sobject: %s | id: %s | field: %s", url, f.Credentials.InstanceUrl, apiVersion, sobject, id, field)
	response, err = f.httpGetStream(url, false)
	if err != nil {
		return
	}
	return
}

func (f *Force) CreateRecord(sobject string, attrs map[string]string) (id string, err error, emessages []ForceError) {
	url := fmt.Sprintf("%s/services/data/%s/sobjects/%s", f.Credentials.InstanceUrl, apiVersion, sobject)
	body, err, emessages := f.httpPost(url, attrs, false)
	if err != nil {
		return
	}
	var result ForceCreateRecordResult
	json.Unmarshal(body, &result)
	id = result.Id
	return
}

func (f *Force) UpdateRecord(sobject, id string, attrs map[string]string) (err error) {
	fields := strings.Split(id, ":")
	var url string
	if len(fields) == 1 {
		url = fmt.Sprintf("%s/services/data/%s/sobjects/%s/%s", f.Credentials.InstanceUrl, apiVersion, sobject, id)
	} else {
		url = fmt.Sprintf("%s/services/data/%s/sobjects/%s/%s/%s", f.Credentials.InstanceUrl, apiVersion, sobject, fields[0], fields[1])
	}
	_, err = f.httpPatch(url, attrs, false)
	return
}

func (f *Force) DeleteRecord(sobject, id string) (err error) {
	url := fmt.Sprintf("%s/services/data/%s/sobjects/%s/%s", f.Credentials.InstanceUrl, apiVersion, sobject, id)
	_, err = f.httpDelete(url, false)
	if err != nil {
		if err.Error() == "The requested resource does not exist" {
			err = DeleteRecordResourceNotExistsError
		}
	}
	return
}

func (f *Force) DeleteRecordExternalID(sobject, field, id string) (err error) {
	url := fmt.Sprintf("%s/services/data/%s/sobjects/%s/%s/%s", f.Credentials.InstanceUrl, apiVersion, sobject, field, id)
	_, err = f.httpDelete(url, false)
	if err != nil {
		if err.Error() == "The requested resource does not exist" {
			err = DeleteRecordResourceNotExistsError
		}
	}
	return
}

func (f *Force) CreateRecordJSON(sobject, data string) (id string, err error) {
	url := fmt.Sprintf("%s/services/data/%s/sobjects/%s/Id", f.Credentials.InstanceUrl, apiVersion, sobject)
	body, err := f.httpPostJSON(url, data, false)
	if err != nil {
		return
	}
	var result ForceCreateRecordResult
	json.Unmarshal(body, &result)
	id = result.Id
	return
}

func (f *Force) UpdateRecordJSON(sobject, id, data string) (err error) {
	url := fmt.Sprintf("%s/services/data/%s/sobjects/%s/%s", f.Credentials.InstanceUrl, apiVersion, sobject, id)
	_, err = f.httpPatchJSON(url, data, false)
	if err != nil {
		if err.Error() == `[Code: ENTITY_IS_DELETED]: Message: "entity is deleted"` {
			err = EntityIsDeleted
		}
		log.Printf("Object data error: sObject(%v) | id(%v) | data(%v)", sobject, id, data)
	}
	return
}

func (f *Force) UpsertRecordJSON(sobject, extidname, extid, data string) (id string, err error) {
	url := fmt.Sprintf("%s/services/data/%s/sobjects/%s/%s/%s", f.Credentials.InstanceUrl, apiVersion, sobject, extidname, extid)
	body, err := f.httpPatchJSON(url, data, false)
	if err != nil {
		if err.Error() == `[Code: ENTITY_IS_DELETED]: Message: "entity is deleted"` {
			err = EntityIsDeleted
		}
		return
	}
	var result ForceCreateRecordResult
	json.Unmarshal(body, &result)
	id = result.Id
	return
}

func (f *Force) Whoami() (me ForceRecord, err error) {
	me, err = f.GetRecord("User", f.Credentials.UserId)
	return
}

func (result *ForceQueryResult) Update(other ForceQueryResult, force *Force) {
	result.Done = other.Done
	result.Records = append(result.Records, other.Records...)
	result.TotalSize = other.TotalSize
	result.NextRecordsUrl = fmt.Sprintf("%s%s", force.Credentials.InstanceUrl, other.NextRecordsUrl)
}

// ValidateSFID validate a Salesforce ID
// Stackoverflow: https://stackoverflow.com/a/29299786/1333724
// Gist: https://gist.github.com/maxmcd/a32a35c0eebcfb77cd005f9dd8958815
// Post about SF unique ID: https://astadiaemea.wordpress.com/2010/06/21/15-or-18-character-ids-in-salesforce-com-–-do-you-know-how-useful-unique-ids-are-to-your-development-effort/
func ValidateSFID(input string) bool {
	r, _ := regexp.Compile("[a-zA-Z0-9]{18}|[a-zA-Z0-9]{15}")

	// 00U4P 00001 hkIWZ UA2
	// P4U00 10000 ZWIkh
	// 10100 00000 11100 [20 0 28]
	//   U     A     2

	if r.MatchString(input) {
		if len(input) == 15 {
			return true
		} else if len(input) == 18 {
			var (
				parts = []string{input[0:5], input[5:10], input[10:15]}
				chars = make([]byte, 3)
			)

			for j, word := range parts {
				for i, char := range []byte(word) {
					if char >= 65 && char <= 90 {
						chars[j] += 1 << uint64(i)
					}
				}
			}

			for i, c := range chars {
				if c <= 25 {
					chars[i] = c + 65 // A..Z
				} else {
					chars[i] = c - 25 + 47 // 0..5
				}
			}

			if string(chars) == input[15:18] {
				return true
			}
		}
	}

	return false
}
