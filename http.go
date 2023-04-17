package gforce

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptrace"
	"runtime"
	"strings"
	"time"

	"go.uber.org/multierr"
)

func (f *Force) SetHTTPTrace(trace bool) {
	traceHTTPRequest = trace
}

func (f *Force) SetHTTPTraceDetail(trace bool) {
	traceHTTPRequest = trace
	traceHTTPRequestDetail = trace
}

// GET

func (f *Force) httpGet(url string, refreshed bool) (body []byte, err error) {
	headers := map[string]string{
		"Authorization":  fmt.Sprintf("Bearer %s", f.Credentials.AccessToken),
		"X-SFDC-Session": fmt.Sprintf("Bearer %s", f.Credentials.AccessToken),
	}
	body, err = f.httpGetRequest(url, headers)
	if err == SessionExpiredError {
		if f.Credentials.RefreshToken != "" && !refreshed {
			log.Printf("Session Expired Credentials: RefreshToken (%v) - refreshed (%v)", f.Credentials.RefreshToken, refreshed)
			if e := f.RefreshSession(); e != nil {
				log.Printf("Error f.RefreshSession(): %w", e)
				return nil, e
			}
			return f.httpGet(url, true)
		}
		return nil, err
	}
	return
}

func (f *Force) httpGetBulk(url string, refreshed bool) (body []byte, err error) {
	headers := map[string]string{
		"X-SFDC-Session": fmt.Sprintf("Bearer %s", f.Credentials.AccessToken),
		"Authorization":  fmt.Sprintf("Bearer %s", f.Credentials.AccessToken),
		"Content-Type":   "application/xml",
		"Accept":         "application/xml",
	}
	body, err = f.httpGetRequest(url, headers)
	if err == SessionExpiredError {

		if f.Credentials.RefreshToken != "" && !refreshed {
			if e := f.RefreshSession(); e != nil {

				return nil, e
			}
			return f.httpGetBulk(url, true)
		}
		return nil, err
	}
	return
}

func (f *Force) httpGetBulkJSON(url string, refreshed bool) (body []byte, err error) {
	headers := map[string]string{
		"X-SFDC-Session": fmt.Sprintf("Bearer %s", f.Credentials.AccessToken),
		"Authorization":  fmt.Sprintf("Bearer %s", f.Credentials.AccessToken),
		"Content-Type":   "application/json",
		"Accept":         "application/json",
	}
	body, err = f.httpGetRequest(url, headers)
	if err == SessionExpiredError {
		if f.Credentials.RefreshToken != "" && !refreshed {
			if e := f.RefreshSession(); e != nil {
				return nil, e
			}
			return f.httpGetBulkJSON(url, true)
		}
		return nil, err
	}
	return
}

func (f *Force) httpGetRequest(url string, headers map[string]string) (body []byte, err error) {
	req, err := httpRequest("GET", url, nil)
	if err != nil {
		return
	}
	for headerName, headerValue := range headers {
		req.Header.Add(headerName, headerValue)
	}
	res, err := doRequest(req)
	if err != nil {
		log.Printf("Error doRequest httpGetRequest: %w", err)
		return
	}
	defer res.Body.Close()
	if res.StatusCode == 401 || res.StatusCode == 403 {
		err = SessionExpiredError
		return
	}
	body, err = ioutil.ReadAll(res.Body)
	if err != nil {
		return
	}

	if res.StatusCode/100 != 2 {
		contentType := res.Header.Get("Content-Type")
		if strings.HasPrefix(contentType, "application/xml") {
			var fault LoginFault
			xml.Unmarshal(body, &fault)
			if fault.ExceptionCode == "InvalidSessionId" {
				err = SessionExpiredError
			}
		} else {
			var messages []ForceError
			json.Unmarshal(body, &messages)
			if len(messages) > 0 {
				err = errors.New(messages[0].Message)
			} else {
				err = errors.New(string(body))
			}
		}
		return
	}
	return
}

// GET STREAM

func (f *Force) httpGetStream(url string, refreshed bool) (response *http.Response, err error) {
	headers := map[string]string{
		"Authorization":  fmt.Sprintf("Bearer %s", f.Credentials.AccessToken),
		"X-SFDC-Session": fmt.Sprintf("Bearer %s", f.Credentials.AccessToken),
	}
	response, err = f.httpGetRequestStream(url, headers)
	if err == SessionExpiredError {
		if f.Credentials.RefreshToken != "" && !refreshed {
			if e := f.RefreshSession(); e != nil {
				return nil, e
			}
			return f.httpGetStream(url, true)
		}
		return nil, err
	}
	return
}

func (f *Force) httpGetBulkStream(url string, refreshed bool) (response *http.Response, err error) {
	headers := map[string]string{
		"X-SFDC-Session": fmt.Sprintf("Bearer %s", f.Credentials.AccessToken),
		"Authorization":  fmt.Sprintf("Bearer %s", f.Credentials.AccessToken),
		"Content-Type":   "application/xml",
		"Accept":         "application/xml",
	}

	response, err = f.httpGetRequestStream(url, headers)
	if err == SessionExpiredError {
		if f.Credentials.RefreshToken != "" && !refreshed {
			if e := f.RefreshSession(); e != nil {
				return nil, e
			}
			return f.httpGetBulkStream(url, true)
		}
		return nil, err
	}

	return
}

func (f *Force) httpGetBulkJSONStream(url string, refreshed bool) (response *http.Response, err error) {
	headers := map[string]string{
		"X-SFDC-Session": fmt.Sprintf("Bearer %s", f.Credentials.AccessToken),
		"Authorization":  fmt.Sprintf("Bearer %s", f.Credentials.AccessToken),
		"Content-Type":   "application/json",
		"Accept":         "application/json",
	}

	response, err = f.httpGetRequestStream(url, headers)
	if err == SessionExpiredError {
		if f.Credentials.RefreshToken != "" && !refreshed {
			if e := f.RefreshSession(); e != nil {
				return nil, e
			}
			return f.httpGetBulkJSONStream(url, true)
		}
		return nil, err
	}

	return
}

func (f *Force) httpGetRequestStream(url string, headers map[string]string) (response *http.Response, err error) {
	req, err := httpRequest("GET", url, nil)
	if err != nil {
		return
	}

	for headerName, headerValue := range headers {
		req.Header.Add(headerName, headerValue)
	}

	response, err = doRequest(req)
	if err != nil {
		return
	}

	return
}

// PUT

func (f *Force) httpPutCSV(url string, data string, refreshed bool) (body []byte, err error) {
	body, err = f.httpPutWithContentType(url, data, "text/csv")
	if err == SessionExpiredError {
		if f.Credentials.RefreshToken != "" && !refreshed {
			if e := f.RefreshSession(); e != nil {
				return nil, e
			}
			return f.httpPutCSV(url, data, true)
		}
		return nil, err
	}
	return
}

func (f *Force) httpPutXML(url string, data string, refreshed bool) (body []byte, err error) {
	body, err = f.httpPutWithContentType(url, data, "application/xml")
	if err == SessionExpiredError {
		if f.Credentials.RefreshToken != "" && !refreshed {
			if e := f.RefreshSession(); e != nil {
				return nil, e
			}
			return f.httpPutXML(url, data, true)
		}
		return nil, err
	}
	return
}

func (f *Force) httpPutJSON(url string, data string, refreshed bool) (body []byte, err error) {
	body, err = f.httpPutWithContentType(url, data, "application/json")
	if err == SessionExpiredError {
		if f.Credentials.RefreshToken != "" && !refreshed {
			if e := f.RefreshSession(); e != nil {
				return nil, e
			}
			return f.httpPutJSON(url, data, true)
		}
		return nil, err
	}
	return
}

func (f *Force) httpPutWithContentType(url string, data string, contenttype string) (body []byte, err error) {
	body, err = f.httpPutPatchPostWithContentType(url, data, contenttype, "PUT")
	return
}

// PATCH

func (f *Force) httpPatchCSV(url string, data string, refreshed bool) (body []byte, err error) {
	body, err = f.httpPatchWithContentType(url, data, "text/csv")
	if err == SessionExpiredError {
		if f.Credentials.RefreshToken != "" && !refreshed {
			if e := f.RefreshSession(); e != nil {
				return nil, e
			}
			return f.httpPatchCSV(url, data, true)
		}
		return nil, err
	}
	return
}

func (f *Force) httpPatchXML(url string, data string, refreshed bool) (body []byte, err error) {
	body, err = f.httpPatchWithContentType(url, data, "application/xml")
	if err == SessionExpiredError {
		if f.Credentials.RefreshToken != "" && !refreshed {
			if e := f.RefreshSession(); e != nil {
				return nil, e
			}
			return f.httpPatchXML(url, data, true)
		}
		return nil, err
	}
	return
}

func (f *Force) httpPatchJSON(url string, data string, refreshed bool) (body []byte, err error) {
	body, err = f.httpPatchWithContentType(url, data, "application/json")
	if err == SessionExpiredError {
		if f.Credentials.RefreshToken != "" && !refreshed {
			log.Printf("Attempt to refresh session: %+v", f.Credentials)
			if e := f.RefreshSession(); e != nil {
				log.Printf("Error on RefreshSession: %w", e)
				return nil, e
			}
			return f.httpPatchJSON(url, data, true)
		}
		return nil, err
	}
	return
}

func (f *Force) httpPatchWithContentType(url string, data string, contenttype string) (body []byte, err error) {
	body, err = f.httpPutPatchPostWithContentType(url, data, contenttype, "PATCH")
	return
}

func (f *Force) httpPatch(url string, attrs map[string]string, refreshed bool) (body []byte, err error) {
	body, err = f.httpPatchAttributes(url, attrs)
	if err == SessionExpiredError {
		if f.Credentials.RefreshToken != "" && !refreshed {
			if e := f.RefreshSession(); e != nil {
				return nil, e
			}
			return f.httpPatch(url, attrs, true)
		}
		return nil, err
	}
	return
}

func (f *Force) httpPatchAttributes(url string, attrs map[string]string) (body []byte, err error) {
	rbody, _ := json.Marshal(attrs)
	req, err := httpRequest("PATCH", url, bytes.NewReader(rbody))
	if err != nil {
		return
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", f.Credentials.AccessToken))
	req.Header.Add("X-SFDC-Session", fmt.Sprintf("Bearer %s", f.Credentials.AccessToken))
	req.Header.Add("Content-Type", "application/json")
	res, err := doRequest(req)
	if err != nil {
		return
	}
	defer res.Body.Close()
	if res.StatusCode == 401 {
		err = SessionExpiredError
		return
	}
	body, err = ioutil.ReadAll(res.Body)
	if res.StatusCode/100 != 2 {
		var messages []ForceError
		json.Unmarshal(body, &messages)
		err = errors.New(messages[0].Message)
		return
	}
	return
}

// POST

func (f *Force) httpPostCSV(url string, data string, refreshed bool) (body []byte, err error) {
	body, err = f.httpPostWithContentType(url, data, "text/csv")
	if err == SessionExpiredError {
		if f.Credentials.RefreshToken != "" && !refreshed {
			if e := f.RefreshSession(); e != nil {
				return nil, e
			}
			return f.httpPostCSV(url, data, true)
		}
		return nil, err
	}
	return
}

func (f *Force) httpPostXML(url string, data string, refreshed bool) (body []byte, err error) {
	body, err = f.httpPostWithContentType(url, data, "application/xml")
	if err == SessionExpiredError {
		if f.Credentials.RefreshToken != "" && !refreshed {
			if e := f.RefreshSession(); e != nil {
				return nil, e
			}
			return f.httpPostXML(url, data, true)
		}
		return nil, err
	}
	return
}

func (f *Force) httpPostJSON(url string, data string, refreshed bool) (body []byte, err error) {
	body, err = f.httpPostWithContentType(url, data, "application/json")
	if err == SessionExpiredError {
		if f.Credentials.RefreshToken != "" && !refreshed {
			if e := f.RefreshSession(); e != nil {
				return nil, e
			}
			return f.httpPostJSON(url, data, true)
		}
		return nil, err
	}
	return
}

func (f *Force) httpPostZIPJSON(url string, data string, refreshed bool) (body []byte, err error) {
	body, err = f.httpPostWithContentType(url, data, "zip/json")
	if err == SessionExpiredError {
		if f.Credentials.RefreshToken != "" && !refreshed {
			if e := f.RefreshSession(); e != nil {
				return nil, e
			}
			return f.httpPostZIPJSON(url, data, true)
		}
		return nil, err
	}
	return
}

func (f *Force) httpPostWithContentType(url string, data string, contenttype string) (body []byte, err error) {
	body, err = f.httpPutPatchPostWithContentType(url, data, contenttype, "POST")
	return
}

func (f *Force) httpPost(url string, attrs map[string]string, refreshed bool) (body []byte, err error, emessages []ForceError) {
	body, err, emessages = f.httpPostAttributes(url, attrs)
	if err == SessionExpiredError {
		if f.Credentials.RefreshToken != "" && !refreshed {
			if e := f.RefreshSession(); e != nil {
				return nil, e, nil
			}
			return f.httpPost(url, attrs, true)
		}
		return nil, err, nil
	}
	return
}

func (f *Force) httpPostAttributes(url string, attrs map[string]string) (body []byte, err error, emessages []ForceError) {
	rbody, _ := json.Marshal(attrs)

	req, err := httpRequest("POST", url, bytes.NewReader(rbody))
	if err != nil {
		return
	}

	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", f.Credentials.AccessToken))
	req.Header.Add("X-SFDC-Session", fmt.Sprintf("Bearer %s", f.Credentials.AccessToken))
	req.Header.Add("Content-Type", "application/json")
	res, err := doRequest(req)
	if err != nil {
		return
	}
	defer res.Body.Close()

	if res.StatusCode == 401 {
		err = SessionExpiredError
		return
	}

	body, err = ioutil.ReadAll(res.Body)
	if res.StatusCode/100 != 2 {
		var messages []ForceError
		json.Unmarshal(body, &messages)
		err = errors.New(messages[0].Message)
		emessages = messages
		return
	}

	return
}

// PUT/PATCH/POST

func (f *Force) httpPutPatchPostWithContentType(url string, data string, contenttype string, method string) (body []byte, err error) {
	rbody := data

	req, err := httpRequest(strings.ToUpper(method), url, bytes.NewReader([]byte(rbody)))
	if err != nil {
		return
	}

	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", f.Credentials.AccessToken))
	req.Header.Add("X-SFDC-Session", fmt.Sprintf("Bearer %s", f.Credentials.AccessToken))
	req.Header.Add("Content-Type", contenttype)
	res, err := doRequest(req)
	if err != nil {
		return
	}
	defer res.Body.Close()

	if res.StatusCode == 401 {
		err = SessionExpiredError
		return
	}

	body, err = ioutil.ReadAll(res.Body)
	if err != nil {
		return
	}

	if int(res.StatusCode/100) != 2 {
		if contenttype == "application/xml" {
			var fault LoginFault
			xml.Unmarshal(body, &fault)
			if fault.ExceptionCode == "InvalidSessionId" {
				err = SessionExpiredError
			}
		} else {
			var messages []ForceError
			if e := json.Unmarshal(body, &messages); e != nil {
				var fault LoginFault
				json.Unmarshal(body, &fault)
				if fault.ExceptionCode == "InvalidSessionId" {
					err = SessionExpiredError
				} else {
					err = fmt.Errorf("%s: %s", fault.ExceptionCode, fault.ExceptionMessage)
				}
			} else {
				for _, e := range messages {
					sb := strings.Builder{}
					sb.WriteString(fmt.Sprintf("[Code: %s]: Message: \"%s\"", e.ErrorCode, e.Message))
					if len(e.Fields) > 0 {
						sb.WriteString(fmt.Sprintf(", Fields: %v", e.Fields))
					}
					err = multierr.Append(err, errors.New(sb.String()))
				}
			}
		}
		return
	} else if res.StatusCode == 204 {
		body = []byte("Patch command succeeded....")
	}

	return
}

// DELETE

func (f *Force) httpDelete(url string, refreshed bool) (body []byte, err error) {
	body, err = f.httpDeleteUrl(url)
	if err == SessionExpiredError {
		if f.Credentials.RefreshToken != "" && !refreshed {
			if e := f.RefreshSession(); e != nil {
				return nil, e
			}
			return f.httpDelete(url, true)
		}
		return nil, err
	}
	return
}

func (f *Force) httpDeleteUrl(url string) (body []byte, err error) {
	req, err := httpRequest("DELETE", url, nil)
	if err != nil {
		return
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", f.Credentials.AccessToken))
	req.Header.Add("X-SFDC-Session", fmt.Sprintf("Bearer %s", f.Credentials.AccessToken))
	res, err := doRequest(req)
	if err != nil {
		return
	}
	defer res.Body.Close()
	if res.StatusCode == 401 {
		err = SessionExpiredError
		return
	}
	body, err = ioutil.ReadAll(res.Body)
	if res.StatusCode/100 != 2 {
		var messages []ForceError
		json.Unmarshal(body, &messages)
		err = errors.New(messages[0].Message)
		return
	}
	return
}

// HTTP

func httpError() (request *http.Request, err error) {
	return nil, nil
}

func httpRequest(method, url string, body io.Reader) (request *http.Request, err error) {
	request, err = http.NewRequest(method, url, body)
	if err != nil {
		return
	}
	request.Header.Add("User-Agent", fmt.Sprintf("gforce (%s-%s)", runtime.GOOS, runtime.GOARCH))
	return
}

func doRequest(req *http.Request) (res *http.Response, err error) {
	t := &transport{}

	// TODO: Resolver BUG
	// BUG: Quando DELETE ocorre erro de nil pointer
	if traceHTTPRequest && req.Method != http.MethodDelete {
		trace := &httptrace.ClientTrace{
			GotConn: t.GotConn,
		}
		req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	}

	client := &http.Client{
		Timeout:   time.Duration(Timeout) * time.Millisecond,
		Transport: t,
	}

	return client.Do(req)
}

// transport is an http.RoundTripper that keeps track of the in-flight
// request and implements hooks to report HTTP tracing events.
type transport struct {
	current *http.Request
}

// RoundTrip wraps http.DefaultTransport.RoundTrip to keep track
// of the current request.
func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.current = req
	return http.DefaultTransport.RoundTrip(req)
}

// GotConn prints whether the connection has been used previously
// for the current request.
func (t *transport) GotConn(info httptrace.GotConnInfo) {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("HTTP -> %s, Reused? %t\n", t.current.URL, info.Reused))
	if traceHTTPRequestDetail {
		sb.WriteString(fmt.Sprintf("%s %s %s\n", t.current.Method, t.current.URL.Path, t.current.Proto))
		sb.WriteString(fmt.Sprintf("Host: %s\n", t.current.URL.Host))
		for k, v := range t.current.Header {
			sb.WriteString(fmt.Sprintf("%s: %v\n", k, v))
		}
		var buf bytes.Buffer
		tee := io.TeeReader(t.current.Body, &buf)
		t.current.Body = ioutil.NopCloser(&buf)
		t.current.Body.Close()
		b, err := ioutil.ReadAll(tee)
		if len(b) > 0 && err == nil {
			sb.WriteString(fmt.Sprintf("Body: \n%s\n", string(b)))
		}
		sb.WriteString(fmt.Sprint("---"))
	}
	log.Println(sb.String())
}
