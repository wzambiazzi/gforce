package gforce

import (
	"encoding/xml"
	"errors"
	"fmt"
)

type ForcePartner struct {
	Force *Force
}

func NewForcePartner(force *Force) (partner *ForcePartner) {
	partner = &ForcePartner{Force: force}
	return
}

func (partner *ForcePartner) CheckStatus(id string) (err error) {
	body, err := partner.soapExecute("checkStatus", fmt.Sprintf("<id>%s</id>", id))
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
		return partner.CheckStatus(id)
	case status.State == "Error":
		return errors.New(status.Message)
	}

	return
}

func (partner *ForcePartner) soapExecute(action, query string) (response []byte, err error) {
	url := fmt.Sprintf("%s/services/Soap/s/%s/%s", partner.Force.Credentials.InstanceUrl, partner.Force.Credentials.SessionOptions.ApiVersion, partner.Force.Credentials.UserInfo.OrgId)
	soap := NewSoap(url, "http://soap.sforce.com/2006/08/apex", partner.Force.Credentials.AccessToken)
	soap.Header = "<apex:DebuggingHeader><apex:debugLevel>DEBUGONLY</apex:debugLevel></apex:DebuggingHeader>"

	response, err = soap.Execute(action, query)

	if err == SessionExpiredError {
		partner.Force.RefreshSession()
		return partner.soapExecute(action, query)
	}

	return
}
