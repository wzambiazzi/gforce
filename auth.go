package gforce

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"strings"
)

func (f *Force) userInfo() (userinfo UserInfo, err error) {
	url := fmt.Sprintf("%s/services/oauth2/userinfo", f.Credentials.InstanceUrl)

	login, err := f.httpGet(url, false)
	if err != nil {
		return
	}

	err = json.Unmarshal([]byte(login), &userinfo)

	return
}

func getUserInfo(creds ForceSession) (userinfo UserInfo, err error) {
	force := NewForce(&creds)

	userinfo, err = force.userInfo()
	if err != nil {
		return
	}

	me, err := force.GetRecord("User", userinfo.UserId)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Problem getting user data, continuing...")
		err = nil
	}

	userinfo.ProfileId = fmt.Sprintf("%s", me["ProfileId"])

	namespace, err := force.getOrgNamespace()
	if err == nil {
		userinfo.OrgNamespace = namespace
	} else {
		fmt.Fprintf(os.Stderr, "Your profile does not have Modify All Data enabled. Functionallity will be limited.\n")
		err = nil
	}

	return
}

func (f *Force) getOrgNamespace() (namespace string, err error) {
	describe, err := f.Metadata.DescribeMetadata()
	if err != nil {
		return
	}

	namespace = describe.NamespacePrefix

	return
}

func (creds *ForceSession) SessionName() string {
	sessionName := creds.UserInfo.UserName

	if creds.SessionOptions.Alias != "" {
		sessionName = creds.SessionOptions.Alias
	}

	return sessionName
}

func (f *Force) UpdateCredentials(creds ForceSession) {
	log.Printf("UpdateCredentials creds: %+v", creds)
	f.Credentials.AccessToken = creds.AccessToken
	f.Credentials.IssuedAt = creds.IssuedAt
	f.Credentials.InstanceUrl = creds.InstanceUrl
	f.Credentials.Scope = creds.Scope
}

// Add UserInfo and SessionOptions to old ForceSession
func upgradeCredentials(creds *ForceSession) (err error) {
	if creds.SessionOptions != nil && creds.UserInfo != nil {
		return
	}

	if creds.SessionOptions == nil {
		creds.SessionOptions = &SessionOptions{
			ApiVersion: ApiVersionNumber(),
		}

		if creds.RefreshToken != "" {
			creds.SessionOptions.RefreshMethod = RefreshOauth
		}
	}

	if creds.UserInfo == nil || creds.UserInfo.UserName == "" {
		force := NewForce(creds)

		err = force.RefreshSession()
		if err != nil {
			return
		}

		var userinfo UserInfo
		userinfo, err = getUserInfo(*creds)
		if err != nil {
			return
		}

		creds.UserInfo = &userinfo
	}

	return
}

func GetAccessAuthorization(code, redirect_uri, client_id, client_secret, endpointURL string) (result ForceSession, err error) {
	if len(code) == 0 {
		return result, errors.New("code is blank")
	}

	if len(redirect_uri) == 0 {
		return result, errors.New("redirect_uri is blank")
	}

	if len(client_id) == 0 {
		return result, errors.New("client_id is blank")
	}

	if len(client_secret) == 0 {
		return result, errors.New("client_secret is blank")
	}

	v := url.Values{}
	v.Set("grant_type", "authorization_code")
	v.Set("code", code)
	v.Set("redirect_uri", redirect_uri)
	v.Set("client_id", client_id)
	v.Set("client_secret", client_secret)

	postVars := v.Encode()
	uri := fmt.Sprintf("%s/services/oauth2/token", endpointURL)

	req, err := httpRequest("POST", uri, bytes.NewReader([]byte(postVars)))
	if err != nil {
		return
	}

	// basicEncoded := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", client_id, client_secret)))
	// authorization := fmt.Sprintf("Basic %s", basicEncoded)

	req.Header.Add("Accept", "application/json")
	// req.Header.Add("Authorization", authorization)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	res, err := doRequest(req)
	if err != nil {
		return
	}
	defer res.Body.Close()

	if res.StatusCode == 401 {
		err = SessionExpiredError
		return
	}

	body, err := ioutil.ReadAll(res.Body)

	if res.StatusCode/100 != 2 {
		var errMsgs OAuthError
		json.Unmarshal(body, &errMsgs)
		err = fmt.Errorf("(%d) %s: %s", res.StatusCode, errMsgs.Error, errMsgs.ErrorDescription)
		return
	}

	if err != nil {
		return
	}

	json.Unmarshal(body, &result)

	u, err := url.Parse(result.UserId)
	if err != nil {
		return
	}
	s := strings.Split(u.Path, "/")
	result.UserId = s[len(s)-1]

	log.Printf("Session returned of GetAccessAuthorization: %+v", result)

	return result, nil
}
