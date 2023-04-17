package gforce

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/url"
)

func (f *Force) refreshOauth() (err error) {
	var refreshAttempts int = 3
	attrs := url.Values{}
	attrs.Set("grant_type", "refresh_token")
	attrs.Set("refresh_token", f.Credentials.RefreshToken)
	attrs.Set("client_id", ClientId)
	if f.Credentials.ClientId != "" {
		attrs.Set("client_id", f.Credentials.ClientId)
	}

	log.Printf("Vars to refreshOauth: refresh_token: %v, client_id: %v f.Credentials.ClientID: %v", f.Credentials.RefreshToken, ClientId, f.Credentials.ClientId)

	postVars := attrs.Encode()

	endpoint, err := f.refreshTokenURL()
	if err != nil {
		return err
	}

	req, err := httpRequest("POST", endpoint, bytes.NewReader([]byte(postVars)))
	if err != nil {
		return err
	}
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	log.Printf("Init attempt of Refresh Token")

	//Attempt 3 times to refresh token before return error
	for i := 0; i < refreshAttempts; i++ {
		res, err := doRequest(req)
		if err != nil {
			// logger.Errorf("Error on Refresh Token Request: %w", err)
			// log.Println(fmt.Errorf("Error on Refresh Token Request: %w", err))
			log.Printf("Error on Refresh Token Request: %w", err)
			continue
		}
		defer res.Body.Close()

		if res.StatusCode != 200 {
			body, err := ioutil.ReadAll(res.Body)
			if err != nil {
				return err
			}
			// logger.Errorf("Error on Refresh Token SF: Status (%d) - Body (%v)", res.StatusCode, res.Body)
			log.Printf("Error on Refresh Token SF: Status (%d) - Body (%v)", res.StatusCode, string(body))
			// log.Println(fmt.Sprintf("Error on Refresh Token SF: Status (%d) - Body (%v)", res.StatusCode, res.Body))
			continue
		}
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			log.Printf("Error on Parse Token Body: %w", err)
			// logger.Errorf("Error on Parse Token Body: %w", err)
			// log.Println(fmt.Errorf("Error on Parse Token Body: %w", err))
		}

		log.Printf("Body on RefreshoAuth Method: %v", string(body))

		var result ForceSession
		json.Unmarshal(body, &result)
		f.UpdateCredentials(result)
		break
	}

	return nil
}

//RefreshSession method
func (f *Force) RefreshSession() (err error) {
	log.Printf("Method RefreshSession: RefreshMethod: %+v", f.Credentials.SessionOptions.RefreshMethod)
	if f.Credentials.SessionOptions.RefreshMethod == RefreshOauth {
		err = f.refreshOauth()
	} else {
		err = errors.New("Unable to refresh")
	}

	log.Printf("Return of refreshOAuth: %w", err)

	if err == nil {
		f.Credentials.SessionRefreshed = true
	}

	return
}
