package gforce

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/dgrijalva/jwt-go"
	"google.golang.org/api/option"
	"source.cloud.google.com/grendene-crm-prod/gforce/keystore"
)

// GetServerAuthorization func
func GetServerAuthorization(orgID, clientID, userMail, authURL, endpointURL string) (result ForceSession, err error) {
	token, err := generateNewCertToken(orgID, clientID, authURL, userMail)
	if err != nil {
		return result, err
	}

	uri := fmt.Sprintf("%s/services/oauth2/token", endpointURL)
	v := url.Values{}
	v.Set("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
	v.Set("assertion", token)

	postVars := v.Encode()

	req, err := httpRequest("POST", uri, bytes.NewReader([]byte(postVars)))
	if err != nil {
		return
	}

	req.Header.Add("Accept", "application/json")
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

	return result, nil
}

func generateNewCertToken(orgID, clientID, loginURL, userMail string) (accessCode string, err error) {
	jksCert, err := getJKSFile(orgID)
	if err != nil {
		return accessCode, err
	}

	password := []byte(os.Getenv("JKS_PASSWORD"))
	ks := readKeyStore(jksCert, password)
	entry := ks["job_certificate"]
	privateKeyEntry := entry.(*keystore.PrivateKeyEntry)
	jwtKey, err := x509.ParsePKCS8PrivateKey(privateKeyEntry.PrivateKey)
	if err != nil {
		return accessCode, fmt.Errorf("Error on Parse Private Key: %w", err)
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss": clientID,
		"aud": loginURL,
		"sub": userMail,
		"exp": time.Now().Add(3 * time.Minute).Unix(),
	})

	tokenSigned, err := token.SignedString(jwtKey)
	if err != nil {
		return accessCode, err
	}
	return tokenSigned, nil

}

func getJKSFile(orgID string) ([]byte, error) {
	ctx := context.Background()
	credentialsPath := os.Getenv("STORAGE_CREDENTIALS")
	client, err := storage.NewClient(ctx, option.WithCredentialsFile(credentialsPath))
	if err != nil {
		return nil, fmt.Errorf("Error to create a Google Cloud Storage Client: %w", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	objectSrc := fmt.Sprintf("%s/cert.jks", orgID)

	jksReader, err := client.Bucket(os.Getenv("JKS_BUCKET")).Object(objectSrc).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("Error to get cert file JKS from Google Cloud Storage Bucket: %w", err)
	}
	defer jksReader.Close()

	fileContent, err := ioutil.ReadAll(jksReader)
	if err != nil {
		return nil, fmt.Errorf("Error to reading JKS File from Google Cloud Storage Bucket: %w", err)
	}
	return fileContent, nil
}

func readKeyStore(content []byte, password []byte) keystore.KeyStore {
	jks := bytes.NewReader(content)

	keyStore, err := keystore.Decode(jks, password)
	if err != nil {
		log.Fatal(err)
	}

	return keyStore
}
