package gforce

import (
	"fmt"
)

var apiVersionNumber = "46.0"
var apiVersion = fmt.Sprintf("v%s", apiVersionNumber)

func ApiVersion() string {
	return apiVersion
}

func ApiVersionNumber() string {
	return apiVersionNumber
}

func (f *Force) UpdateApiVersion(version string) (err error) {
	SetApiVersion(version)
	f.Credentials.SessionOptions.ApiVersion = version
	return
}

func SetApiVersion(version string) {
	apiVersion = "v" + version
	apiVersionNumber = version
}
