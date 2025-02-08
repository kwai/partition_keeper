package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

type CheckHttpResp func(httpResp *http.Response, response interface{}) error

func DefaultCheckResp(httpResp *http.Response, response interface{}) error {
	if httpResp.StatusCode != 200 {
		return fmt.Errorf("req got http status error: %s", httpResp.Status)
	}
	return json.NewDecoder(httpResp.Body).Decode(response)
	// data, err := ioutil.ReadAll(httpResp.Body)
	// if err != nil {
	// 	return err
	// }
	// logging.Info("%s", string(data))
	// return json.Unmarshal(data, response)
}

func HttpPostJson(
	url string,
	header, params map[string]string,
	request interface{},
	response interface{},
) error {
	data, err := json.Marshal(request)
	if err != nil {
		return err
	}
	reader := bytes.NewBuffer(data)

	buf := parserParams(params)
	req, err := http.NewRequest("POST", url+buf, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for key, value := range header {
		req.Header.Set(key, value)
	}
	return doRequest(req, response, DefaultCheckResp)
}

func HttpPostStream(
	url string,
	params map[string]string,
	request []byte,
	response interface{},
	check CheckHttpResp,
) error {
	buf := parserParams(params)

	reader := bytes.NewBuffer(request)
	resp, err := http.Post(url+buf, "application/octet-stream", reader)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return check(resp, response)
}

func HttpPutStream(
	url string,
	params map[string]string,
	request []byte,
	response interface{},
	check CheckHttpResp,
) error {
	buf := parserParams(params)
	reader := bytes.NewBuffer(request)

	req, err := http.NewRequest("PUT", url+buf, reader)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/octet-stream")

	return doRequest(req, response, check)
}

func HttpDelete(
	url string,
	params map[string]string,
	response interface{},
	check CheckHttpResp,
) error {
	buf := parserParams(params)
	req, err := http.NewRequest("DELETE", url+buf, nil)
	if err != nil {
		return err
	}
	return doRequest(req, response, check)
}

func HttpGet(
	url string,
	headers, params map[string]string,
	response interface{},
	check CheckHttpResp,
) error {
	buf := parserParams(params)
	req, err := http.NewRequest("GET", url+buf, nil)
	if err != nil {
		return err
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	return doRequest(req, response, check)
}

func parserParams(params map[string]string) string {
	if len(params) == 0 {
		return ""
	}
	values := url.Values{}
	for key, value := range params {
		values.Set(key, value)
	}
	return "?" + values.Encode()
}

func doRequest(req *http.Request, response interface{}, check CheckHttpResp) error {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return check(resp, response)
}
