package collectorutils

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"reflect"
	"strconv"

	"github.com/elastic/go-elasticsearch"
	es "github.com/elastic/go-elasticsearch"
	esapi "github.com/elastic/go-elasticsearch/esapi"
)

const (
	Native = 1
	Cadence = 2
	Event = 2
)

type PostReqHandler struct {
	ESClient *es.Client
	Filter   Filter
	Enrich   Enrich
	Config   Config
	MDTPaths MDTPaths
	Mode     int
}

type ESmetaData struct {
	Index struct {
		IndexName string `json:"_index"`
	} `json:"index"`
}

type Config struct {
	ESHost       string `json:"ESHost"`
	ESPort       string `json:"ESPort"`
	MDTPathsFile string `json:"MDTPathsFile"`
	ESIndex      string `json:"ESIndex"`
	FilterFile   string `json:"FilterFile"`
	EnrichFile   string `json:"EnrichFile"`
}

type Path []struct {
	Node []struct {
		NodeName  string `json:"NodeName"`
		ToDive    bool   `json:"ToDive"`
		ToCombine bool   `json:"ToCombine"`
	} `json:"Node"`
}

type MDTPaths map[string][]Path

type PathDefinitions []PathDefinition

type PathDefinition struct {
	Key   string `json:"key"`
	Paths []struct {
		Path string `json:"path"`
	} `json:"paths"`
}

type Filter []struct {
	Item string `json:"item"`
}

type Enrich []struct {
	Item struct {
		ItemID   string `json:"itemId"`
		Mappings []struct {
			Name  string `json:"name"`
			Value int    `json:"value"`
		} `json:"mappings"`
	} `json:"item"`
}

type ESClient *es.Client

func ESConnect(ipaddr string, port string) (*es.Client, error) {

	var fulladdress string = "http://" + ipaddr + ":" + port

	cfg := elasticsearch.Config{
		Addresses: []string{
			fulladdress,
		},
	}

	es, _ := elasticsearch.NewClient(cfg)

	return es, nil
}

func ESPush(esClient *es.Client, indexName string, buf []map[string]interface{}) []byte {

	JSONmetaData := `{"index":{"_index":"` + indexName + `"}}`

	JSONRequestData := make([]byte, 0)

	for _, v := range buf {
		JSONData, err := json.Marshal(v)
		if err != nil {
			log.Println(err)
		}

		JSONRequestData = append(JSONRequestData, JSONmetaData...)
		JSONRequestData = append(JSONRequestData, []byte("\n")...)
		JSONRequestData = append(JSONRequestData, JSONData...)
		JSONRequestData = append(JSONRequestData, []byte("\n")...)
	}

	bulkRequest := esapi.BulkRequest{
		Index: indexName,
		Body:  bytes.NewBuffer(JSONRequestData),
	}

	res, err := bulkRequest.Do(context.Background(), esClient)

	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	return JSONRequestData
}

func ToNum(v interface{}) interface{} {
	if reflect.ValueOf(v).Type().Kind() == reflect.String {
		if i, err := strconv.ParseInt(v.(string), 10, 64); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(v.(string), 64); err == nil {
			return f
		}
	}
	return v
}

func FilterMap(src map[string]interface{}, filter Filter) {
	for _, v := range filter {
		if _, ok := src[v.Item]; ok {
			delete(src, v.Item)
		}
	}
}

func EnrichMap(src map[string]interface{}, enrich Enrich) {
	for _, v := range enrich {
		if _, ok := src[v.Item.ItemID]; ok {
			for _, mV := range v.Item.Mappings {
				if src[v.Item.ItemID] == mV.Name {
					src[v.Item.ItemID+"/code"] = mV.Value
				}
			}
		}
	}
}

func PrettyPrint(src map[string]interface{}) {
	empJSON, err := json.MarshalIndent(src, "", "  ")
	if err != nil {
		log.Fatalf(err.Error())
	}
	fmt.Printf("Pretty processed output %s\n", string(empJSON))
}

func CopySlice(sli []string) []string {
	newSli := make([]string, len(sli))
	copy(newSli, sli)
	return newSli
}

func CopyMap(ma map[string]interface{}) map[string]interface{} {
	newMap := make(map[string]interface{})
	for k, v := range ma {
		newMap[k] = v
	}
	return newMap
}

func FlattenMap(src map[string]interface{}, path Path, pathIndex int, pathPassed []string, mode int, header map[string]interface{}, buf *[]map[string]interface{}, filter Filter, enrich Enrich) {
	keysDive := make([]string, 0)
	keysPass := make([]string, 0)
	keysCombine := make([]string, 0)
	pathPassed = CopySlice(pathPassed)
	for k, v := range src {
		switch sType := reflect.ValueOf(v).Type().Kind(); sType {
		case reflect.String:
			if len(pathPassed) == 0 {
				header[k] = ToNum(v)
			} else if len(pathPassed) == 1 {
				header[pathPassed[0]+"."+k] = ToNum(v)
			} else {
				header[pathPassed[len(pathPassed)-mode]+"."+k] = ToNum(v)
			}
			
		case reflect.Float64:
			if len(pathPassed) == 0 {
				header[k] = ToNum(v)
			} else if len(pathPassed) == 1 {
				header[pathPassed[0]+"."+k] = ToNum(v)
			} else {
				header[pathPassed[len(pathPassed)-mode]+"."+k] = v.(float64)
			}		

		default:
			if pathIndex < len(path) {
				for _, v := range path[pathIndex].Node {
					if v.NodeName == k || v.NodeName == "any" {
						if v.ToDive {
							keysDive = append(keysDive, k)
						} else if v.ToCombine {
							keysCombine = append(keysCombine, k)
						} else {
							keysPass = append(keysPass, k)
						}
					}
				}
			}
		}
	}

	if pathIndex == len(path) {
		for _, v := range path[pathIndex-1].Node {
			if pathPassed[len(pathPassed)-1] == v.NodeName && !v.ToCombine {
				newHeader := CopyMap(header)
				FilterMap(newHeader, filter)
				EnrichMap(newHeader, enrich)
				PrettyPrint(newHeader)
				*buf = append(*buf, newHeader)
			}
		}
	} else {
		keys := make([]string, 0)
		keys = append(keysDive, keysCombine...)
		keys = append(keys, keysPass...)

		if pathIndex < len(path) {
			for _, k := range keys {
				pathPassed = append(pathPassed, k)
				switch sType := reflect.ValueOf(src[k]).Type().Kind(); sType {
				case reflect.Map:
					src := src[k].(map[string]interface{})
					FlattenMap(src, path, pathIndex+1, pathPassed, mode, header, buf, filter, enrich)
				case reflect.Slice:
					src := reflect.ValueOf(src[k])
					for i := 0; i < src.Len(); i++ {
						src := src.Index(i).Interface().(map[string]interface{})
						FlattenMap(src, path, pathIndex+1, pathPassed, mode, header, buf, filter, enrich)
					}
				}
			}
		}
	}
}

func LoadMDTPaths(fileName string) MDTPaths {

	var PathDefinitions PathDefinitions
	MDTPaths := make(MDTPaths)

	MDTPathDefinitionsFile, err := os.Open(fileName)
	if err != nil {
		fmt.Println(err)
	}
	defer MDTPathDefinitionsFile.Close()

	MDTPathDefinitionsFileBytes, err := ioutil.ReadAll(MDTPathDefinitionsFile)
	if err != nil {
		fmt.Println(err)
	}

	err = json.Unmarshal(MDTPathDefinitionsFileBytes, &PathDefinitions)
	if err != nil {
		fmt.Println(err)
	}

	for _, v := range PathDefinitions {
		var paths []Path

		for _, v := range v.Paths {
			pathFile, err := os.Open(v.Path)
			if err != nil {
				fmt.Println(err)
			}
			defer pathFile.Close()

			pathFileBytes, _ := ioutil.ReadAll(pathFile)
			var path Path
			err = json.Unmarshal(pathFileBytes, &path)
			if err != nil {
				fmt.Println(err)
			}
			paths = append(paths, path)
		}

		MDTPaths[v.Key] = paths
	}

	return MDTPaths
}

func Initialize(configFile string) (*es.Client, Config, MDTPaths, Filter, Enrich) {

	var Config Config
	var MDTPaths MDTPaths
	var Filter Filter
	var Enrich Enrich

	ConfigFile, err := os.Open(configFile)
	if err != nil {
		fmt.Println(err)
	}
	defer ConfigFile.Close()

	ConfigFileBytes, _ := ioutil.ReadAll(ConfigFile)

	err = json.Unmarshal(ConfigFileBytes, &Config)
	if err != nil {
		fmt.Println(err)
	}

	MDTPaths = LoadMDTPaths(Config.MDTPathsFile)

	FilterFile, err := os.Open(Config.FilterFile)
	if err != nil {
		fmt.Println(err)
	}
	defer FilterFile.Close()

	FilterFileBytes, _ := ioutil.ReadAll(FilterFile)

	err = json.Unmarshal(FilterFileBytes, &Filter)
	if err != nil {
		fmt.Println(err)
	}

	EnrichFile, err := os.Open(Config.EnrichFile)
	if err != nil {
		fmt.Println(err)
	}
	defer ConfigFile.Close()

	EnrichFileBytes, _ := ioutil.ReadAll(EnrichFile)

	err = json.Unmarshal(EnrichFileBytes, &Enrich)
	if err != nil {
		fmt.Println(err)
	}

	esClient, error := ESConnect(Config.ESHost, Config.ESPort)
	if error != nil {
		log.Fatalf("error: %s", error)
	}

	return esClient, Config, MDTPaths, Filter, Enrich
}

func GetHttpBody(httpRequest *http.Request) map[string]interface{} {
	src := make(map[string]interface{})
	if httpRequest.Method != "POST" {
		fmt.Println("Is not POST method")
	} else {
		bodyBytes, _ := ioutil.ReadAll(httpRequest.Body)
		err := json.Unmarshal(bodyBytes, &src)
		if err != nil {
			panic(err)
		}
	}

	return src
}
