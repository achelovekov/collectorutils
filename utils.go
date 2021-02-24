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

type postReqHandler struct {
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

type MDTPathDefinitions []struct {
	MdtPath      string `json:"mdtPath"`
	MdtPathFiles []struct {
		MdtPathFile string `json:"mdtPathFile"`
	} `json:"mdtPathFiles"`
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

func esConnect(ipaddr string, port string) (*es.Client, error) {

	var fulladdress string = "http://" + ipaddr + ":" + port

	cfg := elasticsearch.Config{
		Addresses: []string{
			fulladdress,
		},
	}

	es, _ := elasticsearch.NewClient(cfg)

	return es, nil
}

func esPush(esClient *es.Client, indexName string, buf []map[string]interface{}) []byte {

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

	fmt.Println(res)

	return JSONRequestData
}

func toNum(v interface{}) interface{} {
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

func filterMap(src map[string]interface{}, filter Filter) {
	for _, v := range filter {
		if _, ok := src[v.Item]; ok {
			delete(src, v.Item)
			//fmt.Printf("SUCCESSFULY DELETED - %v\n", v.Item)
		}
	}
}

func enrichMap(src map[string]interface{}, enrich Enrich) {
	for _, v := range enrich {
		if _, ok := src[v.Item.ItemID]; ok {
			for _, mV := range v.Item.Mappings {
				if src[v.Item.ItemID] == mV.Name {
					src[v.Item.ItemID+"/code"] = mV.Value
					//fmt.Printf("SUCCESSFULY ENRICHED - %v\n", v.Item.ItemID)
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

func copySlice(sli []string) []string {
	newSli := make([]string, len(sli))
	copy(newSli, sli)
	return newSli
}

func copyMap(ma map[string]interface{}) map[string]interface{} {
	newMap := make(map[string]interface{})
	for k, v := range ma {
		newMap[k] = v
	}
	return newMap
}

func flattenMap(src map[string]interface{}, path Path, pathIndex int, pathPassed []string, mode int, header map[string]interface{}, buf *[]map[string]interface{}, filter Filter, enrich Enrich) {
	//fmt.Printf("pathPassed: %v\n", pathPassed)
	keysDive := make([]string, 0)
	keysPass := make([]string, 0)
	keysCombine := make([]string, 0)
	pathPassed = copySlice(pathPassed)
	for k, v := range src {
		switch sType := reflect.ValueOf(v).Type().Kind(); sType {
		case reflect.String:
			if len(pathPassed) == 0 {
				header[k] = toNum(v)
			} else if len(pathPassed) == 1 {
				header[pathPassed[0]+"."+k] = toNum(v)
			} else {
				header[pathPassed[len(pathPassed)-mode]+"."+k] = toNum(v)
			}
			
		case reflect.Float64:
			if len(pathPassed) == 0 {
				header[k] = toNum(v)
			} else if len(pathPassed) == 1 {
				header[pathPassed[0]+"."+k] = toNum(v)
			} else {
				//fmt.Printf("len(pathPassed): %v, mode: %v\n", len(pathPassed), mode)
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
				newHeader := copyMap(header)
				filterMap(newHeader, filter)
				enrichMap(newHeader, enrich)
				//PrettyPrint(newHeader)
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
					flattenMap(src, path, pathIndex+1, pathPassed, mode, header, buf, filter, enrich)
				case reflect.Slice:
					src := reflect.ValueOf(src[k])
					for i := 0; i < src.Len(); i++ {
						src := src.Index(i).Interface().(map[string]interface{})
						flattenMap(src, path, pathIndex+1, pathPassed, mode, header, buf, filter, enrich)
					}
				}
			}
		}
	}
}

func worker(src map[string]interface{}, ESClient *es.Client, ESIndex string, path Path, mode int, filter Filter, enrich Enrich) {
		var pathIndex int
		header := make(map[string]interface{})
		buf := make([]map[string]interface{}, 0)
		pathPassed := make([]string, 0)

		//PrettyPrint(src)

		flattenMap(src, path, pathIndex, pathPassed, mode, header, &buf, filter, enrich)
		esPush(ESClient, ESIndex, buf)
}

func LoadMDTPaths(fileName string) MDTPaths {

	var MDTPathDefinitions MDTPathDefinitions
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

	err = json.Unmarshal(MDTPathDefinitionsFileBytes, &MDTPathDefinitions)
	if err != nil {
		fmt.Println(err)
	}

	for _, v := range MDTPathDefinitions {
		var paths []Path

		for _, v := range v.MdtPathFiles {
			pathFile, err := os.Open(v.MdtPathFile)
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

		MDTPaths[v.MdtPath] = paths
	}

	return MDTPaths
}

func initialize(configFile string) (*es.Client, Config, MDTPaths, Filter, Enrich) {

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

	esClient, error := esConnect(Config.ESHost, Config.ESPort)
	if error != nil {
		log.Fatalf("error: %s", error)
	}

	return esClient, Config, MDTPaths, Filter, Enrich
}

func getHttpBody(httpRequest *http.Request) map[string]interface{} {
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


func (prh *postReqHandler) SysBgp(w http.ResponseWriter, httpRequest *http.Request) {
	src := getHttpBody(httpRequest)	

	for i := range(prh.MDTPaths["sys/bgp"]) {
		src := copyMap(src)
		go worker(src, prh.ESClient, prh.Config.ESIndex, prh.MDTPaths["sys/bgp"][i], Cadence, prh.Filter, prh.Enrich)
	}	
}

func (prh *postReqHandler) SysOspf(w http.ResponseWriter, httpRequest *http.Request) {
	src := getHttpBody(httpRequest)	

	for i := range(prh.MDTPaths["sys/ospf"]) {
		src := copyMap(src)
		go worker(src, prh.ESClient, prh.Config.ESIndex, prh.MDTPaths["sys/ospf"][i], Cadence, prh.Filter, prh.Enrich)
	}	
}

func (prh *postReqHandler) RIBHandler(w http.ResponseWriter, httpRequest *http.Request) {
	src := getHttpBody(httpRequest)	

	for i := range(prh.MDTPaths["rib"]) {
		src := copyMap(src)
		go worker(src, prh.ESClient, prh.Config.ESIndex, prh.MDTPaths["rib"][i], Native, prh.Filter, prh.Enrich)
	}	
}

func (prh *postReqHandler) MacAllHandler(w http.ResponseWriter, httpRequest *http.Request) {
	src := getHttpBody(httpRequest)	

	for i := range(prh.MDTPaths["mac-all"]) {
		src := copyMap(src)
		go worker(src, prh.ESClient, prh.Config.ESIndex, prh.MDTPaths["mac-all"][i], Native, prh.Filter, prh.Enrich)
	}	
}

func (prh *postReqHandler) AdjacencyHandler(w http.ResponseWriter, httpRequest *http.Request) {
	src := getHttpBody(httpRequest)	

	for i := range(prh.MDTPaths["adjacency"]) {
		src := copyMap(src)
		go worker(src, prh.ESClient, prh.Config.ESIndex, prh.MDTPaths["adjacency"][i], Native, prh.Filter, prh.Enrich)
	}	
}

func (prh *postReqHandler) EventHandler(w http.ResponseWriter, httpRequest *http.Request) {
	src := getHttpBody(httpRequest)	

	for i := range(prh.MDTPaths["event"]) {
		src := copyMap(src)
		go worker(src, prh.ESClient, prh.Config.ESIndex, prh.MDTPaths["event"][i], Event, prh.Filter, prh.Enrich)
	}	
}

func (prh *postReqHandler) VxlanSysEps(w http.ResponseWriter, httpRequest *http.Request) {
	src := getHttpBody(httpRequest)	

	for i := range(prh.MDTPaths["vxlan:sys/eps"]) {
		src := copyMap(src)
		go worker(src, prh.ESClient, prh.Config.ESIndex, prh.MDTPaths["vxlan:sys/eps"][i], Cadence, prh.Filter, prh.Enrich)
	}	
}

func (prh *postReqHandler) VxlanSysBD(w http.ResponseWriter, httpRequest *http.Request) {
	src := getHttpBody(httpRequest)	

	for i := range(prh.MDTPaths["vxlan:sys/bd"]) {
		src := copyMap(src)
		go worker(src, prh.ESClient, prh.Config.ESIndex, prh.MDTPaths["vxlan:sys/bd"][i], Cadence, prh.Filter, prh.Enrich)
	}	
}

func (prh *postReqHandler) SysIntfHandler(w http.ResponseWriter, httpRequest *http.Request) {
	src := getHttpBody(httpRequest)	

	for i := range(prh.MDTPaths["interface:sys/intf"]) {
		src := copyMap(src)
		go worker(src, prh.ESClient, prh.Config.ESIndex, prh.MDTPaths["interface:sys/intf"][i], Cadence, prh.Filter, prh.Enrich)
	}	
}

func (prh *postReqHandler) SysChHandler(w http.ResponseWriter, httpRequest *http.Request) {
	src := getHttpBody(httpRequest)	

	for i := range(prh.MDTPaths["environment:sys/ch"]) {
		src := copyMap(src)
		go worker(src, prh.ESClient, prh.Config.ESIndex, prh.MDTPaths["environment:sys/ch"][i], Cadence, prh.Filter, prh.Enrich)
	}	
}

func (prh *postReqHandler) sysProcHandler(w http.ResponseWriter, httpRequest *http.Request) {
	src := getHttpBody(httpRequest)	

	for i := range(prh.MDTPaths["resources:sys/proc"]) {
		src := copyMap(src)
		go worker(src, prh.ESClient, prh.Config.ESIndex, prh.MDTPaths["resources:sys/proc"][i], Cadence, prh.Filter, prh.Enrich)
	}	
}

func (prh *postReqHandler) sysProcSysHandler(w http.ResponseWriter, httpRequest *http.Request) {
	src := getHttpBody(httpRequest)	

	for i := range(prh.MDTPaths["resources:sys/procsys"]) {
		src := copyMap(src)
		go worker(src, prh.ESClient, prh.Config.ESIndex, prh.MDTPaths["resources:sys/procsys"][i], Cadence, prh.Filter, prh.Enrich)
	}	
}

func main() {

	ESClient, Config, MDTPaths, Filter, Enrich := initialize("config.json")

	postReqHandler := &postReqHandler{ESClient: ESClient, Filter: Filter, Enrich: Enrich, Config: Config, MDTPaths: MDTPaths, Mode: 2}

	http.HandleFunc("/network/sys/bgp", postReqHandler.SysBgp)
/* 	http.HandleFunc("/network/sys/ospf", postReqHandler.SysOspf)
	http.HandleFunc("/network/rib", postReqHandler.RIBHandler)
	http.HandleFunc("/network/mac-all", postReqHandler.MacAllHandler)
	http.HandleFunc("/network/adjacency", postReqHandler.AdjacencyHandler)
	http.HandleFunc("/network/EVENT-LIST", postReqHandler.EventHandler)
	http.HandleFunc("/network/vxlan:sys/eps", postReqHandler.VxlanSysEps)
	http.HandleFunc("/network/vxlan:sys/bd", postReqHandler.VxlanSysBD)
	http.HandleFunc("/network/interface:sys/intf", postReqHandler.SysIntfHandler)
	http.HandleFunc("/network/environment:sys/ch", postReqHandler.SysChHandler) 
	http.HandleFunc("/network/resources:sys/proc", postReqHandler.sysProcHandler)
	http.HandleFunc("/network/resources:sys/procsys", postReqHandler.sysProcSysHandler) */

	http.ListenAndServe(":11000", nil)
}
