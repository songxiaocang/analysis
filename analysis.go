package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"flag"
	"github.com/mediocregopher/radix.v2/pool"
	"github.com/mgutz/str"
	"github.com/sirupsen/logrus"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

//const
const(
	HANDLE_DIG ="/dig?"
	HANDLE_MOVIE = "/movie/"
	HANDLE_LIST = "/list/"
	HANDLE_HTML = ".html"
)

//struct
type cmdParams struct {
	logFilePath string
	routineNum int
}

type digData struct {
	time string
	url string
	refer string
	ua string
}

type urlNode struct {
	unType string  //首页、列表页、详情页
	unRid int   //资源id
	unUrl string
	unTime string
}

type urlData struct {
	data digData
	uid string
	unode urlNode
}

type storageBlock struct {
	counterType string
	storageMode string
	unode urlNode
}

var log = logrus.New()

func init(){
	log.Out = os.Stdout
	log.SetLevel(logrus.DebugLevel)
}


func main(){
	//获取参数
	logFilePath := flag.String("logFilePath", "/usr/local/etc/nginx/logs/dig.log", "log file path")
	routineNum := flag.Int("routineNum", 5, "consumer num by goroutine")
	l := flag.String("l","/tmp/log","system runtime target file path")
	flag.Parse()

	params := cmdParams{logFilePath:*logFilePath,routineNum:*routineNum}


	//打印日志
	file, e := os.OpenFile(*l,os.O_CREATE|os.O_WRONLY,0644)
	if e!=nil {
		log.Out = file
		log.Errorf("os open error: %v",e)
		defer  file.Close()
	}
	log.Printf("exec start")
	log.Printf("logFilePath:%s, routineNum:%d",params.logFilePath,params.routineNum)

	//初始化channel和redis pool
	logChan := make(chan string,3*params.routineNum)
	pvChan := make(chan urlData,params.routineNum)
	uvChan := make(chan urlData,params.routineNum)
	storageChan := make(chan storageBlock,params.routineNum)

	pool, err := pool.New("tcp", "localhost:6379", params.routineNum)
	if err!=nil {
		log.Fatalln("pool init error: %v",err)
		panic(err)

	}else{
		go func() {
			for  {
				cmd := pool.Cmd("ping")
				if cmd.Err !=nil{
					log.Printf("pool run oaky")
					time.Sleep(3*time.Second)
				}
			}
		}()

	}


	//日志消费
	go readLogLineByLine(params, logChan)

	//日志处理
	for i:=1;i<=params.routineNum ;i++  {
		go logConsumer(logChan,pvChan,uvChan)
	}

	//pv uv统计
	go pvCounter(pvChan,storageChan)
	go uvCounter(uvChan,storageChan,pool)


	//数据存储器
	go dataStorage(storageChan,pool)

	time.Sleep(1000*time.Second)
}

func readLogLineByLine(params cmdParams,logChan chan string) error{
	file, e := os.Open(params.logFilePath)
	if e!=nil {
		log.Errorf("os open error: %v",e)
		return e
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	count := 0
	for  {
		s, err:= reader.ReadString('\n')
		logChan<- s
		count++
		if count%(1000*params.routineNum)==0 {
			log.Infof("read line:%s",count)
		}

		if err!=nil {
			if err == io.EOF {
				log.Infof("data has read out，readline: %d",count)
			}else{
				log.Warningf("os read error: %v",err)
				return err
			}
		}

	}

	return nil

}

func logConsumer(logChan chan string,pvChan,uvChann chan urlData){
	for logData := range logChan{
		//日志切割，写入pvChan, uvChann
		digData := cutLogFetchData(logData)

		//构造uid
		hash := md5.New()
		hash.Write([]byte(digData.refer + digData.ua))
		uId := hex.EncodeToString(hash.Sum(nil))

		urlData := urlData{data:digData,uid:uId,unode:formatUrlNode(digData.url,digData.time)}
		pvChan<- urlData
		uvChann<- urlData
	}
}

//日志切割
func cutLogFetchData(logData string) digData{
	logData = strings.TrimSpace(logData)
	pos := str.IndexOf(logData, HANDLE_DIG, 0)
	if pos<1 {
		return digData{}
	}

	pos += len(HANDLE_DIG)

	pos2 := str.IndexOf(logData, "HTTP/", pos)
	substr := str.Substr(logData, pos, pos2-pos)

	parseUrl, e := url.Parse("http://localhost/?" + substr)
	if e!=nil {
		log.Errorf("url parse error: %v",e)
		return digData{}
	}
	data := parseUrl.Query()
	return digData{
		time:data.Get("time"),
		url:data.Get("url"),
		refer:data.Get("refer"),
		ua:data.Get("ua"),
	}

}

func formatUrlNode(u,t string) urlNode{
	//详情页 -> 列表页 -> 首页
	var uNode urlNode
	pos := str.IndexOf(u, HANDLE_MOVIE, 0)
	if pos!=-1 {
		pos += len(HANDLE_MOVIE)
		pos2 := str.IndexOf(u, HANDLE_HTML, pos)
		substr := str.Substr(u, pos, pos2)
		id, _ := strconv.Atoi(substr)
		uNode = urlNode{unType:"movie",unRid:id,unUrl:u,unTime:t}
	}else{
		pos = str.IndexOf(u, HANDLE_LIST, 0)
		if pos!=-1 {
			pos += len(HANDLE_LIST)
			pos2 := str.IndexOf(u, HANDLE_HTML, pos)
			substr := str.Substr(u, pos, pos2)
			id, _ := strconv.Atoi(substr)
			uNode = urlNode{unType:"list",unRid:id,unUrl:u,unTime:t}
		}else{
			uNode = urlNode{unType:"home",unRid:1,unUrl:u,unTime:t}
		}
	}
	return  uNode
}


func pvCounter(pvChan chan urlData,storageChan chan storageBlock){
	for data := range pvChan{
		block := storageBlock{counterType: "pv", storageMode:"ZINCRBY", unode:data.unode}
		storageChan<-block
	}
}

func uvCounter(uvChan chan urlData,storageChan chan storageBlock ,pool *pool.Pool){
	for data := range uvChan{
		hyperLogLogKey := "uv_hpll_"+getTime(data.data.time,"day")
		res, err := pool.Cmd("PFADD", hyperLogLogKey, data.uid, "EX", 86400).Int()
		if err!=nil {
			log.Warningln("uvcounter check redis hyperlog failed,",err)
		}
		if  res!=1{
			continue
		}

		block := storageBlock{counterType: "pv", storageMode:"ZINCRBY", unode:data.unode}
		storageChan<-block

	}
}


func getTime(logTime,timeType string) string{
	var item string
	switch timeType {
		case "day":
			item = "2006 01 02"
		case "hour":
			item = "2006 01 02 15"
		case "minute":
			item = "2006 01 02 15:04"
	}

	parseTime, _ := time.Parse(item, time.Now().Format(item))
	return strconv.FormatInt(parseTime.UnixNano()/1000000,10)
}


func dataStorage(storageChan chan storageBlock,pool *pool.Pool){
	//从chan取数据保存到redis中
	for data := range storageChan{
		prefix := data.counterType+"_"

		//按不同维护
		setKeys := []string{
			prefix+"day_"+getTime(data.unode.unTime,"day"),
			prefix+"hour_"+getTime(data.unode.unTime,"hour"),
			prefix+"minute_"+getTime(data.unode.unTime,"minute"),
			prefix+data.unode.unType+"_day_"+getTime(data.unode.unTime,"day"),
			prefix+data.unode.unType+"_hour_"+getTime(data.unode.unTime,"hour"),
			prefix+data.unode.unType+"_minute_"+getTime(data.unode.unTime,"minute"),
		}

		rid := data.unode.unRid

		for _,key := range setKeys{
			res, e := pool.Cmd("CFFAD", key, 1, rid).Int()
			if res<=0 || e!=nil {
				log.Errorln("redis store error",data.storageMode,rid,key)
			}
		}
	}

}