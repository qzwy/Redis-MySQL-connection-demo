package main

import (
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"math/rand"
	"time"
)

//
var DB *sqlx.DB

//定义数据库查询的实体
type entity struct {
	Id   string `db:"id"`
	Name string `db:"id"`
}

//定义json结构-总览数据
type TotalInformation struct {
	OnlineRanking      int `json:"online_ranking"`
	OperationalMileage int `json:"operational_mileage"`
	PeopleNumber       int `json:"people_number"`
}

//线路数据结构
type LineData struct {
	Id               string         `json:"id"`
	InBoundCapacity  int            `json:"in_bound_capacity"`
	OutBoundCapacity int            `json:"out_bound_capacity"`
	ChainRatio       int            `json:"chain_ratio"`
	Shijifache       int            `json:"shijifache"`
	Jihuafache       int            `json:"jihuafache"`
	Zhengdian        int            `json:"zhengdian"`
	Duixian          float32        `json:"duixian"`
	PassengerFlow    []stationValue `json:"passenger_flow"`
}

//站点客流数据
type stationValue struct {
	StationName string `json:"station_name"`
	Flow        int    `json:"flow"`
}

//生成线路信息json数据
func lineDataProduct() []LineData {
	result := make([]LineData, 3)
	var entities []entity
	err := DB.Select(&entities, "select `id`, name from tbl_line")
	if err != nil {
		fmt.Println("Executed failed : ", err)
	}
	for _, value := range entities {
		var station []entity
		passengerFlow := make([]stationValue, len(entities))
		err := DB.Select(&station, "select `id`, name from tbl_staiton where line_id = ?1", value.Id)
		if err != nil {
			fmt.Println("Executed failed : ", err)
		}
		for _, stations := range station {
			passengerFlow = append(passengerFlow, stationValue{StationName: stations.Name, Flow: rand.Intn(100) + 100})
		}
		result = append(result, LineData{Id: value.Id, InBoundCapacity: rand.Intn(300) + 200, OutBoundCapacity: rand.Intn(300) + 200,
			ChainRatio: rand.Intn(300) + 200, Shijifache: rand.Intn(300) + 200, Jihuafache: rand.Intn(300) + 200, Zhengdian: rand.Intn(300) + 200,
			Duixian: float32(rand.Intn(50)+50) / 100, PassengerFlow: passengerFlow})
	}
	return result
}

//总览信息json数据生成
func totalInformation() TotalInformation {
	totalInformation := TotalInformation{
		OnlineRanking:      rand.Intn(10) + 10,
		OperationalMileage: rand.Intn(200) + 200,
		PeopleNumber:       rand.Intn(40000) + 10000,
	}
	return totalInformation
}

//消息发布
//chanel : topic
//content : 内容 (string)
//创建redis连接
//设定发布的topic 和 content
func producer(chanel string, content string) {
	redisCon, _ := redis.Dial("tcp", "n1:6379")
	_, err := redisCon.Do("PUBLISH", chanel, content)
	if err != nil {
		fmt.Print(err)
		return
	}
}

//订阅者
func consumer() {
	redisCon, err := redis.Dial("tcp", "n1:6379")
	if err != nil {
		fmt.Print(err)
		return
	}
	defer redisCon.Close()

	content := redis.PubSubConn{Conn: redisCon}
	_ = content.Subscribe("line", "total", "historyValue", "analyse")
	for {
		switch v := content.Receive().(type) {
		case redis.Message:
			fmt.Printf("chanel: %s\nmessage: %s\n", v.Channel, v.Data)
		case redis.Subscription:
			fmt.Printf("%s___%s___%d\n", v.Channel, v.Kind, v.Count)
		case error:
			fmt.Println(v)
			return
		}
	}
}

//初始化-连接数据库
func init() {
	//db = sqlx.NewDb(sql.Open())
	database, err := sqlx.Open("mysql", "root:KXSxw8xzH3U3UCt@tcp(10.10.49.116:13306)/ncc_dev")
	if err != nil {
		fmt.Println("DataBase connection happened some mistake")
		return
	}
	DB = database
}

func main() {
	//订阅
	go consumer()
	//持续发布消息
	for {
		content, err := json.Marshal(totalInformation())
		if err != nil {
			fmt.Println("json.marshal failed, err:", err)
		}
		go producer("total", string(content))

		lines, err := json.Marshal(lineDataProduct())
		if err != nil {
			fmt.Println("json.marshal failed, err: ", err)
		}
		go producer("line", string(lines))
		//go producer("line", "{\n"+
		//	"    {\"id\":\"02\",\"inboundCapacity\":200,\"outboundCapacity\":300,\"chainRatio\":0.5,\"shijifache\":200,\"jihuafache\":200,\"zhengdian\":0.2,\"duixian\":0.5,\"passengerFlow\":[{\"stationName\":\"李村公园站\",\"flow\":200},{\"stationName\":\"李村站\",\"flow\":200},{\"stationName\":\"枣山路站\",\"flow\":200},{\"stationName\":\"华楼山路站\",\"flow\":200},{\"stationName\":\"东韩站\",\"flow\":200},{\"stationName\":\"辽阳东路站\",\"flow\":200},{\"stationName\":\"同安路站\",\"flow\":200},{\"stationName\":\"苗岭路站\",\"flow\":200},{\"stationName\":\"石老人浴场站\",\"flow\":200},{\"stationName\":\"海安路站\",\"flow\":200},{\"stationName\":\"海川路站\",\"flow\":200},{\"stationName\":\"麦岛站\",\"flow\":200},{\"stationName\":\"高雄路站\",\"flow\":200},{\"stationName\":\"燕儿岛路站\",\"flow\":200},{\"stationName\":\"浮山所站\",\"flow\":200},{\"stationName\":\"五四广场站\",\"flow\":200},{\"stationName\":\"芝泉路站\",\"flow\":200}]},\n"+
		//	"    {\"id\":\"03\",\"inboundCapacity\":200,\"outboundCapacity\":300,\"chainRatio\":0.5,\"shijifache\":200,\"jihuafache\":200,\"zhengdian\":0.2,\"duixian\":0.5,\"passengerFlow\":[{\"stationName\":\"青岛站\",\"flow\":200},{\"stationName\":\"人民会堂站\",\"flow\":200},{\"stationName\":\"汇泉广场站\",\"flow\":200},{\"stationName\":\"中山公园站\",\"flow\":200},{\"stationName\":\"太平角公园站\",\"flow\":200},{\"stationName\":\"延安三路站\",\"flow\":200},{\"stationName\":\"五四广场站\",\"flow\":200},{\"stationName\":\"江西路站\",\"flow\":200},{\"stationName\":\"宁夏路站\",\"flow\":200},{\"stationName\":\"敦化路站\",\"flow\":200},{\"stationName\":\"错埠岭站\",\"flow\":200},{\"stationName\":\"清江路站\",\"flow\":200},{\"stationName\":\"双山站\",\"flow\":200},{\"stationName\":\"长沙路站\",\"flow\":200},{\"stationName\":\"地铁大厦站\",\"flow\":200},{\"stationName\":\"海尔路站\",\"flow\":200},{\"stationName\":\"万年泉路站\",\"flow\":200},{\"stationName\":\"李村站\",\"flow\":200},{\"stationName\":\"君峰路站\",\"flow\":200},{\"stationName\":\"振华路站\",\"flow\":200},{\"stationName\":\"永平路站\",\"flow\":200},{\"stationName\":\"青岛北站\",\"flow\":200}]},\n"+
		//	"    {\"id\":\"11\",\"inboundCapacity\":200,\"outboundCapacity\":300,\"chainRatio\":0.5,\"shijifache\":200,\"jihuafache\":200,\"zhengdian\":0.2,\"duixian\":0.5,\"passengerFlow\":[{\"stationName\":\"苗岭路站\",\"flow\":200},{\"stationName\":\"会展中心站\",\"flow\":200},{\"stationName\":\"青岛二中站\",\"flow\":200},{\"stationName\":\"青岛科大站\",\"flow\":200},{\"stationName\":\"张村站\",\"flow\":200},{\"stationName\":\"枯桃站\",\"flow\":200},{\"stationName\":\"海洋大学站\",\"flow\":200},{\"stationName\":\"世博园站\",\"flow\":200},{\"stationName\":\"北宅站\",\"flow\":200},{\"stationName\":\"北九水站\",\"flow\":200},{\"stationName\":\"庙石站\",\"flow\":200},{\"stationName\":\"浦里站\",\"flow\":200},{\"stationName\":\"鳌山卫站\",\"flow\":200},{\"stationName\":\"山东大学站\",\"flow\":200},{\"stationName\":\"蓝色硅谷站\",\"flow\":200},{\"stationName\":\"水泊站\",\"flow\":200},{\"stationName\":\"博览中心站\",\"flow\":200},{\"stationName\":\"温泉东站\",\"flow\":200},{\"stationName\":\"皋虞站\",\"flow\":200},{\"stationName\":\"臧村站\",\"flow\":200},{\"stationName\":\"钱谷山站\",\"flow\":200},{\"stationName\":\"鳌山湾站\",\"flow\":200}]},\n"+
		//	"    {\"id\":\"13\",\"inboundCapacity\":200,\"outboundCapacity\":300,\"chainRatio\":0.5,\"shijifache\":200,\"jihuafache\":200,\"zhengdian\":0.2,\"duixian\":0.5,\"passengerFlow\":[{\"stationName\":\"嘉陵江路站\",\"flow\":200},{\"stationName\":\"香江路站\",\"flow\":200},{\"stationName\":\"井冈山路站\",\"flow\":200},{\"stationName\":\"积米崖站\",\"flow\":200},{\"stationName\":\"积米崖站\",\"flow\":200},{\"stationName\":\"学院路站\",\"flow\":200},{\"stationName\":\"朝阳山站\",\"flow\":200},{\"stationName\":\"辛屯（灵山湾站）\",\"flow\":200},{\"stationName\":\"两河站\",\"flow\":200},{\"stationName\":\"隐珠站\",\"flow\":200},{\"stationName\":\"凤凰山路站\",\"flow\":200},{\"stationName\":\"双珠路站\",\"flow\":200},{\"stationName\":\"世纪大道站\",\"flow\":200},{\"stationName\":\"盛海路（世博城站）\",\"flow\":200},{\"stationName\":\"大珠山站\",\"flow\":200},{\"stationName\":\"张家楼站\",\"flow\":200},{\"stationName\":\"古镇口站\",\"flow\":200},{\"stationName\":\"龙湾站\",\"flow\":200},{\"stationName\":\"琅琊站\",\"flow\":200},{\"stationName\":\"贡口湾站\",\"flow\":200},{\"stationName\":\"董家口港站\",\"flow\":200},{\"stationName\":\"泊里站\",\"flow\":200},{\"stationName\":\"董家口火车站\",\"flow\":200}]}\n"+
		//	"｝")
		go producer("analyse", "{\n"+
			"    {\"id\":\"01\",\"inboundCapacity\":2000,\"outboundCapacity\":300,\"chainRatio\":0.5,\"transfer\":200,\"totalMileage\":1000,\"kilometerEnergyConsumption\":500,\n"+
			"     \"passengerTrafficVolume\":[{\"monthid\":01,\"volum\":100},{\"monthid\":02,\"volum\":100},{\"monthid\":03,\"volum\":100},{\"monthid\":04,\"volum\":100},{\"monthid\":05,\"volum\":100},{\"monthid\":06,\"volum\":100},{\"monthid\":07,\"volum\":100},      {\"monthid\":08,\"volum\":100},{\"monthid\":09,\"volum\":100},{\"monthid\":10,\"volum\":100},{\"monthid\":11,\"volum\":100},{\"monthid\":12,\"volum\":100}],\n"+
			"     \"eightMonthsActual\":[{\"monthid\":01,\"actual\":100},{\"monthid\":02,\"actual\":100},{\"monthid\":03,\"actual\":100},{\"monthid\":04,\"actual\":100},{\"monthid\":05,\"actual\":100},{\"monthid\":06,\"actual\":100},{\"monthid\":07,\"actual\":100},     {\"monthid\":08,\"actual\":100}],\n"+
			"    \"eightMonthsChainRatio\":[{\"monthid\":01,\"chainRatio\":100},{\"monthid\":02,\"chainRatio\":100},{\"monthid\":03,\"chainRatio\":100},{\"monthid\":04,\"chainRatio\":100},{\"monthid\":05,\"chainRatio\":100},{\"monthid\":06,\"chainRatio\":100},        {\"monthid\":07,\"chainRatio\":100},{\"monthid\":08,\"chainRatio\":100}],\n"+
			"   \"fourMonthsChainRatio\":[{\"monthid\":09,\"chainRatio\":100},{\"monthid\":10,\"chainRatio\":100},{\"monthid\":11,\"chainRatio\":100},{\"monthid\":12,\"chainRatio\":100}]\n"+
			"   },\n"+
			"   {\"id\":\"02\",\"inboundCapacity\":2000,\"outboundCapacity\":300,\"chainRatio\":0.5,\"transfer\":200,\"totalMileage\":1000,\"kilometerEnergyConsumption\":500,\n"+
			"     \"passengerTrafficVolume\":[{\"monthid\":01,\"volum\":100},{\"monthid\":02,\"volum\":100},{\"monthid\":03,\"volum\":100},{\"monthid\":04,\"volum\":100},{\"monthid\":05,\"volum\":100},{\"monthid\":06,\"volum\":100},{\"monthid\":07,\"volum\":100},      {\"monthid\":08,\"volum\":100},{\"monthid\":09,\"volum\":100},{\"monthid\":10,\"volum\":100},{\"monthid\":11,\"volum\":100},{\"monthid\":12,\"volum\":100}],\n"+
			"     \"eightMonthsActual\":[{\"monthid\":01,\"actual\":100},{\"monthid\":02,\"actual\":100},{\"monthid\":03,\"actual\":100},{\"monthid\":04,\"actual\":100},{\"monthid\":05,\"actual\":100},{\"monthid\":06,\"actual\":100},{\"monthid\":07,\"actual\":100},     {\"monthid\":08,\"actual\":100}],\n"+
			"    \"eightMonthsChainRatio\":[{\"monthid\":01,\"chainRatio\":100},{\"monthid\":02,\"chainRatio\":100},{\"monthid\":03,\"chainRatio\":100},{\"monthid\":04,\"chainRatio\":100},{\"monthid\":05,\"chainRatio\":100},{\"monthid\":06,\"chainRatio\":100},        {\"monthid\":07,\"chainRatio\":100},{\"monthid\":08,\"chainRatio\":100}],\n"+
			"   \"fourMonthsChainRatio\":[{\"monthid\":09,\"chainRatio\":100},{\"monthid\":10,\"chainRatio\":100},{\"monthid\":11,\"chainRatio\":100},{\"monthid\":12,\"chainRatio\":100}]\n"+
			"   },\n"+
			"   {\"id\":\"03\",\"inboundCapacity\":2000,\"outboundCapacity\":300,\"chainRatio\":0.5,\"transfer\":200,\"totalMileage\":1000,\"kilometerEnergyConsumption\":500,\n"+
			"     \"passengerTrafficVolume\":[{\"monthid\":01,\"volum\":100},{\"monthid\":02,\"volum\":100},{\"monthid\":03,\"volum\":100},{\"monthid\":04,\"volum\":100},{\"monthid\":05,\"volum\":100},{\"monthid\":06,\"volum\":100},{\"monthid\":07,\"volum\":100},      {\"monthid\":08,\"volum\":100},{\"monthid\":09,\"volum\":100},{\"monthid\":10,\"volum\":100},{\"monthid\":11,\"volum\":100},{\"monthid\":12,\"volum\":100}],\n"+
			"     \"eightMonthsActual\":[{\"monthid\":01,\"actual\":100},{\"monthid\":02,\"actual\":100},{\"monthid\":03,\"actual\":100},{\"monthid\":04,\"actual\":100},{\"monthid\":05,\"actual\":100},{\"monthid\":06,\"actual\":100},{\"monthid\":07,\"actual\":100},     {\"monthid\":08,\"actual\":100}],\n"+
			"    \"eightMonthsChainRatio\":[{\"monthid\":01,\"chainRatio\":100},{\"monthid\":02,\"chainRatio\":100},{\"monthid\":03,\"chainRatio\":100},{\"monthid\":04,\"chainRatio\":100},{\"monthid\":05,\"chainRatio\":100},{\"monthid\":06,\"chainRatio\":100},        {\"monthid\":07,\"chainRatio\":100},{\"monthid\":08,\"chainRatio\":100}],\n"+
			"   \"fourMonthsChainRatio\":[{\"monthid\":09,\"chainRatio\":100},{\"monthid\":10,\"chainRatio\":100},{\"monthid\":11,\"chainRatio\":100},{\"monthid\":12,\"chainRatio\":100}]\n"+
			"   },\n"+
			"   {\"id\":\"04\",\"inboundCapacity\":2000,\"outboundCapacity\":300,\"chainRatio\":0.5,\"transfer\":200,\"totalMileage\":1000,\"kilometerEnergyConsumption\":500,\n"+
			"     \"passengerTrafficVolume\":[{\"monthid\":01,\"volum\":100},{\"monthid\":02,\"volum\":100},{\"monthid\":03,\"volum\":100},{\"monthid\":04,\"volum\":100},{\"monthid\":05,\"volum\":100},{\"monthid\":06,\"volum\":100},{\"monthid\":07,\"volum\":100},      {\"monthid\":08,\"volum\":100},{\"monthid\":09,\"volum\":100},{\"monthid\":10,\"volum\":100},{\"monthid\":11,\"volum\":100},{\"monthid\":12,\"volum\":100}],\n"+
			"     \"eightMonthsActual\":[{\"monthid\":01,\"actual\":100},{\"monthid\":02,\"actual\":100},{\"monthid\":03,\"actual\":100},{\"monthid\":04,\"actual\":100},{\"monthid\":05,\"actual\":100},{\"monthid\":06,\"actual\":100},{\"monthid\":07,\"actual\":100},     {\"monthid\":08,\"actual\":100}],\n"+
			"    \"eightMonthsChainRatio\":[{\"monthid\":01,\"chainRatio\":100},{\"monthid\":02,\"chainRatio\":100},{\"monthid\":03,\"chainRatio\":100},{\"monthid\":04,\"chainRatio\":100},{\"monthid\":05,\"chainRatio\":100},{\"monthid\":06,\"chainRatio\":100},        {\"monthid\":07,\"chainRatio\":100},{\"monthid\":08,\"chainRatio\":100}],\n"+
			"   \"fourMonthsChainRatio\":[{\"monthid\":09,\"chainRatio\":100},{\"monthid\":10,\"chainRatio\":100},{\"monthid\":11,\"chainRatio\":100},{\"monthid\":12,\"chainRatio\":100}]\n"+
			"   }\n"+
			"}")

		go producer("historyValue", "{\n"+
			"     \"dayPeakValue\":29,\"passengerTrafficCapacity\":20000,\"inboundCapacity\":2000,\"outboundCapacity\":20000,\"sectionPeakValue\":2000,\"stationPeekValue\":[{\"id\":02,\"peekValue\":[{\"stationName\":\"李村公园站\",\"value\":200},{\"stationName\":\"李村站\",\"value\":200},{\"stationName\":\"枣山路站\",\"value\":200},{\"stationName\":\"华楼山路站\",\"value\":200},{\"stationName\":\"东韩站\",\"value\":200},{\"stationName\":\"辽阳东路站\",\"value\":200},{\"stationName\":\"同安路站\",\"value\":200},{\"stationName\":\"苗岭路站\",\"value\":200},{\"stationName\":\"石老人浴场站\",\"value\":200},{\"stationName\":\"海安路站\",\"value\":200},{\"stationName\":\"海川路站\",\"value\":200},{\"stationName\":\"麦岛站\",\"value\":200},{\"stationName\":\"高雄路站\",\"value\":200},{\"stationName\":\"燕儿岛路站\",\"value\":200},{\"stationName\":\"浮山所站\",\"value\":200},{\"stationName\":\"五四广场站\",\"value\":200},{\"stationName\":\"芝泉路站\",\"value\":200}]},\n"+
			"{\"id\":03,\"peekValue\":[{\"stationName\":\"青岛站\",\"flow\":200},{\"stationName\":\"人民会堂站\",\"flow\":200},{\"stationName\":\"汇泉广场站\",\"flow\":200},{\"stationName\":\"中山公园站\",\"flow\":200},{\"stationName\":\"太平角公园站\",\"flow\":200},{\"stationName\":\"延安三路站\",\"value\":200},{\"stationName\":\"五四广场站\",\"value\":200},{\"stationName\":\"江西路站\",\"value\":200},{\"stationName\":\"宁夏路站\",\"value\":200},{\"stationName\":\"敦化路站\",\"value\":200},{\"stationName\":\"错埠岭站\",\"value\":200},{\"stationName\":\"清江路站\",\"value\":200},{\"stationName\":\"双山站\",\"value\":200},{\"stationName\":\"长沙路站\",\"value\":200},{\"stationName\":\"地铁大厦站\",\"value\":200},{\"stationName\":\"海尔路站\",\"value\":200},{\"stationName\":\"万年泉路站\",\"value\":200},{\"stationName\":\"李村站\",\"value\":200},{\"stationName\":\"君峰路站\",\"value\":200},{\"stationName\":\"振华路站\",\"value\":200},{\"stationName\":\"永平路站\",\"value\":200},{\"stationName\":\"青岛北站\",\"value\":200}]},\n"+
			"{\"id\":11\"peekValue\":[{\"stationName\":\"苗岭路站\",\"value\":200},{\"stationName\":\"会展中心站\",\"value\":200},{\"stationName\":\"青岛二中站\",\"value\":200},{\"stationName\":\"青岛科大站\",\"value\":200},{\"stationName\":\"张村站\",\"value\":200},{\"stationName\":\"枯桃站\",\"value\":200},{\"stationName\":\"海洋大学站\",\"value\":200},{\"stationName\":\"世博园站\",\"value\":200},{\"stationName\":\"北宅站\",\"value\":200},{\"stationName\":\"北九水站\",\"value\":200},{\"stationName\":\"庙石站\",\"value\":200},{\"stationName\":\"浦里站\",\"value\":200},{\"stationName\":\"鳌山卫站\",\"value\":200},{\"stationName\":\"山东大学站\",\"value\":200},{\"stationName\":\"蓝色硅谷站\",\"value\":200},{\"stationName\":\"水泊站\",\"value\":200},{\"stationName\":\"博览中心站\",\"value\":200},{\"stationName\":\"温泉东站\",\"value\":200},{\"stationName\":\"皋虞站\",\"value\":200},{\"stationName\":\"臧村站\",\"value\":200},{\"stationName\":\"钱谷山站\",\"value\":200},{\"stationName\":\"鳌山湾站\",\"value\":200}]},\n"+
			"{\"id\":13,\"peekValue\":[{\"stationName\":\"嘉陵江路站\",\"value\":200},{\"stationName\":\"香江路站\",\"value\":200},{\"stationName\":\"井冈山路站\",\"value\":200},{\"stationName\":\"积米崖站\",\"value\":200},{\"stationName\":\"积米崖站\",\"value\":200},{\"stationName\":\"学院路站\",\"value\":200},{\"stationName\":\"朝阳山站\",\"value\":200},{\"stationName\":\"辛屯（灵山湾站）\",\"value\":200},{\"stationName\":\"两河站\",\"value\":200},{\"stationName\":\"隐珠站\",\"value\":200},{\"stationName\":\"凤凰山路站\",\"value\":200},{\"stationName\":\"双珠路站\",\"value\":200},{\"stationName\":\"世纪大道站\",\"value\":200},{\"stationName\":\"盛海路（世博城站）\",\"value\":200},{\"stationName\":\"大珠山站\",\"value\":200},{\"stationName\":\"张家楼站\",\"value\":200},{\"stationName\":\"古镇口站\",\"value\":200},{\"stationName\":\"龙湾站\",\"value\":200},{\"stationName\":\"琅琊站\",\"value\":200},{\"stationName\":\"贡口湾站\",\"value\":200},{\"stationName\":\"董家口港站\",\"value\":200},{\"stationName\":\"泊里站\",\"value\":200},{\"stationName\":\"董家口火车站\",\"value\":200}]},]\n"+
			" }")
		time.Sleep(time.Second * 5)
	}
}
