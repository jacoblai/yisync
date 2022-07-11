package engine

import (
	"context"
	"github.com/jacoblai/yisync/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"io/ioutil"
	"log"
	"runtime"
	"time"
)

const (
	OperationTypeInsert  = "insert"
	OperationTypeDelete  = "delete"
	OperationTypeUpdate  = "update"
	OperationTypeReplace = "replace"
)

type DbEngine struct {
	Master      *mongo.Database //主数据库
	Client      *mongo.Database //从数据库
	resumeToken bson.Raw
	dir         string
}

func NewDbEngine(idir string) *DbEngine {
	return &DbEngine{
		dir: idir,
	}
}

func (d *DbEngine) Open(masterMg, masterDb, clientMg, clientDb string) error {
	mops := options.Client().ApplyURI(masterMg)
	p := uint64(runtime.NumCPU() * 2)
	mops.MaxPoolSize = &p
	mops.ReadPreference = readpref.SecondaryPreferred()
	db, err := mongo.NewClient(mops)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = db.Connect(ctx)
	if err != nil {
		return err
	}
	d.Master = db.Database(masterDb)

	cops := options.Client().ApplyURI(clientMg)
	cops.MaxPoolSize = &p
	cops.WriteConcern = writeconcern.New(writeconcern.J(true), writeconcern.W(1))
	cdb, err := mongo.NewClient(cops)
	if err != nil {
		return err
	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = cdb.Connect(ctx)
	if err != nil {
		return err
	}
	d.Client = cdb.Database(clientDb)

	return nil
}

func (d *DbEngine) Sync() {
	for {
		d.watch(d.Master)
	}
}

func (d *DbEngine) watch(client *mongo.Database) {
	defer func() {
		err := recover()
		if err != nil {
			log.Printf("同步出现异常: %+v \n", err)
		}
	}()

	//设置过滤条件
	pipeline := mongo.Pipeline{
		bson.D{{"$match",
			bson.M{"operationType": bson.M{"$in": bson.A{"insert", "delete", "replace", "update"}}},
		}},
	}

	//当前时间前一小时
	now := time.Now()
	m, _ := time.ParseDuration("-1h")
	now = now.Add(m)
	timestamp := &primitive.Timestamp{
		T: uint32(now.Unix()),
		I: 0,
	}

	//设置监听option
	opt := options.ChangeStream().SetFullDocument(options.UpdateLookup).SetStartAtOperationTime(timestamp)
	if d.resumeToken != nil {
		opt.SetResumeAfter(d.resumeToken)
		opt.SetStartAtOperationTime(nil)
	}

	//获得watch监听
	watch, err := client.Watch(context.TODO(), pipeline, opt)
	if err != nil {
		log.Fatal("watch监听失败：", err)
	}

	//获得从库连接
	slaveClient := d.Client

	for watch.Next(context.TODO()) {
		var stream model.StreamObject
		err = watch.Decode(&stream)
		if err != nil {
			log.Println("watch数据失败：", err)
		}

		log.Println("=============", stream.FullDocument["_id"])

		//保存现在resumeToken
		d.resumeToken = watch.ResumeToken()

		switch stream.OperationType {
		case OperationTypeInsert:
			_, err := slaveClient.Collection(stream.Ns.Collection).InsertOne(context.TODO(), stream.FullDocument)
			if err != nil {
				log.Println("插入失败：", err)
			}
		case OperationTypeDelete:
			filter := bson.M{"_id": stream.FullDocument["_id"]}
			_, err := slaveClient.Collection(stream.Ns.Collection).DeleteOne(context.TODO(), filter)
			if err != nil {
				log.Println("删除失败：", err)
			}
		case OperationTypeUpdate:
			filter := bson.M{"_id": stream.FullDocument["_id"]}
			update := bson.M{"$set": stream.FullDocument}
			_, err := slaveClient.Collection(stream.Ns.Collection).UpdateOne(context.TODO(), filter, update)
			if err != nil {
				log.Println("更新失败：", err)
			}
		case OperationTypeReplace:
			filter := bson.M{"_id": stream.FullDocument["_id"]}
			_, err := slaveClient.Collection(stream.Ns.Collection).ReplaceOne(context.TODO(), filter, stream.FullDocument)
			if err != nil {
				log.Println("替换失败：", err)
			}
		}
	}
}

func (d *DbEngine) Close() {
	_ = d.Master.Client().Disconnect(context.Background())
	_ = d.Client.Client().Disconnect(context.Background())
	if len(d.resumeToken) > 0 {
		_ = ioutil.WriteFile(d.dir+"/resumeToken", d.resumeToken, 755)
	}
}
