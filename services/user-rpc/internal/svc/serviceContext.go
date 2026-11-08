package svc

import (
	"context"
	"user-rpc/internal/config"
	"user-rpc/internal/model"
	"user-rpc/internal/mq"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type UsersStore interface {
	FindByIds(ctx context.Context, ids []int64) ([]*model.Users, error)
	FindOne(ctx context.Context, id int64) (*model.Users, error)
	FindOneByEmail(ctx context.Context, email string) (*model.Users, error)
	InsertAndReturnID(ctx context.Context, data *model.Users) (int64, error)
	Update(ctx context.Context, data *model.Users) error
}

type ServiceContext struct {
	Config    config.Config
	Db        sqlx.SqlConn
	Redis     redis.UniversalClient
	Users     UsersStore
	Publisher *mq.Publisher
}

func NewServiceContext(c config.Config) *ServiceContext {
	db := sqlx.NewMysql(c.Mysql.DataSource)
	publisher, err := mq.NewPublisher(c)
	if err != nil {
		logx.Errorf("rabbitmq init failed: %v", err)
		publisher = nil
	}
	return &ServiceContext{
		Config:    c,
		Db:        db,
		Redis:     config.NewRedisClient(c.BizRedis),
		Users:     model.NewUsersExtendedModel(db),
		Publisher: publisher,
	}
}
