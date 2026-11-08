package logic

import (
	"context"
	"strings"
	"time"

	appmetrics "user-rpc/internal/metrics"
	"user-rpc/internal/svc"
	"user-rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SendEmailCodeLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewSendEmailCodeLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SendEmailCodeLogic {
	return &SendEmailCodeLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// Auth / Account
func (l *SendEmailCodeLogic) SendEmailCode(in *user.SendEmailCodeRequest) (*user.SendEmailCodeResponse, error) {
	email := strings.TrimSpace(in.GetEmail())
	scene, ok := normalizeEmailScene(in.GetScene())
	if email == "" {
		appmetrics.IncEmailCode("unknown", "invalid_email")
		return nil, status.Error(codes.InvalidArgument, "email is required")
	}
	if !ok {
		appmetrics.IncEmailCode("unknown", "invalid_scene")
		return nil, status.Error(codes.InvalidArgument, "invalid scene")
	}

	limitKey := emailLimitKey(scene, email)
	allowed, err := l.svcCtx.Redis.SetNX(l.ctx, limitKey, "1", time.Minute).Result()
	if err != nil {
		appmetrics.IncEmailCode(scene, "limit_check_error")
		return nil, status.Error(codes.Internal, "rate limit error")
	}
	if !allowed {
		appmetrics.IncEmailCode(scene, "rate_limited")
		return nil, status.Error(codes.ResourceExhausted, "operation too frequent")
	}

	code, err := randCode()
	if err != nil {
		return nil, status.Error(codes.Internal, "generate code failed")
	}

	subject := "【分享派】验证码"
	body := "您的验证码是：" + code + "。有效期为5分钟，请勿泄露给他人。"
	if err := sendEmail(l.svcCtx.Config, email, subject, body); err != nil {
		l.Logger.Errorf("send email failed: %s", err)
		_, _ = l.svcCtx.Redis.Del(l.ctx, limitKey).Result()
		appmetrics.IncEmailCode(scene, "send_mail_error")
		return nil, status.Errorf(codes.Internal, "mail send failed: %s", err)
	}

	codeKey := emailCodeKey(scene, email)
	if err := l.svcCtx.Redis.SetEx(l.ctx, codeKey, code, 5*time.Minute).Err(); err != nil {
		appmetrics.IncEmailCode(scene, "store_code_error")
		return nil, status.Error(codes.Internal, "store code failed")
	}
	appmetrics.IncEmailCode(scene, "success")

	return &user.SendEmailCodeResponse{
		NextRetrySeconds: 60,
	}, nil
}
