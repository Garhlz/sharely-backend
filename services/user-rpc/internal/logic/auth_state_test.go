package logic

import (
	"context"
	"errors"
	"testing"

	"user-rpc/internal/config"
	"user-rpc/internal/model"
	"user-rpc/internal/svc"
	"user-rpc/user"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeUsersStore struct {
	byID      *model.Users
	byEmail   *model.Users
	findIDErr error
	findErr   error
	insertID  int64
	insertErr error
	inserted  *model.Users
	updateErr error
}

func (f *fakeUsersStore) FindByIds(context.Context, []int64) ([]*model.Users, error) {
	if f.byID == nil {
		return nil, f.findIDErr
	}
	return []*model.Users{f.byID}, f.findIDErr
}

func (f *fakeUsersStore) FindOne(context.Context, int64) (*model.Users, error) {
	return f.byID, f.findIDErr
}

func (f *fakeUsersStore) FindOneByEmail(context.Context, string) (*model.Users, error) {
	return f.byEmail, f.findErr
}

func (f *fakeUsersStore) InsertAndReturnID(_ context.Context, data *model.Users) (int64, error) {
	copy := *data
	f.inserted = &copy
	return f.insertID, f.insertErr
}

func (f *fakeUsersStore) Update(context.Context, *model.Users) error {
	return f.updateErr
}

func testAuthConfig() config.Config {
	cfg := config.Config{}
	cfg.Jwt.Secret = "unit-test-secret-with-enough-entropy"
	cfg.Jwt.Issuer = "sharely-test"
	cfg.Jwt.AccessExpireSeconds = 60
	cfg.Jwt.RefreshExpireSeconds = 120
	cfg.User.DefaultAvatar = "default.png"
	return cfg
}

func newAuthTestContext(t *testing.T, users svc.UsersStore) (*svc.ServiceContext, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return &svc.ServiceContext{
		Config: testAuthConfig(),
		Redis:  client,
		Users:  users,
	}, server
}

func activeUser(t *testing.T, password string) *model.Users {
	t.Helper()
	hashed, err := hashPassword(password)
	require.NoError(t, err)
	return &model.Users{
		Id:           42,
		Email:        "user@example.com",
		Password:     hashed,
		Nickname:     "Elaine",
		Role:         "USER",
		Status:       1,
		TokenVersion: 3,
	}
}

func TestLoginStateMachine(t *testing.T) {
	t.Run("unknown user", func(t *testing.T) {
		ctx, _ := newAuthTestContext(t, &fakeUsersStore{findErr: model.ErrNotFound})
		resp, err := NewLoginLogic(context.Background(), ctx).
			Login(&user.LoginRequest{Email: "user@example.com", Password: "password"})
		assert.Nil(t, resp)
		assert.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("query failure", func(t *testing.T) {
		ctx, _ := newAuthTestContext(t, &fakeUsersStore{findErr: errors.New("db down")})
		resp, err := NewLoginLogic(context.Background(), ctx).
			Login(&user.LoginRequest{Email: "user@example.com", Password: "password"})
		assert.Nil(t, resp)
		assert.Equal(t, codes.Internal, status.Code(err))
	})

	t.Run("disabled account", func(t *testing.T) {
		u := activeUser(t, "password")
		u.Status = 0
		ctx, _ := newAuthTestContext(t, &fakeUsersStore{byEmail: u})
		resp, err := NewLoginLogic(context.Background(), ctx).
			Login(&user.LoginRequest{Email: u.Email, Password: "password"})
		assert.Nil(t, resp)
		assert.Equal(t, codes.PermissionDenied, status.Code(err))
	})

	t.Run("wrong password", func(t *testing.T) {
		u := activeUser(t, "correct")
		ctx, _ := newAuthTestContext(t, &fakeUsersStore{byEmail: u})
		resp, err := NewLoginLogic(context.Background(), ctx).
			Login(&user.LoginRequest{Email: u.Email, Password: "wrong"})
		assert.Nil(t, resp)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
	})

	t.Run("success stores refresh and token version", func(t *testing.T) {
		u := activeUser(t, "correct")
		ctx, server := newAuthTestContext(t, &fakeUsersStore{byEmail: u})
		resp, err := NewLoginLogic(context.Background(), ctx).
			Login(&user.LoginRequest{Email: "  " + u.Email + "  ", Password: "correct"})

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, int64(42), resp.User.UserId)
		assert.NotEmpty(t, resp.Tokens.AccessToken)
		assert.NotEmpty(t, resp.Tokens.RefreshToken)
		version, err := server.Get(tokenVersionKey(u.Id))
		require.NoError(t, err)
		assert.Equal(t, "3", version)

		claims, err := parseToken(ctx.Config, resp.Tokens.RefreshToken)
		require.NoError(t, err)
		assert.True(t, server.Exists(refreshTokenKey(u.Id, claims.ID)))
	})
}

func TestRegisterStateMachine(t *testing.T) {
	const email = "new@example.com"
	const code = "123456"

	t.Run("invalid verification code", func(t *testing.T) {
		store := &fakeUsersStore{}
		ctx, _ := newAuthTestContext(t, store)
		resp, err := NewRegisterLogic(context.Background(), ctx).Register(&user.RegisterRequest{
			Email: email, Code: "wrong", Password: "password",
		})
		assert.Nil(t, resp)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		assert.Nil(t, store.inserted)
	})

	t.Run("email conflict", func(t *testing.T) {
		store := &fakeUsersStore{byEmail: &model.Users{Id: 1}}
		ctx, server := newAuthTestContext(t, store)
		server.Set(emailCodeKey(emailSceneRegister, email), code)

		resp, err := NewRegisterLogic(context.Background(), ctx).Register(&user.RegisterRequest{
			Email: email, Code: code, Password: "password",
		})
		assert.Nil(t, resp)
		assert.Equal(t, codes.AlreadyExists, status.Code(err))
		assert.Nil(t, store.inserted)
	})

	t.Run("success creates defaults and consumes code", func(t *testing.T) {
		store := &fakeUsersStore{findErr: model.ErrNotFound, insertID: 99}
		ctx, server := newAuthTestContext(t, store)
		server.Set(emailCodeKey(emailSceneRegister, email), code)

		resp, err := NewRegisterLogic(context.Background(), ctx).Register(&user.RegisterRequest{
			Email: email, Code: code, Password: "password", Nickname: " ",
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, int64(99), resp.User.UserId)
		require.NotNil(t, store.inserted)
		assert.Equal(t, "用户", store.inserted.Nickname)
		assert.Equal(t, "default.png", store.inserted.Avatar)
		assert.True(t, checkPassword(store.inserted.Password, "password"))
		assert.False(t, server.Exists(emailCodeKey(emailSceneRegister, email)))
	})
}

func TestRefreshTokenStateMachine(t *testing.T) {
	u := activeUser(t, "password")

	t.Run("access token cannot be refreshed", func(t *testing.T) {
		ctx, _ := newAuthTestContext(t, &fakeUsersStore{byID: u})
		pair, _, err := buildTokenPair(ctx.Config, u)
		require.NoError(t, err)

		resp, err := NewRefreshTokenLogic(context.Background(), ctx).
			RefreshToken(&user.RefreshTokenRequest{RefreshToken: pair.AccessToken})
		assert.Nil(t, resp)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("blocked token is rejected", func(t *testing.T) {
		ctx, server := newAuthTestContext(t, &fakeUsersStore{byID: u})
		pair, _, err := buildTokenPair(ctx.Config, u)
		require.NoError(t, err)
		server.Set(blockedTokenKey(pair.RefreshToken), "1")

		resp, err := NewRefreshTokenLogic(context.Background(), ctx).
			RefreshToken(&user.RefreshTokenRequest{RefreshToken: pair.RefreshToken})
		assert.Nil(t, resp)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
	})

	t.Run("token version mismatch is rejected", func(t *testing.T) {
		ctx, _ := newAuthTestContext(t, &fakeUsersStore{byID: u})
		old := *u
		old.TokenVersion = u.TokenVersion - 1
		pair, _, err := buildTokenPair(ctx.Config, &old)
		require.NoError(t, err)

		resp, err := NewRefreshTokenLogic(context.Background(), ctx).
			RefreshToken(&user.RefreshTokenRequest{RefreshToken: pair.RefreshToken})
		assert.Nil(t, resp)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
	})

	t.Run("success rotates whitelist entry", func(t *testing.T) {
		ctx, server := newAuthTestContext(t, &fakeUsersStore{byID: u})
		oldPair, oldJTI, err := buildTokenPair(ctx.Config, u)
		require.NoError(t, err)
		oldKey := refreshTokenKey(u.Id, oldJTI)
		server.Set(oldKey, "1")

		resp, err := NewRefreshTokenLogic(context.Background(), ctx).
			RefreshToken(&user.RefreshTokenRequest{RefreshToken: oldPair.RefreshToken})

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.False(t, server.Exists(oldKey))
		newClaims, err := parseToken(ctx.Config, resp.Tokens.RefreshToken)
		require.NoError(t, err)
		assert.NotEqual(t, oldJTI, newClaims.ID)
		assert.True(t, server.Exists(refreshTokenKey(u.Id, newClaims.ID)))
	})
}
