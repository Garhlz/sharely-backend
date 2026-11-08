package logic

import (
	"context"
	"testing"
	"time"

	"user-rpc/user"

	"github.com/stretchr/testify/require"
)

func TestAuthRedisExpiryAndMissingCode(t *testing.T) {
	u := activeUser(t, "correct")
	s, server := newAuthTestContext(t, &fakeUsersStore{byEmail: u})
	response, err := NewLoginLogic(context.Background(), s).Login(&user.LoginRequest{Email: u.Email, Password: "correct"})
	require.NoError(t, err)
	claims, err := parseToken(s.Config, response.Tokens.RefreshToken)
	require.NoError(t, err)
	require.Equal(t, 120*time.Second, server.TTL(refreshTokenKey(u.Id, claims.ID)))
	require.True(t, server.Exists(tokenVersionKey(u.Id)))
	require.Zero(t, server.TTL(tokenVersionKey(u.Id)))
	verified, err := NewVerifyEmailCodeLogic(context.Background(), s).VerifyEmailCode(&user.VerifyEmailCodeRequest{Email: u.Email, Code: "123456", Scene: "register"})
	require.NoError(t, err)
	require.False(t, verified.Valid)
}
