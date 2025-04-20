package compression

import (
	"github.com/gookit/goutil/dump"
	"testing"
)

type UserTest struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

func TestUnmarshalDataFormJsonWithGzip(t *testing.T) {
	user := UserTest{
		ID:   1,
		Name: "RedBo",
	}

	userByte, err := MarshalJsonAndGzip(user)
	if err != nil {
		t.Errorf("MarshalJsonAndGzip err %v", err)
	}
	outPutUser := UserTest{}
	err = UnmarshalDataFromJsonWithGzip(userByte, &outPutUser)
	if err != nil {
		t.Errorf("UnmarshalDataFromJsonWithGzip err %v", err)
	}
	dump.Println(outPutUser)
	t.Log("outputUser:", outPutUser)
}
