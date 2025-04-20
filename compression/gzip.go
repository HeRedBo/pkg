package compression

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
)

func GzipEncode(input []byte) ([]byte, error) {
	// 创建一个姓的 byte 输出流
	var buf bytes.Buffer
	gzipWirter := gzip.NewWriter(&buf)
	_, err := gzipWirter.Write(input)
	if err != nil {
		_ = gzipWirter.Close()
		return nil, err
	}
	if err := gzipWirter.Close(); err != nil {
		return nil, err
	}
	// 返回压缩后的 bytes 数组
	return buf.Bytes(), nil
}

func GzipDecode(input []byte) ([]byte, error) {
	bytesReader := bytes.NewReader(input)
	gzipReader, err := gzip.NewReader(bytesReader)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = gzipReader.Close()
	}()
	buf := new(bytes.Buffer)
	// 从 Reader 中读取出数据
	if _, err := buf.ReadFrom(gzipReader); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// MarshalJsonAndGzip 压缩
func MarshalJsonAndGzip(data interface{}) ([]byte, error) {
	marshalData, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	gzipData, err := GzipEncode(marshalData)
	if err != nil {
		return nil, err
	}
	return gzipData, err
}

// UnmarshalDataFormJsonWithGzip 解压
func UnmarshalDataFormJsonWithGzip(input []byte, output interface{}) error {
	decodeData, err := GzipDecode(input)
	if err != nil {
		return nil
	}
	err = json.Unmarshal(decodeData, output)
	if err != nil {
		return err
	}
	return nil
}
