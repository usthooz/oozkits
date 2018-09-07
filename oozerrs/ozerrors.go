package oozerr

import (
	"encoding/json"
)

/*
	{
		"code": 10010102, // 自定义-如: 项目序号(4位)+模块(2位)+功能(2位)
		"message": {
			"title": "提示",
			"content": "数据异常",
			"detail": {
				"type": 1,
				"notice": "default"
			}
		},
		"reason": "sql: expected 4 arguments, got 3"
	}
*/

type (
	// 错误类型结构体
	OozError struct {
		Code    int32
		Message Msg
		Reason  string
	}
	// Msg 错误信息
	Msg struct {
		// Title 标题
		Title string `json:"title,omitempty"`
		// Content 简易错误提示
		Content string `json:"content,omitempty"`
		// detail 详情(可json序列化，包含多种内容格式)-如: {"type": 1, "duration": 12, "detail": "重复编辑"}
		Detail string `json:"detail,omitempty"`
	}
)

// NewOzerr 新建错误
func NewOzerr(code int32, msg Msg, reason ...string) *OozError {
	zerr := &OozError{
		Code:    code,
		Message: msg,
	}
	if len(reason) > 0 {
		zerr.Reason = reason[0]
	}
	return zerr
}

func (o *OozError) GetCode() int32 {
	return o.Code
}

func (o *OozError) SetCode(code int32) int32 {
	o.Code = code
	return o.Code
}

func (o *OozError) GetReason() string {
	return o.Reason
}

// SetReason
func (o *OozError) SetReason(reason string) *OozError {
	o.Reason = reason
	return o
}

// GetMessage
func (o *OozError) GetMessage() Msg {
	return o.Message
}

// SetMessage
func (o *OozError) SetMessage(msg Msg) *OozError {
	o.Message = msg
	return o
}

// GetMsgTitle
func (o *OozError) GetMsgTitle() string {
	return o.Message.Title
}

// SetMsgTitle
func (o *OozError) SetMsgTitle(title string) *OozError {
	o.Message.Title = title
	return o
}

// GetMsgContent
func (o *OozError) GetMsgContent() string {
	return o.Message.Content
}

// SetMsgContent
func (o *OozError) SetMsgContent(content string) *OozError {
	o.Message.Content = content
	return o
}

// DetailString 转换为string
func (o *OozError) DetailString(data interface{}) *OozError {
	encode, _ := json.Marshal(data)
	o.Message.Detail = string(encode)
	return o
}
