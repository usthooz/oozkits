package redis

import (
	"fmt"
)

// Module
type Module struct {
	key    string
	module string
	prefix string
}

// NewModule create module.
func NewModule(module string) *Module {
	return &Module{
		key:    "%s:%s",
		module: module,
		prefix: fmt.Sprintf("%s:", module),
	}
}

// GetKey
func (m *Module) GetKey(shortKey string) string {
	return fmt.Sprintf(m.key, m.module, shortKey)
}

// GetPrefix
func (m *Module) GetPrefix() string {
	return m.prefix
}

// GetModuleString
func (m *Module) GetModuleString() string {
	return fmt.Sprintf("module: %s", m.module)
}

// SetModuleString
func (m *Module) SetModuleString(module string) *Module {
	m.module = module
	return m
}
