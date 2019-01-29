//  Copyright (c) 2017-2018 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"github.com/sirupsen/logrus"
	"github.com/uber-common/bark"
	"github.com/uber-common/bark/zbark"
	"go.uber.org/zap"
)

// LoggerFactory defines the log factory ares needs.
type LoggerFactory interface {
	// GetDefaultLogger returns the default logger.
	GetDefaultLogger() *zap.Logger
	// GetLogger returns logger given the logger name.
	GetLogger(name string) *zap.Logger
}

// LogrusLoggerFactory is the standard logrus implementation of LoggerFactory
type LogrusLoggerFactory struct {
	logger *logrus.Logger
}

// ZapLoggerFactory is the stdlog implementation of LoggerFactory
type ZapLoggerFactory struct {
	logger *zap.Logger
}

// NewLoggerFactory creates a default logrus LoggerFactory implementation.
func NewLoggerFactory() LoggerFactory {
	return &LogrusLoggerFactory{
		logger: logrus.StandardLogger(),
	}
}

// GetDefaultLogger returns the default zap logger.
func (r *LogrusLoggerFactory) GetDefaultLogger() *zap.Logger {
	return zbark.Zapify(bark.NewLoggerFromLogrus(r.logger))
}

// GetLogger of LogrusLoggerFactory ignores the given name and just return the default logger.
func (r *LogrusLoggerFactory) GetLogger(name string) *zap.Logger {
	return zbark.Zapify(bark.NewLoggerFromLogrus(r.logger))
}

// NewZapLoggerFactory creates a default zap LoggerFactory implementation.
func NewZapLoggerFactory() LoggerFactory {
	return &ZapLoggerFactory{
		logger: zap.NewExample(),
	}
}

// GetDefaultLogger returns the default zap logger.
func (r *ZapLoggerFactory) GetDefaultLogger() *zap.Logger {
	return r.logger
}

// GetLogger of ZapLoggerFactory ignores the given name and just return the default logger.
func (r *ZapLoggerFactory) GetLogger(name string) *zap.Logger {
	return r.logger
}
