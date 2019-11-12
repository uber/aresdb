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

package handlers

import (
	"io/ioutil"
	"net/http"
	"path"

	"github.com/gorilla/mux"
	"github.com/uber/aresdb/utils"
	"go.uber.org/config"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// UIHandlerParams defines params needed to build UI handler
type UIHandlerParams struct {
	fx.In

	ConfigProvider config.Provider
	Logger         *zap.SugaredLogger
}

// UIHandler handles health check requests
type UIHandler struct {
	path string
	mapper    utils.ResourceMapper
	logger    *zap.SugaredLogger
}

type uiHandlerConfig struct {
	Path string `yaml:"path"`
}

// NewUIHandler returns a new health handler
func NewUIHandler(p UIHandlerParams) UIHandler {
	var cfg uiHandlerConfig
	if err := p.ConfigProvider.Get("ui").Populate(&cfg); err != nil {
		p.Logger.Fatal("failed to get config for UI")
	}
	p.Logger.Infof("ui config %v", cfg)
	mapper := utils.NewResourceMapper(path.Join(cfg.Path, "build"))
	return UIHandler{
		logger:    p.Logger,
		path: 	   cfg.Path,
		mapper:    mapper,
	}
}

// Register adds paths to router
func (h UIHandler) Register(router *mux.Router) {
	router.HandleFunc("/", utils.ApplyHTTPWrappers2(h.Index))

	swaggerHandler := http.StripPrefix("/swagger/", http.FileServer(http.Dir(path.Join(h.path, "swagger"))))
	nodeModulesHandler := http.StripPrefix("/node_modules/", http.FileServer(http.Dir(path.Join(h.path, "node_modules"))))
	router.PathPrefix("/swagger/").Handler(swaggerHandler)
	router.PathPrefix("/node_modules").Handler(nodeModulesHandler)
	router.PathPrefix("/static").Handler(http.StripPrefix("/static/",
		http.FileServer(http.Dir(path.Join(h.path, "build", "static")))))
}

// Index serves landing page for ares-controller UI
func (h UIHandler) Index(w *utils.ResponseWriter, r *http.Request) {
	h.logger.Info("hello word index")
	indexFileName, err := h.mapper.Map("index.html")
	indexPath := path.Join(h.path, "build", indexFileName)
	if err != nil {
		w.WriteError(err)
		return
	}

	indexBytes, err := ioutil.ReadFile(indexPath)
	if err != nil {
		w.WriteError(err)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(indexBytes)
}
