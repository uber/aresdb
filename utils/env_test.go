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

package utils

import (
	"github.com/onsi/ginkgo"
	"os"

	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Env test", func() {
	var oldAresEnv string
	ginkgo.BeforeEach(func() {
		ResetDefaults()
		oldAresEnv = GetConfig().Env
	})

	ginkgo.AfterEach(func() {
		os.Setenv("ARES_ENV", oldAresEnv)
		ResetDefaults()
	})

	ginkgo.It("GetAresEnv should work after changing "+
		"ARES_ENV env variable", func() {
		os.Setenv("ARES_ENV", string(EnvTest))
		ResetDefaults()
		Ω(IsTest()).Should(BeTrue())

		os.Setenv("ARES_ENV", string(EnvDev))
		ResetDefaults()
		Ω(IsDev()).Should(BeTrue())

		os.Setenv("ARES_ENV", string(EnvProd))
		ResetDefaults()
		Ω(IsProd()).Should(BeTrue())

		os.Setenv("ARES_ENV", string(EnvStaging))
		ResetDefaults()
		Ω(IsStaging()).Should(BeTrue())
	})
})
