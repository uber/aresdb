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
	"io"
	"io/ioutil"
	"os"
)

// FileSystem is a file system interface
type FileSystem interface {
	ReadFile(filename string) ([]byte, error)
	ReadDir(dirname string) ([]os.FileInfo, error)
	Stat(path string) (os.FileInfo, error)
	Mkdir(name string, perm os.FileMode) error
	MkdirAll(path string, perm os.FileMode) error
	Remove(path string) error
	RemoveAll(path string) error
	OpenFileForWrite(name string, flag int, perm os.FileMode) (io.WriteCloser, error)
}

// OSFileSystem implements FileSystem using os package
type OSFileSystem struct{}

// ReadFile reads whole file into byte buffer
func (OSFileSystem) ReadFile(name string) ([]byte, error) {
	return ioutil.ReadFile(name)
}

// ReadDir reads file infos under given directory
func (OSFileSystem) ReadDir(dirname string) ([]os.FileInfo, error) {
	return ioutil.ReadDir(dirname)
}

// Mkdir makes directory with given name and permission
func (OSFileSystem) Mkdir(name string, perm os.FileMode) error {
	return os.Mkdir(name, perm)
}

// MkdirAll makes directory with necessary parent directories in path
func (OSFileSystem) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

// OpenFileForWrite opens a file for write
func (OSFileSystem) OpenFileForWrite(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
	return os.OpenFile(name, flag, perm)
}

// Remove removes a file
func (OSFileSystem) Remove(path string) error {
	return os.Remove(path)
}

// RemoveAll removes a file and all its children
func (OSFileSystem) RemoveAll(path string) error {
	return os.RemoveAll(path)
}

// Stat tries gets file info for t
func (OSFileSystem) Stat(path string) (os.FileInfo, error) {
	return os.Stat(path)
}
