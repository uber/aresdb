package main

import (
	"github.com/abiosoft/ishell"
	"github.com/spf13/cobra"
	"net/http"
	"fmt"
	"github.com/fatih/color"
	"io/ioutil"
	"encoding/json"
	"strings"
)

const contentType = "application/json"

type shellContext struct {
	host string
	port int
	clusterMode bool
	client http.Client
}

// script global context
var ctx shellContext

func show(c *ishell.Context) {
	if len(c.Args) != 1 {
		c.Println(color.New(color.FgRed).Println("invalid argument for show command"))
	} else {
		arg := c.Args[0]
		switch arg {
		case "tables":
			if ctx.clusterMode {
				// TODO: @shz implement cluster mode with gateway.controller
				c.Println("Not Implemented yet")
			} else {
				// local mode
				resp, err := http.Get(fmt.Sprintf("http://%s:%d/schema/tables", ctx.host, ctx.port))
				if err != nil {
					c.Println(color.New(color.FgRed).Sprintf(err.Error()))
					return
				}
				if resp.StatusCode != http.StatusOK {
					c.Println(color.New(color.FgRed).Sprintf("Got code %d from aresdb server", resp.StatusCode))
					return
				}
				var tables []string
				var data []byte
				data, err = ioutil.ReadAll(resp.Body)
				if err != nil {
					c.Println(color.New(color.FgRed).Sprintf("error reading response: %s", err))
					return
				}
				err = json.Unmarshal(data, &tables)
				if err != nil {
					c.Println(color.New(color.FgRed).Sprintf("error decoding response: %s", err))
					return
				}
				c.Println(strings.Join(tables, " "))
			}
		case "configs":
			c.Printf("%+v\n",ctx)
		}
	}
}

func aql(c *ishell.Context) {
	c.Println("aql query ending with semicolon ';':")
	lines := c.ReadMultiLines(";")
	lines = strings.Trim(lines, ";")
	resp, err := http.Post(fmt.Sprintf("http://%s:%d/query/aql", ctx.host, ctx.port), contentType, strings.NewReader(lines))
	if err != nil {
		c.Println(color.New(color.FgRed).Sprintf(err.Error()))
		return
	}
	if resp.StatusCode != http.StatusOK {
		c.Println(color.New(color.FgRed).Sprintf("Got code %d from aresdb server", resp.StatusCode))
		return
	}
	var data []byte
	data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		c.Println(color.New(color.FgRed).Sprintf("error reading response: %s", err))
		return
	}

	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	if err != nil {
		c.Println(color.New(color.FgRed).Sprintf("error decoding response: %s", err))
		return
	}
	data, err = json.MarshalIndent(result, "", "  ")
	if err != nil {
		c.Println(color.New(color.FgRed).Sprintf("error formatting response: %s", err))
		return
	}
	c.ShowPaged(string(data))
}

func Execute() {

	// ishell shell
	shell := ishell.New()

	shell.Println("Welcome to AresDB Cli!")
	shell.AddCmd(&ishell.Cmd{
		Name: "show",
		Help: "`show tables` will show all tables in current cluster",
		Func: show,
		Completer: func(args []string) []string {
			return []string{"tables", "configs"}
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "aql",
		Help: "start a new aql query",
		Func: aql,
	})

	//TODO: add sql cmd

	// cobra command
	cmd := &cobra.Command{
		Use: "arescli",
		Short: "AresDB cli",
		Long: "AresDB command line tool to interact with the backend",
		Example: "arescli --host localhost --port 9374",
		Run: func(cmd *cobra.Command, args []string) {
			// read args
			var err error
			ctx.host, err = cmd.Flags().GetString("host")
			if err != nil {
				panic("failed to get aresdb host")
			}
			ctx.port, err = cmd.Flags().GetInt("port")
			if err != nil {
				panic("failed to get aresdb port")
			}
			ctx.clusterMode, err = cmd.Flags().GetBool("cluster")
			if err != nil {
				panic("failed to get aresdb cluster mode config")
			}

			// config http client
			ctx.client = http.Client{}

			if len(args) > 1 {
				shell.Process(args[1:]...)
			} else {
				shell.Run()
				shell.Close()
			}
		},
	}

	cmd.Flags().StringP("host", "","localhost", "host of aresdb service, in cluster mode, host of controller")
	cmd.Flags().IntP("port", "p", 9374, "port of aresdb service, in cluster mode, port of controller")
	cmd.Flags().BoolP("cluster", "c", false, "whether to use cluster mode")
	cmd.Execute()
}


func main() {
	Execute()
}


