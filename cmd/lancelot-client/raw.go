/*
Copyright © 2021 huangnauh <huanglibo2010@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/tikv"
)

// rawCmd represents the raw command
var rawCmd = &cobra.Command{
	Use:   "raw",
	Short: "raw key and value",
	// Run: func(cmd *cobra.Command, args []string) {
	// 	fmt.Println("raw called")
	// },
}

var rawGetCmd = &cobra.Command{
	Use:   "get",
	Short: "get key and value",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("raw get called")
	},
}

var rawSetCmd = &cobra.Command{
	Use:   "set",
	Short: "set key and value",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("raw set called")
	},
}

var rawListCmd = &cobra.Command{
	Use:   "list",
	Short: "list key and value",
	Run: func(cmd *cobra.Command, args []string) {
		var start, end, regionStartKey, regionEndKey []byte
		var err error

		if startFlag != "" {
			if base64Flag {
				start, err = base64.StdEncoding.DecodeString(startFlag)
				if err != nil {
					errorExitf("start %s, err %s", startFlag, err)
				}
			} else if hexFlag {
				start, err = hex.DecodeString(startFlag)
				if err != nil {
					errorExitf("start %s, err %s", startFlag, err)
				}
			} else {
				start = []byte(startFlag)
			}
		} else {
			start = []byte{}
		}

		if endFlag != "" {
			if base64Flag {
				end, err = base64.StdEncoding.DecodeString(endFlag)
				if err != nil {
					errorExitf("end %s, err %s", endFlag, err)
				}
			} else if hexFlag {
				end, err = hex.DecodeString(endFlag)
				if err != nil {
					errorExitf("end %s, err %s", endFlag, err)
				}
			} else {
				end = []byte(endFlag)
			}
		} else {
			end = []byte{'\xFF'}
		}

		if regionFlag > 0 {
			region, err := store.GetRegionCache().PDClient().GetRegionByID(context.Background(), uint64(regionFlag))
			if err != nil {
				errorExitf("get region %d err %s", regionFlag, err)
			}
			regionStartKey = region.Meta.StartKey
			regionEndKey = region.Meta.EndKey
			if bytes.Compare(start, regionStartKey) < 0 {
				start = regionStartKey
			}
			if bytes.Compare(end, regionEndKey) > 0 {
				end = regionEndKey
			}
		} else if regionFlag < 0 {
			region, err := store.GetRegionCache().PDClient().GetRegionByID(context.Background(), uint64(-regionFlag))
			if err != nil {
				errorExitf("get region %d err %s", regionFlag, err)
			}
			if reverseFlag {
				end = region.Meta.StartKey
			} else {
				start = region.Meta.EndKey
			}
		}
		txn, err := store.Begin()
		if err != nil {
			errorExitf("client begin, err %s", err)
		}
		var it tikv.Iterator
		if !reverseFlag {
			it, err = txn.Iter(start, end)
			if err != nil {
				errorExitf("client iter, err %s", err)
			}
		} else {
			it, err = txn.IterReverse(end)
			if err != nil {
				errorExitf("client iter, err %s", err)
			}
		}

		count := 0
		for it.Valid() {
			key := it.Key()
			if bytes.Compare(key, start) < 0 || bytes.Compare(key, end) > 0 {
				break
			}
			if keyOnlyFlag {
				fmt.Println(string(key))
			} else {
				fmt.Printf("base64-key: %s, key: %s, value:%s\n", base64.StdEncoding.EncodeToString(it.Key()), []byte(it.Key()), []byte(it.Value()))
			}
			count++
			if count >= limitFlag {
				break
			}
			err = it.Next()
			if err != nil {
				errorExitf("client iter next, err %s", err)
			}
		}
	},
}

var (
	startFlag           string
	endFlag             string
	base64Flag, hexFlag bool
	keyOnlyFlag         bool
	limitFlag           int
	reverseFlag         bool
	regionFlag          int
)

func init() {
	rootCmd.AddCommand(rawCmd)
	rawCmd.AddCommand(rawGetCmd)
	rawCmd.AddCommand(rawSetCmd)
	rawCmd.AddCommand(rawListCmd)
	rawListCmd.Flags().StringVarP(&startFlag, "start", "s", "", "start")
	rawListCmd.Flags().StringVarP(&endFlag, "end", "e", "", "end")
	rawListCmd.Flags().IntVarP(&limitFlag, "limit", "l", 256, "limit")
	rawListCmd.Flags().BoolVar(&base64Flag, "base64", false, "base64")
	rawListCmd.Flags().BoolVar(&hexFlag, "hex", false, "hex")
	rawListCmd.Flags().BoolVarP(&keyOnlyFlag, "only-key", "k", false, "only keys")
	rawListCmd.Flags().BoolVarP(&reverseFlag, "reverse", "r", false, "reverse")
	rawListCmd.Flags().IntVar(&regionFlag, "region", 0, "region")

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// rawCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// rawCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
