package main

import (
	"fmt"
	"strconv"

	"6.824/raft"
)

func printMenu() {
	println("\na. Print current server")
	println("b. Connect a servers")
	println("c. Disconnect a servers")
	println("d. Store K/V")
	println("e. emit a command")
	println("f. crash a server")
	println("g. start a server")
	println("h. Back to the main menu ")
	println("q. Quit")
	print("ENTER: ")
}

var unreliableNetowrk = true

func main() {

	for {
		fmt.Println("\n================================================")
		fmt.Println("WELCOME to raft simulation!   (press q to leave)")
		fmt.Println("================================================")
		fmt.Print("\nPlease select number of server:")
		var ns string
		fmt.Scanln(&ns)
		if ns == "q" {
			fmt.Println("\nGoodbye")
			break
		}
		numServer, _ := strconv.Atoi(ns)
		if numServer < 2 || numServer > 12 {
			fmt.Println("\nInvalid Server Number (please select between 2 - 12)")
			continue
		}
		cfg := raft.Make_config(numServer, unreliableNetowrk)
		for {
			printMenu()
			var command string
			fmt.Scanln(&command)
			if command == "a" {
				cfg.PrintAllInformation()
			} else if command == "b" {
				print("\nSelect a server that your want connect: ")
				var sn string
				fmt.Scanln(&sn)
				if sn == "q" {
					return
				}
				serverNum, _ := strconv.Atoi(sn)
				if serverNum < 0 || serverNum > numServer {
					println("Error, their is no such server")
					continue
				}
				cfg.Connect(serverNum)
			} else if command == "c" {
				print("\nSelect a server that your want disconnect: ")
				var sn string
				fmt.Scanln(&sn)
				if sn == "q" {
					return
				}
				serverNum, _ := strconv.Atoi(sn)
				if serverNum < 0 || serverNum > numServer {
					println("Error, their is no such server")
					continue
				}
				cfg.Disconnect(serverNum)
			} else if command == "d" {
				println("Currently not supportted")
				continue
			} else if command == "e" {
				println("\nWrite down a command or a number")
				var sn string
				fmt.Scanln(&sn)
				if sn == "q" {
					return
				}
				cfg.One(sn)
			} else if command == "f" {
				print("\nSelect a server that your want crash: ")
				var sn string
				fmt.Scanln(&sn)
				if sn == "q" {
					return
				}
				serverNum, _ := strconv.Atoi(sn)
				if serverNum < 0 || serverNum > numServer {
					println("Error, their is no such server")
					continue
				}
				cfg.Crash1(serverNum)
			} else if command == "g" {
				print("\nSelect a server that your want Start: ")
				var sn string
				fmt.Scanln(&sn)
				if sn == "q" {
					return
				}
				serverNum, _ := strconv.Atoi(sn)
				if serverNum < 0 || serverNum > numServer {
					println("Error, their is no such server")
					continue
				}
				cfg.Start1(serverNum)
			} else if command == "h" {
				cfg.Cleanup()
				break
			} else if command == "q" {
				cfg.Cleanup()
				fmt.Println("\nGoodbye")
				return
			} else {
				fmt.Println("\n reselect command\n")
				continue
			}
		}
	}
}
