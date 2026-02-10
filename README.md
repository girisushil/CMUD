p3
==

This repository contains the starter code for project 3 (15-440, Fall 2025). It also contains
the tests that we will use to grade your implementation, the CMUD game (`app` folder), and two simple kvserver/kvclient runners (`srunner` and `crunner`) that you might find useful for your own testing
purposes. 

## Starter Code

The starter code for this project is organized as follows:

```
src/github.com/cmu440/
  actor/                Actor system, TODO: you implement mailbox.go and remote_tell.go!
  app/                  CMUD client, a game which you can play to test your implementation!
  crunner/              Simple kvclient runner program
  example/              "Counter actor" example using the actor package
  kvclient/             Key-value store client library, TODO: you implement this!
  kvcommon/             Shared internals (RPC types) for kvclient and kvserver
  kvserver/             Key-value store server, TODO: you implement this!
  srunner/              Simple kvserver runner program
  staff/                Utilities used to control network conditions during tests
  tests/                Tests (go tests and shell scripts)
```

Besides filling in the `TODO` files, you may add additional files and packages as you see fit, so long as they are inside the `src/github.com/cmu440` directory. Any changes you make to other starter files will be ignored on Gradescope.

## Instructions

**How to Write Go Code**

If at any point you have any trouble with building, installing, or testing your code, the article
titled [How to Write Go Code](http://golang.org/doc/code.html) is a great resource for understanding
how Go workspaces are built and organized. You might also find the documentation for the
[`go` command](http://golang.org/cmd/go/) to be helpful. As always, feel free to post your questions
on Edstem.

### Testing your code using `srunner` & `crunner`

To make testing your key-value store a bit easier, we have provided two simple kvserver/kvclient runner programs called `srunner` and `crunner`. If you look at the source code for the two programs,
you'll notice that they import the `github.com/cmu440/kvserver` or `github.com/cmu440/kvclient` package (in other words, they compile
against the current state of your key-value store implementation). We believe you will find these programs
useful at any stage of development, to check whether your kvclient and kvserver can connect, whether local actors are syncing, whether remote actors are syncing, etc.

To run these programs, use the `go run` command from inside the directory
storing the file.

Start a server that has a single request actor on port 6001 by default: navigate to `src/github.com/cmu440/srunner` and run
```bash
go run srunner.go
```
Connect to a request actor and make requests: navigate to `src/github.com/cmu440/crunner` and run
```bash
go run crunner.go <request actor address>
```
where `<request actor address>` is e.g. `localhost:6001`.

The `srunner` program may be customized using command line flags. For more
information, specify the `-h` flag at the command line. For example,

```bash
$ go run srunner.go -h
Usage:
	srunner [options] <existing server descs...>
where options are:
  -count int
    	request actor count (default 1)
  -port int
    	starting port number (default 6000)
```

### Testing your code by playing the game

We have also provided a game client, CMUD, which you can use to test your key-value store implementation.

To run a server, navigate to `github.com/cmu440/srunner` and run:
```bash
go run srunner.go -count 5
```

To run the CMUD client, navigate to `github.com/cmu440/app` and run:
```bash
go run cmud.go <request actor address>
```
where `<request actor address>` is e.g. `localhost:6001`.

### Running the tests

All tests are inside the `src/github.com/cmu440/tests` directory. On Gradescope, we will execute the following command from inside the appropriate directory for each test (where `<test name>` is the name of one of the test cases, such as `TestOneActorGet` or `TestLocalSyncBasic2`):

```sh
go test -race -cpu 4 -timeout <timeout> -run=<test name>
```

Note that we will execute each test _individually_ using the `-run` flag and by specifying the test to run. To ensure that previous tests don't affect the outcome of later tests,
we recommend executing the tests individually as opposed to all together using `go test`. Alternatively, you may implement the `Close()` functions in `kvserver/server.go` and `kvclient/client.go`, which should make it possible to run multiple/repeated tests in a single `go test` command.

We have also provided Gradescope test script mocks `checkpoint.sh` and `final.sh` in `src/github.com/cmu440/tests/`. When you execute one of these scripts, you can get a rough sense of what your
score should be on Gradescope.

### Submitting to Gradescope

Please disable or remove all debug prints regardless of whether you are using a logging framework or not before submitting to Gradescope. This helps avoid inadvertent failures, messy autograder outputs, and style point deductions.

For both the checkpoint and the final submission, create `handin.zip` using the following command under the `p3/` directory, and then upload it to Gradescope.

```bash
sh make_submit.sh
```

If you have a Windows computer, there have been issues with path encodings and you might need to modify the make_submit.sh file to replace -r handin.zip with

```bash
& "C:\Program Files\7-Zip\7z.exe" a handin.zip
```

Keep in mind the submission limits and partner guidelines described in the handout.

## Miscellaneous

### Running the actor example

To run the "counter actor" example in `example/`, first finish `actor/mailbox.go`. Then in the `example/` folder, run:
```
go run .
```
(`go run main.go` doesn't work due to a technicality.)

### Reading the API Documentation

Before you begin the project, you should read and understand all of the starter code we provide.
To make this experience a little less traumatic (we know, it's a lot :P),
you can read the documentation in a browser:
1. Install `godoc` globally, by running the following command **outside** the `src/github.com/cmu440` directory:
```sh
go install golang.org/x/tools/cmd/godoc@latest
```
2. Start a godoc server by running the following command **inside** the `src/github.com/cmu440` directory:
```sh
godoc -http=:5050
```
3. While the server is running, navigate to [localhost:5050/pkg/github.com/cmu440](http://localhost:5050/pkg/github.com/cmu440) in a browser.

